// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package stream

import (
	"context"
	"errors"

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// allShardLevelMetrics is the concrete set of shard-level metrics that the
// special "ALL" value expands to. Kinesis reports enabled metrics back in
// their expanded form (never as "ALL"), so the sentinel must be normalized
// before desired and latest state can be compared.
var allShardLevelMetrics = []svcsdktypes.MetricsName{
	svcsdktypes.MetricsNameIncomingBytes,
	svcsdktypes.MetricsNameIncomingRecords,
	svcsdktypes.MetricsNameOutgoingBytes,
	svcsdktypes.MetricsNameOutgoingRecords,
	svcsdktypes.MetricsNameWriteProvisionedThroughputExceeded,
	svcsdktypes.MetricsNameReadProvisionedThroughputExceeded,
	svcsdktypes.MetricsNameIteratorAgeMilliseconds,
}

// normalizeShardLevelMetrics converts a list of shard-level metric names into a
// set, expanding the "ALL" sentinel into the concrete metrics it represents.
// nil entries and empty strings are ignored, so an unset field and an empty
// list compare equal.
func normalizeShardLevelMetrics(metrics []*string) map[svcsdktypes.MetricsName]struct{} {
	set := map[svcsdktypes.MetricsName]struct{}{}
	for _, m := range metrics {
		if m == nil || *m == "" {
			continue
		}
		if svcsdktypes.MetricsName(*m) == svcsdktypes.MetricsNameAll {
			for _, all := range allShardLevelMetrics {
				set[all] = struct{}{}
			}
			continue
		}
		set[svcsdktypes.MetricsName(*m)] = struct{}{}
	}
	return set
}

// shardLevelMetricSetsEqual reports whether two normalized metric sets contain
// exactly the same members.
func shardLevelMetricSetsEqual(a, b map[svcsdktypes.MetricsName]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// compareShardLevelMetrics is a custom comparison for the ShardLevelMetrics
// spec field. Order is insignificant and the "ALL" value expands to the full
// concrete metric set, so the default generated slice comparison would report
// spurious differences. Both sides are normalized into sets before comparison.
func compareShardLevelMetrics(
	delta *ackcompare.Delta,
	a *resource,
	b *resource,
) {
	desired := normalizeShardLevelMetrics(a.ko.Spec.ShardLevelMetrics)
	latest := normalizeShardLevelMetrics(b.ko.Spec.ShardLevelMetrics)
	if !shardLevelMetricSetsEqual(desired, latest) {
		delta.Add("Spec.ShardLevelMetrics", a.ko.Spec.ShardLevelMetrics, b.ko.Spec.ShardLevelMetrics)
	}
}

// syncShardLevelMetrics reconciles the enhanced (shard-level) CloudWatch
// metrics enabled on a stream. Metrics present in the desired spec but not
// currently enabled are turned on via EnableEnhancedMonitoring; metrics
// currently enabled but absent from the desired spec are turned off via
// DisableEnhancedMonitoring. ShardLevelMetrics cannot be modified through the
// standard UpdateShardCount update path, so it is handled out-of-band here.
func (rm *resourceManager) syncShardLevelMetrics(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncShardLevelMetrics")
	defer func(err error) { exit(err) }(err)

	if latest.ko.Status.ACKResourceMetadata == nil || latest.ko.Status.ACKResourceMetadata.ARN == nil {
		return errors.New("stream ARN is required to update shard-level metrics")
	}
	streamARN := (*string)(latest.ko.Status.ACKResourceMetadata.ARN)

	desiredSet := normalizeShardLevelMetrics(desired.ko.Spec.ShardLevelMetrics)
	latestSet := normalizeShardLevelMetrics(latest.ko.Spec.ShardLevelMetrics)

	var toEnable []svcsdktypes.MetricsName
	for m := range desiredSet {
		if _, ok := latestSet[m]; !ok {
			toEnable = append(toEnable, m)
		}
	}
	var toDisable []svcsdktypes.MetricsName
	for m := range latestSet {
		if _, ok := desiredSet[m]; !ok {
			toDisable = append(toDisable, m)
		}
	}

	if len(toEnable) > 0 {
		_, err = rm.sdkapi.EnableEnhancedMonitoring(
			ctx,
			&svcsdk.EnableEnhancedMonitoringInput{
				StreamARN:         streamARN,
				ShardLevelMetrics: toEnable,
			},
		)
		rm.metrics.RecordAPICall("UPDATE", "EnableEnhancedMonitoring", err)
		if err != nil {
			return err
		}
	}

	if len(toDisable) > 0 {
		_, err = rm.sdkapi.DisableEnhancedMonitoring(
			ctx,
			&svcsdk.DisableEnhancedMonitoringInput{
				StreamARN:         streamARN,
				ShardLevelMetrics: toDisable,
			},
		)
		rm.metrics.RecordAPICall("UPDATE", "DisableEnhancedMonitoring", err)
		if err != nil {
			return err
		}
	}

	return nil
}
