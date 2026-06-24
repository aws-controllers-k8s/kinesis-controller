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
	"fmt"
	"math"
	"sort"

	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	svcapitypes "github.com/aws-controllers-k8s/kinesis-controller/apis/v1alpha1"
	util "github.com/aws-controllers-k8s/kinesis-controller/pkg/resource/tags"
)

// defaultRetentionPeriodHours is the retention period (in hours) that Kinesis
// applies to a newly created stream when none is requested.
const defaultRetentionPeriodHours = int64(24)

func isStreamActive(status *string) bool {
	if status == nil {
		return false
	}

	return *status == string(svcsdktypes.StreamStatusActive)
}

func (rm *resourceManager) getTags(ctx context.Context, streamName *string) (map[string]*string, error) {
	return util.GetResourceTags(ctx, rm.sdkapi, rm.metrics, streamName)
}

func (rm *resourceManager) syncTags(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	return util.SyncResourceTags(ctx, rm.sdkapi, rm.metrics, latest.ko.Spec.Name, desired.ko.Spec.Tags, latest.ko.Spec.Tags)
}

func streamARN(r *resource) *string {
	if r == nil || r.ko.Status.ACKResourceMetadata == nil || r.ko.Status.ACKResourceMetadata.ARN == nil {
		return nil
	}
	return (*string)(r.ko.Status.ACKResourceMetadata.ARN)
}

func int64ToInt32Ptr(field string, v int64) (*int32, error) {
	if v > math.MaxInt32 || v < math.MinInt32 {
		return nil, fmt.Errorf("error: field %s is of type int32", field)
	}
	c := int32(v)
	return &c, nil
}

func (rm *resourceManager) syncRetentionPeriod(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncRetentionPeriod")
	defer func() { exit(err) }()

	if desired.ko.Spec.RetentionPeriodHours == nil {
		return nil
	}
	desiredHours := *desired.ko.Spec.RetentionPeriodHours
	latestHours := defaultRetentionPeriodHours
	if latest.ko.Spec.RetentionPeriodHours != nil {
		latestHours = *latest.ko.Spec.RetentionPeriodHours
	}
	if desiredHours == latestHours {
		return nil
	}
	hours, err := int64ToInt32Ptr("RetentionPeriodHours", desiredHours)
	if err != nil {
		return err
	}
	if desiredHours > latestHours {
		_, err = rm.sdkapi.IncreaseStreamRetentionPeriod(ctx, &svcsdk.IncreaseStreamRetentionPeriodInput{
			StreamARN:            streamARN(latest),
			StreamName:           latest.ko.Spec.Name,
			RetentionPeriodHours: hours,
		})
		rm.metrics.RecordAPICall("UPDATE", "IncreaseStreamRetentionPeriod", err)
		return err
	}
	_, err = rm.sdkapi.DecreaseStreamRetentionPeriod(ctx, &svcsdk.DecreaseStreamRetentionPeriodInput{
		StreamARN:            streamARN(latest),
		StreamName:           latest.ko.Spec.Name,
		RetentionPeriodHours: hours,
	})
	rm.metrics.RecordAPICall("UPDATE", "DecreaseStreamRetentionPeriod", err)
	return err
}

func (rm *resourceManager) syncEnhancedMonitoring(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncEnhancedMonitoring")
	defer func() { exit(err) }()

	if desired.ko.Spec.ShardLevelMetrics == nil {
		return nil
	}
	desiredSet := expandShardLevelMetrics(desired.ko.Spec.ShardLevelMetrics)
	latestSet := expandShardLevelMetrics(latest.ko.Spec.ShardLevelMetrics)

	toEnable := metricsNotIn(desiredSet, latestSet)
	toDisable := metricsNotIn(latestSet, desiredSet)

	if len(toEnable) > 0 {
		_, err = rm.sdkapi.EnableEnhancedMonitoring(ctx, &svcsdk.EnableEnhancedMonitoringInput{
			StreamARN:         streamARN(latest),
			StreamName:        latest.ko.Spec.Name,
			ShardLevelMetrics: toEnable,
		})
		rm.metrics.RecordAPICall("UPDATE", "EnableEnhancedMonitoring", err)
		if err != nil {
			return err
		}
	}
	if len(toDisable) > 0 {
		_, err = rm.sdkapi.DisableEnhancedMonitoring(ctx, &svcsdk.DisableEnhancedMonitoringInput{
			StreamARN:         streamARN(latest),
			StreamName:        latest.ko.Spec.Name,
			ShardLevelMetrics: toDisable,
		})
		rm.metrics.RecordAPICall("UPDATE", "DisableEnhancedMonitoring", err)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rm *resourceManager) syncEncryption(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncEncryption")
	defer func() { exit(err) }()

	if desired.ko.Spec.EncryptionType == nil {
		return nil
	}
	desiredType := *desired.ko.Spec.EncryptionType
	latestType := string(svcsdktypes.EncryptionTypeNone)
	if latest.ko.Spec.EncryptionType != nil && *latest.ko.Spec.EncryptionType != "" {
		latestType = *latest.ko.Spec.EncryptionType
	}

	switch desiredType {
	case string(svcsdktypes.EncryptionTypeKms):
		_, err = rm.sdkapi.StartStreamEncryption(ctx, &svcsdk.StartStreamEncryptionInput{
			StreamARN:      streamARN(latest),
			StreamName:     latest.ko.Spec.Name,
			EncryptionType: svcsdktypes.EncryptionTypeKms,
			KeyId:          desired.ko.Spec.KeyID,
		})
		rm.metrics.RecordAPICall("UPDATE", "StartStreamEncryption", err)
		return err
	default:
		if latestType != string(svcsdktypes.EncryptionTypeKms) {
			return nil
		}
		_, err = rm.sdkapi.StopStreamEncryption(ctx, &svcsdk.StopStreamEncryptionInput{
			StreamARN:      streamARN(latest),
			StreamName:     latest.ko.Spec.Name,
			EncryptionType: svcsdktypes.EncryptionTypeKms,
			KeyId:          latest.ko.Spec.KeyID,
		})
		rm.metrics.RecordAPICall("UPDATE", "StopStreamEncryption", err)
		return err
	}
}

func (rm *resourceManager) syncStreamMode(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncStreamMode")
	defer func() { exit(err) }()

	if desired.ko.Spec.StreamModeDetails == nil ||
		desired.ko.Spec.StreamModeDetails.StreamMode == nil {
		return nil
	}
	_, err = rm.sdkapi.UpdateStreamMode(ctx, &svcsdk.UpdateStreamModeInput{
		StreamARN: streamARN(latest),
		StreamModeDetails: &svcsdktypes.StreamModeDetails{
			StreamMode: svcsdktypes.StreamMode(*desired.ko.Spec.StreamModeDetails.StreamMode),
		},
	})
	rm.metrics.RecordAPICall("UPDATE", "UpdateStreamMode", err)
	return err
}

func (rm *resourceManager) syncMaxRecordSize(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncMaxRecordSize")
	defer func() { exit(err) }()

	if desired.ko.Spec.MaxRecordSizeInKiB == nil {
		return nil
	}
	size, err := int64ToInt32Ptr("MaxRecordSizeInKiB", *desired.ko.Spec.MaxRecordSizeInKiB)
	if err != nil {
		return err
	}
	_, err = rm.sdkapi.UpdateMaxRecordSize(ctx, &svcsdk.UpdateMaxRecordSizeInput{
		StreamARN:          streamARN(latest),
		MaxRecordSizeInKiB: size,
	})
	rm.metrics.RecordAPICall("UPDATE", "UpdateMaxRecordSize", err)
	return err
}

func (rm *resourceManager) syncWarmThroughput(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncWarmThroughput")
	defer func() { exit(err) }()

	if desired.ko.Spec.WarmThroughputMiBps == nil {
		return nil
	}
	mibps, err := int64ToInt32Ptr("WarmThroughputMiBps", *desired.ko.Spec.WarmThroughputMiBps)
	if err != nil {
		return err
	}
	_, err = rm.sdkapi.UpdateStreamWarmThroughput(ctx, &svcsdk.UpdateStreamWarmThroughputInput{
		StreamARN:           streamARN(latest),
		StreamName:          latest.ko.Spec.Name,
		WarmThroughputMiBps: mibps,
	})
	rm.metrics.RecordAPICall("UPDATE", "UpdateStreamWarmThroughput", err)
	return err
}

func allConcreteShardLevelMetrics() []svcsdktypes.MetricsName {
	out := []svcsdktypes.MetricsName{}
	for _, m := range svcsdktypes.MetricsName("").Values() {
		if m != svcsdktypes.MetricsNameAll {
			out = append(out, m)
		}
	}
	return out
}

func expandShardLevelMetrics(metrics []*string) map[svcsdktypes.MetricsName]struct{} {
	set := map[svcsdktypes.MetricsName]struct{}{}
	for _, m := range metrics {
		if m == nil {
			continue
		}
		if *m == string(svcsdktypes.MetricsNameAll) {
			for _, a := range allConcreteShardLevelMetrics() {
				set[a] = struct{}{}
			}
			continue
		}
		set[svcsdktypes.MetricsName(*m)] = struct{}{}
	}
	return set
}

func metricsNotIn(
	a map[svcsdktypes.MetricsName]struct{},
	b map[svcsdktypes.MetricsName]struct{},
) []svcsdktypes.MetricsName {
	out := []svcsdktypes.MetricsName{}
	for m := range a {
		if _, ok := b[m]; !ok {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func flattenEnhancedMonitoring(em []*svcapitypes.EnhancedMetrics) []*string {
	out := []*string{}
	for _, e := range em {
		if e == nil {
			continue
		}
		out = append(out, e.ShardLevelMetrics...)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func reconcileShardLevelMetrics(desired []*string, actual []*string) []*string {
	if desired == nil {
		return nil
	}
	if shardLevelMetricsEqual(desired, actual) {
		return desired
	}
	return actual
}

func shardLevelMetricsEqual(a []*string, b []*string) bool {
	sa := expandShardLevelMetrics(a)
	sb := expandShardLevelMetrics(b)
	if len(sa) != len(sb) {
		return false
	}
	for m := range sa {
		if _, ok := sb[m]; !ok {
			return false
		}
	}
	return true
}

// warmThroughputTarget returns the warm throughput (MiBps) reported by a
// DescribeStreamSummary response, as the int64 the Spec field uses, or nil.
//
// While the stream is scaling, AWS reports the requested value in TargetMiBps;
// once scaling settles it clears TargetMiBps and reports the effective value in
// CurrentMiBps. We prefer the target (so we match the desired value mid-scale)
// and fall back to current (so we still match once settled) - reading only
// TargetMiBps would make an unset target diff against the desired spec forever.
func warmThroughputTarget(wt *svcsdktypes.WarmThroughputObject) *int64 {
	if wt == nil {
		return nil
	}
	if wt.TargetMiBps != nil {
		return aws.Int64(int64(*wt.TargetMiBps))
	}
	if wt.CurrentMiBps != nil {
		return aws.Int64(int64(*wt.CurrentMiBps))
	}
	return nil
}
