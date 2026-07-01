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
	"fmt"
	"math"

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
)

const defaultWarmThroughputMiBps int64 = 0

func compareWarmThroughput(
	delta *ackcompare.Delta,
	a *resource,
	b *resource,
) {
	if !isOnDemand(a) || !isOnDemand(b) {
		return
	}
	desired := defaultWarmThroughputMiBps
	if a.ko.Spec.WarmThroughputMiBps != nil {
		desired = *a.ko.Spec.WarmThroughputMiBps
	}
	latest := defaultWarmThroughputMiBps
	if b.ko.Spec.WarmThroughputMiBps != nil {
		latest = *b.ko.Spec.WarmThroughputMiBps
	}
	if desired != latest {
		delta.Add("Spec.WarmThroughputMiBps", a.ko.Spec.WarmThroughputMiBps, b.ko.Spec.WarmThroughputMiBps)
	}
}

// syncWarmThroughput updates a stream's target warm throughput via the
// dedicated UpdateStreamWarmThroughput API. WarmThroughputMiBps cannot be
// modified through the standard UpdateShardCount update path, so it is handled
// out-of-band here.
func (rm *resourceManager) syncWarmThroughput(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncWarmThroughput")
	defer func(err error) { exit(err) }(err)

	if latest.ko.Status.ACKResourceMetadata == nil || latest.ko.Status.ACKResourceMetadata.ARN == nil {
		return errors.New("stream ARN is required to update warm throughput")
	}
	streamARN := (*string)(latest.ko.Status.ACKResourceMetadata.ARN)

	warmThroughput := defaultWarmThroughputMiBps
	if desired.ko.Spec.WarmThroughputMiBps != nil {
		warmThroughput = *desired.ko.Spec.WarmThroughputMiBps
	}
	if warmThroughput > math.MaxInt32 || warmThroughput < math.MinInt32 {
		return fmt.Errorf("error: field WarmThroughputMiBps is of type int32")
	}
	warmThroughputCopy := int32(warmThroughput)

	_, err = rm.sdkapi.UpdateStreamWarmThroughput(
		ctx,
		&svcsdk.UpdateStreamWarmThroughputInput{
			StreamARN:           streamARN,
			WarmThroughputMiBps: &warmThroughputCopy,
		},
	)
	rm.metrics.RecordAPICall("UPDATE", "UpdateStreamWarmThroughput", err)
	return err
}
