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

// defaultStreamMode is the capacity mode Kinesis applies when none is specified.
const defaultStreamMode = string(svcsdktypes.StreamModeProvisioned)

func effectiveStreamMode(r *resource) string {
	if r.ko.Spec.StreamModeDetails != nil && r.ko.Spec.StreamModeDetails.StreamMode != nil {
		return *r.ko.Spec.StreamModeDetails.StreamMode
	}
	return defaultStreamMode
}

func compareStreamModeDetails(
	delta *ackcompare.Delta,
	a *resource,
	b *resource,
) {
	if effectiveStreamMode(a) != effectiveStreamMode(b) {
		delta.Add("Spec.StreamModeDetails", a.ko.Spec.StreamModeDetails, b.ko.Spec.StreamModeDetails)
	}
}

// isOnDemand reports whether the resource's effective capacity mode is ON_DEMAND.
func isOnDemand(r *resource) bool {
	return effectiveStreamMode(r) == string(svcsdktypes.StreamModeOnDemand)
}

// compareShardCount records a delta on ShardCount only for PROVISIONED streams.
// ON_DEMAND streams manage shard capacity automatically and reject a supplied
// shard count on both CreateStream and UpdateShardCount, so a ShardCount value
// in the spec (or the shard count read back from AWS) must never register as a
// diff for an on-demand stream — doing so would drive a perpetual, failing
// UpdateShardCount loop.
func compareShardCount(
	delta *ackcompare.Delta,
	a *resource,
	b *resource,
) {
	if isOnDemand(a) || isOnDemand(b) {
		return
	}
	// ShardCount has no fixed default to restore (it is required at creation for
	// a PROVISIONED stream and set by AWS when switching from ON_DEMAND), so an
	// absent desired value means "leave the stream's current shard count as-is"
	// rather than reverting. This also avoids a failing UpdateShardCount(nil)
	// after a stream is reverted from ON_DEMAND to PROVISIONED.
	if a.ko.Spec.ShardCount == nil {
		return
	}
	if b.ko.Spec.ShardCount == nil || *a.ko.Spec.ShardCount != *b.ko.Spec.ShardCount {
		delta.Add("Spec.ShardCount", a.ko.Spec.ShardCount, b.ko.Spec.ShardCount)
	}
}

// syncStreamMode updates a stream's capacity mode via the dedicated
// UpdateStreamMode API. StreamModeDetails cannot be modified through the
// standard UpdateShardCount update path, so it is handled out-of-band here.
func (rm *resourceManager) syncStreamMode(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncStreamMode")
	defer func(err error) { exit(err) }(err)

	if latest.ko.Status.ACKResourceMetadata == nil || latest.ko.Status.ACKResourceMetadata.ARN == nil {
		return errors.New("stream ARN is required to update stream mode")
	}
	streamARN := (*string)(latest.ko.Status.ACKResourceMetadata.ARN)

	_, err = rm.sdkapi.UpdateStreamMode(
		ctx,
		&svcsdk.UpdateStreamModeInput{
			StreamARN: streamARN,
			StreamModeDetails: &svcsdktypes.StreamModeDetails{
				StreamMode: svcsdktypes.StreamMode(effectiveStreamMode(desired)),
			},
		},
	)
	rm.metrics.RecordAPICall("UPDATE", "UpdateStreamMode", err)
	return err
}
