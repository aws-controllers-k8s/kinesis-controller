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

	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	util "github.com/aws-controllers-k8s/kinesis-controller/pkg/resource/tags"
)

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

// syncMaxRecordSize updates a stream's maximum record size via the dedicated
// UpdateMaxRecordSize API. MaxRecordSizeInKiB cannot be modified through the
// standard UpdateShardCount update path, so it is handled out-of-band here.
func (rm *resourceManager) syncMaxRecordSize(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncMaxRecordSize")
	defer func(err error) { exit(err) }(err)

	if desired.ko.Spec.MaxRecordSizeInKiB == nil {
		return nil
	}

	if latest.ko.Status.ACKResourceMetadata == nil || latest.ko.Status.ACKResourceMetadata.ARN == nil {
		return errors.New("stream ARN is required to update max record size")
	}
	streamARN := (*string)(latest.ko.Status.ACKResourceMetadata.ARN)

	maxRecordSize := *desired.ko.Spec.MaxRecordSizeInKiB
	if maxRecordSize > math.MaxInt32 || maxRecordSize < math.MinInt32 {
		return fmt.Errorf("error: field MaxRecordSizeInKiB is of type int32")
	}
	maxRecordSizeCopy := int32(maxRecordSize)

	_, err = rm.sdkapi.UpdateMaxRecordSize(
		ctx,
		&svcsdk.UpdateMaxRecordSizeInput{
			StreamARN:          streamARN,
			MaxRecordSizeInKiB: &maxRecordSizeCopy,
		},
	)
	rm.metrics.RecordAPICall("UPDATE", "UpdateMaxRecordSize", err)
	return err
}
