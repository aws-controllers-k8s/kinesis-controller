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
	return util.SyncResourceTags(ctx, rm.sdkapi, rm.metrics, string(*latest.ko.Spec.Name), desired.ko.Spec.Tags, latest.ko.Spec.Tags)
}
