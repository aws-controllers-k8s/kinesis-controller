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
	"encoding/json"
	"errors"
	"reflect"

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	awsiampolicy "github.com/micahhausler/aws-iam-policy/policy"
)

func (rm *resourceManager) syncResourcePolicy(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncResourcePolicy")
	defer func() { exit(err) }()

	streamARN := streamResourceARN(latest)
	if streamARN == nil || *streamARN == "" {
		return errors.New("stream ARN is required to sync resource policy")
	}

	if desired.ko.Spec.ResourcePolicy == nil {
		return rm.deleteResourcePolicy(ctx, streamARN)
	}
	return rm.putResourcePolicy(ctx, streamARN, desired.ko.Spec.ResourcePolicy)
}

func (rm *resourceManager) putResourcePolicy(
	ctx context.Context,
	streamARN *string,
	policy *string,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.putResourcePolicy")
	defer func() { exit(err) }()

	_, err = rm.sdkapi.PutResourcePolicy(
		ctx,
		&svcsdk.PutResourcePolicyInput{
			ResourceARN: streamARN,
			Policy:      policy,
		},
	)
	rm.metrics.RecordAPICall("UPDATE", "PutResourcePolicy", err)
	return err
}

func (rm *resourceManager) deleteResourcePolicy(
	ctx context.Context,
	streamARN *string,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.deleteResourcePolicy")
	defer func() { exit(err) }()

	_, err = rm.sdkapi.DeleteResourcePolicy(
		ctx,
		&svcsdk.DeleteResourcePolicyInput{
			ResourceARN: streamARN,
		},
	)
	rm.metrics.RecordAPICall("DELETE", "DeleteResourcePolicy", err)
	if err != nil {
		var notFound *svcsdktypes.ResourceNotFoundException
		if errors.As(err, &notFound) {
			// Policy already doesn't exist, this is a success case.
			return nil
		}
	}
	return err
}

func (rm *resourceManager) getResourcePolicy(
	ctx context.Context,
	streamARN *string,
) (policy *string, err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.getResourcePolicy")
	defer func() { exit(err) }()

	if streamARN == nil || *streamARN == "" {
		return nil, errors.New("stream ARN is required to get resource policy")
	}

	res, err := rm.sdkapi.GetResourcePolicy(
		ctx,
		&svcsdk.GetResourcePolicyInput{
			ResourceARN: streamARN,
		},
	)
	rm.metrics.RecordAPICall("GET", "GetResourcePolicy", err)
	if err != nil {
		var notFound *svcsdktypes.ResourceNotFoundException
		if errors.As(err, &notFound) {
			return nil, nil
		}
		return nil, err
	}
	return res.Policy, nil
}

func streamResourceARN(r *resource) *string {
	if r == nil || r.ko.Status.ACKResourceMetadata == nil {
		return nil
	}
	return (*string)(r.ko.Status.ACKResourceMetadata.ARN)
}

func customPreCompare(
	delta *ackcompare.Delta,
	a *resource,
	b *resource,
) {
	compareResourcePolicyDocument(delta, a, b)
}

// compareResourcePolicyDocument is a custom comparison function for the
// ResourcePolicy document. A dedicated function is needed to handle the
// variability in the shapes of JSON objects representing IAM policies
// (statements, actions, etc.), since Kinesis returns a normalized document that
// would otherwise show as a perpetual diff.
//
// Copied from the DynamoDB controller, which copied it from the IAM controller:
// https://github.com/aws-controllers-k8s/iam-controller/blob/main/pkg/resource/role/hooks.go
func compareResourcePolicyDocument(
	delta *ackcompare.Delta,
	a *resource,
	b *resource,
) {
	// One side has a policy and the other doesn't - they differ.
	if ackcompare.HasNilDifference(a.ko.Spec.ResourcePolicy, b.ko.Spec.ResourcePolicy) {
		delta.Add("Spec.ResourcePolicy", a.ko.Spec.ResourcePolicy, b.ko.Spec.ResourcePolicy)
		return
	}
	// Both nil - no difference.
	if a.ko.Spec.ResourcePolicy == nil && b.ko.Spec.ResourcePolicy == nil {
		return
	}

	// Both non-nil: compare the parsed policy documents semantically.
	var policyDocumentA awsiampolicy.Policy
	_ = json.Unmarshal([]byte(*a.ko.Spec.ResourcePolicy), &policyDocumentA)
	var policyDocumentB awsiampolicy.Policy
	_ = json.Unmarshal([]byte(*b.ko.Spec.ResourcePolicy), &policyDocumentB)

	if !reflect.DeepEqual(policyDocumentA, policyDocumentB) {
		delta.Add("Spec.ResourcePolicy", a.ko.Spec.ResourcePolicy, b.ko.Spec.ResourcePolicy)
	}
}
