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

// syncResourcePolicy updates a Kinesis data stream's resource-based policy. If
// the desired state has no policy, any existing policy is removed.
func (rm *resourceManager) syncResourcePolicy(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncResourcePolicy")
	defer func(err error) { exit(err) }(err)

	if desired.ko.Spec.ResourcePolicy == nil {
		return rm.deleteResourcePolicy(ctx, latest)
	}

	return rm.putResourcePolicy(ctx, desired)
}

// putResourcePolicy attaches or updates a resource-based policy on a Kinesis
// data stream.
func (rm *resourceManager) putResourcePolicy(
	ctx context.Context,
	r *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.putResourcePolicy")
	defer func(err error) { exit(err) }(err)

	if r.ko.Spec.ResourcePolicy == nil {
		return nil
	}

	if r.ko.Status.ACKResourceMetadata == nil || r.ko.Status.ACKResourceMetadata.ARN == nil {
		return errors.New("stream ARN is required to put resource policy")
	}
	streamARN := (*string)(r.ko.Status.ACKResourceMetadata.ARN)

	_, err = rm.sdkapi.PutResourcePolicy(
		ctx,
		&svcsdk.PutResourcePolicyInput{
			ResourceARN: streamARN,
			Policy:      r.ko.Spec.ResourcePolicy,
		},
	)
	rm.metrics.RecordAPICall("UPDATE", "PutResourcePolicy", err)
	return err
}

// deleteResourcePolicy removes a resource-based policy from a Kinesis data
// stream.
func (rm *resourceManager) deleteResourcePolicy(
	ctx context.Context,
	r *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.deleteResourcePolicy")
	defer func(err error) { exit(err) }(err)

	if r.ko.Status.ACKResourceMetadata == nil || r.ko.Status.ACKResourceMetadata.ARN == nil {
		return errors.New("stream ARN is required to delete resource policy")
	}
	streamARN := (*string)(r.ko.Status.ACKResourceMetadata.ARN)

	_, err = rm.sdkapi.DeleteResourcePolicy(
		ctx,
		&svcsdk.DeleteResourcePolicyInput{
			ResourceARN: streamARN,
		},
	)
	rm.metrics.RecordAPICall("DELETE", "DeleteResourcePolicy", err)
	if err != nil {
		// Kinesis has no dedicated PolicyNotFoundException; a missing policy
		// surfaces as ResourceNotFoundException. Treat that as success.
		var notFoundErr *svcsdktypes.ResourceNotFoundException
		if errors.As(err, &notFoundErr) {
			return nil
		}
	}

	return err
}

// getResourcePolicyWithContext retrieves the resource-based policy of a Kinesis
// data stream. A stream with no attached policy returns
// ResourceNotFoundException, which is normalized to a nil policy.
func (rm *resourceManager) getResourcePolicyWithContext(
	ctx context.Context,
	streamARN *string,
) (*string, error) {
	var err error
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.getResourcePolicyWithContext")
	defer func(err error) { exit(err) }(err)

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
		var notFoundErr *svcsdktypes.ResourceNotFoundException
		if errors.As(err, &notFoundErr) {
			return nil, nil
		}
		return nil, err
	}

	return res.Policy, nil
}

// compareResourcePolicyDocument is a custom comparison function for
// ResourcePolicy documents. The reason why we need a custom function for
// this field is to handle the variability in shapes of JSON objects representing
// IAM policies, especially when it comes to statements, actions, and other fields.
func compareResourcePolicyDocument(
	delta *ackcompare.Delta,
	a *resource,
	b *resource,
) {
	// Handle cases where one policy is nil and the other is not.
	// This means one resource has a policy and the other doesn't - they're different.
	if ackcompare.HasNilDifference(a.ko.Spec.ResourcePolicy, b.ko.Spec.ResourcePolicy) {
		delta.Add("Spec.ResourcePolicy", a.ko.Spec.ResourcePolicy, b.ko.Spec.ResourcePolicy)
		return
	}

	// If both policies are nil, there's no difference - both resources have no policy.
	if a.ko.Spec.ResourcePolicy == nil && b.ko.Spec.ResourcePolicy == nil {
		return
	}

	// At this point, both policies are non-nil. We need to compare their JSON content.
	// To handle the variability in shapes of JSON objects representing IAM policies,
	// especially when it comes to statements, actions, and other fields, we need
	// a custom json.Unmarshaller approach crafted to our specific needs. Luckily,
	// it happens that @micahhausler built a library dedicated to this very special
	// need: github.com/micahhausler/aws-iam-policy.
	//
	// Copied from IAM Controller: https://github.com/aws-controllers-k8s/iam-controller/blob/main/pkg/resource/role/hooks.go#L398-L432
	// Based on review feedback: https://github.com/aws-controllers-k8s/dynamodb-controller/pull/154#discussion_r2443876840
	var policyDocumentA awsiampolicy.Policy
	_ = json.Unmarshal([]byte(*a.ko.Spec.ResourcePolicy), &policyDocumentA)
	var policyDocumentB awsiampolicy.Policy
	_ = json.Unmarshal([]byte(*b.ko.Spec.ResourcePolicy), &policyDocumentB)

	if !reflect.DeepEqual(policyDocumentA, policyDocumentB) {
		delta.Add("Spec.ResourcePolicy", a.ko.Spec.ResourcePolicy, b.ko.Spec.ResourcePolicy)
	}
}
