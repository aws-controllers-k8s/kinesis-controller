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
	"strings"

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

	// A stream with no resource-based policy can come back as an empty policy
	// document ("{}") rather than a ResourceNotFoundException. Normalize that to
	// a nil policy so the delta comparison treats it identically to "no policy"
	// and does not register a perpetual diff against an unset desired
	// ResourcePolicy.
	if isEmptyResourcePolicy(res.Policy) {
		return nil, nil
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
	// Treat a nil policy and an empty ("{}") policy document as equivalent. A
	// stream with no resource-based policy has GetResourcePolicy return an empty
	// document rather than a ResourceNotFoundException, which must not register
	// as a diff against an unset desired policy.
	aEmpty := isEmptyResourcePolicy(a.ko.Spec.ResourcePolicy)
	bEmpty := isEmptyResourcePolicy(b.ko.Spec.ResourcePolicy)
	if aEmpty && bEmpty {
		return
	}
	if aEmpty != bEmpty {
		delta.Add("Spec.ResourcePolicy", a.ko.Spec.ResourcePolicy, b.ko.Spec.ResourcePolicy)
		return
	}

	// At this point, both policies are non-empty. We need to compare their JSON content.
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

// isEmptyResourcePolicy reports whether the given policy document represents the
// absence of a resource-based policy. A nil pointer, a blank string, or a
// well-formed policy document with no statements (e.g. "{}") are all treated as
// "no policy".
func isEmptyResourcePolicy(policy *string) bool {
	if policy == nil || strings.TrimSpace(*policy) == "" {
		return true
	}
	var doc awsiampolicy.Policy
	if err := json.Unmarshal([]byte(*policy), &doc); err != nil {
		// A malformed but non-empty document is not "empty"; let the content
		// comparison surface the difference.
		return false
	}
	// Only a document with no identifying fields at all (e.g. "{}") represents
	// the absence of a policy — that is what GetResourcePolicy returns for a
	// stream with no resource-based policy. A document carrying an Id or Version
	// is a real, user-authored policy even if it currently has no statements.
	return doc.Id == "" && doc.Version == "" &&
		(doc.Statements == nil || len(doc.Statements.Values()) == 0)
}
