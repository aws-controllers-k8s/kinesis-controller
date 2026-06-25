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
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws-controllers-k8s/kinesis-controller/apis/v1alpha1"
	compare "github.com/aws-controllers-k8s/runtime/pkg/compare"
)

func streamWithPolicy(policy *string) *resource {
	return &resource{
		ko: &v1alpha1.Stream{
			Spec: v1alpha1.StreamSpec{
				ResourcePolicy: policy,
			},
		},
	}
}

func Test_compareResourcePolicyDocument(t *testing.T) {
	type args struct {
		a *resource
		b *resource
	}
	tests := []struct {
		name          string
		args          args
		wantDifferent bool
	}{
		{
			name: "both policies are nil",
			args: args{
				a: streamWithPolicy(nil),
				b: streamWithPolicy(nil),
			},
			wantDifferent: false,
		},
		{
			name: "desired policy is set, latest policy is nil",
			args: args{
				a: streamWithPolicy(aws.String(`{"Version": "2012-10-17"}`)),
				b: streamWithPolicy(nil),
			},
			wantDifferent: true,
		},
		{
			name: "identical policy documents",
			args: args{
				a: streamWithPolicy(aws.String(`{
					"Version": "2012-10-17",
					"Statement": [
						{
							"Effect": "Allow",
							"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
							"Action": ["kinesis:GetRecords", "kinesis:GetShardIterator"],
							"Resource": "arn:aws:kinesis:us-west-2:123456789012:stream/my-stream"
						}
					]
				}`)),
				b: streamWithPolicy(aws.String(`{
					"Version": "2012-10-17",
					"Statement": [
						{
							"Effect": "Allow",
							"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
							"Action": ["kinesis:GetRecords", "kinesis:GetShardIterator"],
							"Resource": "arn:aws:kinesis:us-west-2:123456789012:stream/my-stream"
						}
					]
				}`)),
			},
			wantDifferent: false,
		},
		{
			name: "same policy content with different whitespace formatting",
			args: args{
				a: streamWithPolicy(aws.String(`{
					"Version": "2012-10-17",
					"Statement": [
						{
							"Effect": "Allow",
							"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
							"Action": ["kinesis:GetRecords", "kinesis:GetShardIterator"],
							"Resource": "arn:aws:kinesis:us-west-2:123456789012:stream/my-stream"
						}
					]
				}`)),
				b: streamWithPolicy(aws.String(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},"Action":["kinesis:GetRecords","kinesis:GetShardIterator"],"Resource":"arn:aws:kinesis:us-west-2:123456789012:stream/my-stream"}]}`)),
			},
			wantDifferent: false,
		},
		{
			name: "different effect in statement",
			args: args{
				a: streamWithPolicy(aws.String(`{
					"Version": "2012-10-17",
					"Statement": [
						{
							"Effect": "Allow",
							"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
							"Action": ["kinesis:GetRecords"],
							"Resource": "arn:aws:kinesis:us-west-2:123456789012:stream/my-stream"
						}
					]
				}`)),
				b: streamWithPolicy(aws.String(`{
					"Version": "2012-10-17",
					"Statement": [
						{
							"Effect": "Deny",
							"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
							"Action": ["kinesis:GetRecords"],
							"Resource": "arn:aws:kinesis:us-west-2:123456789012:stream/my-stream"
						}
					]
				}`)),
			},
			wantDifferent: true,
		},
		{
			name: "different actions in statement",
			args: args{
				a: streamWithPolicy(aws.String(`{
					"Version": "2012-10-17",
					"Statement": [
						{
							"Effect": "Allow",
							"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
							"Action": ["kinesis:GetRecords"],
							"Resource": "arn:aws:kinesis:us-west-2:123456789012:stream/my-stream"
						}
					]
				}`)),
				b: streamWithPolicy(aws.String(`{
					"Version": "2012-10-17",
					"Statement": [
						{
							"Effect": "Allow",
							"Principal": {"AWS": "arn:aws:iam::123456789012:root"},
							"Action": ["kinesis:PutRecord"],
							"Resource": "arn:aws:kinesis:us-west-2:123456789012:stream/my-stream"
						}
					]
				}`)),
			},
			wantDifferent: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delta := &compare.Delta{}
			compareResourcePolicyDocument(delta, tt.args.a, tt.args.b)
			if got := delta.DifferentAt("Spec.ResourcePolicy"); got != tt.wantDifferent {
				t.Errorf("compareResourcePolicyDocument() difference = %v, want %v", got, tt.wantDifferent)
			}
		})
	}
}
