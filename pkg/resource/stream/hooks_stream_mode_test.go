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

func streamModeResource(mode *string) *resource {
	r := &resource{ko: &v1alpha1.Stream{Spec: v1alpha1.StreamSpec{}}}
	if mode != nil {
		r.ko.Spec.StreamModeDetails = &v1alpha1.StreamModeDetails{StreamMode: mode}
	}
	return r
}

func Test_compareStreamModeDetails(t *testing.T) {
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
			name:          "both unset",
			args:          args{a: streamModeResource(nil), b: streamModeResource(nil)},
			wantDifferent: false,
		},
		{
			// The reported phantom diff: user leaves the mode unset, AWS
			// reports the PROVISIONED default. ACK must not try to reconcile it.
			name:          "desired unset is never managed even when latest is the AWS default",
			args:          args{a: streamModeResource(nil), b: streamModeResource(aws.String("PROVISIONED"))},
			wantDifferent: false,
		},
		{
			name:          "desired set, latest unset",
			args:          args{a: streamModeResource(aws.String("ON_DEMAND")), b: streamModeResource(nil)},
			wantDifferent: true,
		},
		{
			name:          "desired equals latest",
			args:          args{a: streamModeResource(aws.String("ON_DEMAND")), b: streamModeResource(aws.String("ON_DEMAND"))},
			wantDifferent: false,
		},
		{
			name:          "desired differs from latest",
			args:          args{a: streamModeResource(aws.String("ON_DEMAND")), b: streamModeResource(aws.String("PROVISIONED"))},
			wantDifferent: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delta := &compare.Delta{}
			compareStreamModeDetails(delta, tt.args.a, tt.args.b)
			if got := delta.DifferentAt("Spec.StreamModeDetails"); got != tt.wantDifferent {
				t.Errorf("compareStreamModeDetails() difference = %v, want %v", got, tt.wantDifferent)
			}
		})
	}
}
