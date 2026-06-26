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

func maxRecordSizeResource(kib *int64) *resource {
	return &resource{
		ko: &v1alpha1.Stream{
			Spec: v1alpha1.StreamSpec{
				MaxRecordSizeInKiB: kib,
			},
		},
	}
}

func Test_compareMaxRecordSize(t *testing.T) {
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
			args:          args{a: maxRecordSizeResource(nil), b: maxRecordSizeResource(nil)},
			wantDifferent: false,
		},
		{
			name:          "desired unset is never managed even when latest reports the default",
			args:          args{a: maxRecordSizeResource(nil), b: maxRecordSizeResource(aws.Int64(1024))},
			wantDifferent: false,
		},
		{
			name:          "desired set, latest unset",
			args:          args{a: maxRecordSizeResource(aws.Int64(2048)), b: maxRecordSizeResource(nil)},
			wantDifferent: true,
		},
		{
			name:          "desired equals latest",
			args:          args{a: maxRecordSizeResource(aws.Int64(2048)), b: maxRecordSizeResource(aws.Int64(2048))},
			wantDifferent: false,
		},
		{
			name:          "desired differs from latest",
			args:          args{a: maxRecordSizeResource(aws.Int64(4096)), b: maxRecordSizeResource(aws.Int64(1024))},
			wantDifferent: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delta := &compare.Delta{}
			compareMaxRecordSize(delta, tt.args.a, tt.args.b)
			if got := delta.DifferentAt("Spec.MaxRecordSizeInKiB"); got != tt.wantDifferent {
				t.Errorf("compareMaxRecordSize() difference = %v, want %v", got, tt.wantDifferent)
			}
		})
	}
}
