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

// warmThroughputResource builds an ON_DEMAND stream (warm throughput is an
// on-demand-only feature) with the given warm throughput value.
func warmThroughputResource(mibps *int64) *resource {
	return &resource{
		ko: &v1alpha1.Stream{
			Spec: v1alpha1.StreamSpec{
				StreamModeDetails:   &v1alpha1.StreamModeDetails{StreamMode: aws.String("ON_DEMAND")},
				WarmThroughputMiBps: mibps,
			},
		},
	}
}

func Test_compareWarmThroughput(t *testing.T) {
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
			name:          "both unset (off)",
			args:          args{a: warmThroughputResource(nil), b: warmThroughputResource(nil)},
			wantDifferent: false,
		},
		{
			name:          "explicit zero matches unset latest",
			args:          args{a: warmThroughputResource(aws.Int64(0)), b: warmThroughputResource(nil)},
			wantDifferent: false,
		},
		{
			name:          "desired unset turns off a set latest",
			args:          args{a: warmThroughputResource(nil), b: warmThroughputResource(aws.Int64(250))},
			wantDifferent: true,
		},
		{
			name:          "desired set, latest off",
			args:          args{a: warmThroughputResource(aws.Int64(100)), b: warmThroughputResource(nil)},
			wantDifferent: true,
		},
		{
			name:          "desired equals latest",
			args:          args{a: warmThroughputResource(aws.Int64(100)), b: warmThroughputResource(aws.Int64(100))},
			wantDifferent: false,
		},
		{
			name:          "desired differs from latest",
			args:          args{a: warmThroughputResource(aws.Int64(200)), b: warmThroughputResource(aws.Int64(100))},
			wantDifferent: true,
		},
		{
			name: "provisioned stream is never managed",
			args: args{
				a: streamModeResource(aws.String("PROVISIONED")),
				b: streamModeResource(aws.String("PROVISIONED")),
			},
			wantDifferent: false,
		},
	}
	// Give the provisioned-stream case differing warm throughput values to prove
	// the guard (not the value comparison) is what suppresses the delta.
	tests[len(tests)-1].args.a.ko.Spec.WarmThroughputMiBps = aws.Int64(100)
	tests[len(tests)-1].args.b.ko.Spec.WarmThroughputMiBps = aws.Int64(250)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delta := &compare.Delta{}
			compareWarmThroughput(delta, tt.args.a, tt.args.b)
			if got := delta.DifferentAt("Spec.WarmThroughputMiBps"); got != tt.wantDifferent {
				t.Errorf("compareWarmThroughput() difference = %v, want %v", got, tt.wantDifferent)
			}
		})
	}
}
