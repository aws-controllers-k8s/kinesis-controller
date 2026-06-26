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

func slmResource(metrics ...string) *resource {
	var list []*string
	for _, m := range metrics {
		list = append(list, aws.String(m))
	}
	return &resource{
		ko: &v1alpha1.Stream{
			Spec: v1alpha1.StreamSpec{
				ShardLevelMetrics: list,
			},
		},
	}
}

func Test_compareShardLevelMetrics(t *testing.T) {
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
			args:          args{a: slmResource(), b: slmResource()},
			wantDifferent: false,
		},
		{
			name:          "unset vs empty are equal",
			args:          args{a: slmResource(), b: &resource{ko: &v1alpha1.Stream{Spec: v1alpha1.StreamSpec{ShardLevelMetrics: []*string{}}}}},
			wantDifferent: false,
		},
		{
			name:          "same metrics, different order",
			args:          args{a: slmResource("IncomingBytes", "OutgoingBytes"), b: slmResource("OutgoingBytes", "IncomingBytes")},
			wantDifferent: false,
		},
		{
			name: "ALL is equal to the fully expanded metric set",
			args: args{
				a: slmResource("ALL"),
				b: slmResource(
					"IncomingBytes", "IncomingRecords", "OutgoingBytes",
					"OutgoingRecords", "WriteProvisionedThroughputExceeded",
					"ReadProvisionedThroughputExceeded", "IteratorAgeMilliseconds",
				),
			},
			wantDifferent: false,
		},
		{
			name:          "duplicate entries are ignored",
			args:          args{a: slmResource("IncomingBytes", "IncomingBytes"), b: slmResource("IncomingBytes")},
			wantDifferent: false,
		},
		{
			name:          "desired adds a metric",
			args:          args{a: slmResource("IncomingBytes", "OutgoingBytes"), b: slmResource("IncomingBytes")},
			wantDifferent: true,
		},
		{
			name:          "desired removes all metrics",
			args:          args{a: slmResource(), b: slmResource("IncomingBytes")},
			wantDifferent: true,
		},
		{
			name:          "disjoint metric sets",
			args:          args{a: slmResource("IncomingBytes"), b: slmResource("OutgoingBytes")},
			wantDifferent: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delta := &compare.Delta{}
			compareShardLevelMetrics(delta, tt.args.a, tt.args.b)
			if got := delta.DifferentAt("Spec.ShardLevelMetrics"); got != tt.wantDifferent {
				t.Errorf("compareShardLevelMetrics() difference = %v, want %v", got, tt.wantDifferent)
			}
		})
	}
}
