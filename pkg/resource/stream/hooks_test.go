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
	"fmt"
	"maps"
	"slices"
	"testing"

	ackmetrics "github.com/aws-controllers-k8s/runtime/pkg/metrics"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/smithy-go/middleware"

	svcapitypes "github.com/aws-controllers-k8s/kinesis-controller/apis/v1alpha1"
)

// capturedTagCalls records the inputs of the tag API calls made by syncTags.
type capturedTagCalls struct {
	add    *svcsdk.AddTagsToStreamInput
	remove *svcsdk.RemoveTagsFromStreamInput
}

// newCapturingClient returns a Kinesis client whose tag API calls are captured
// into c and short-circuited before any network request is made.
func newCapturingClient(c *capturedTagCalls) *svcsdk.Client {
	return svcsdk.New(svcsdk.Options{
		Region: "us-east-1",
		APIOptions: []func(*middleware.Stack) error{
			func(stack *middleware.Stack) error {
				return stack.Initialize.Add(middleware.InitializeMiddlewareFunc(
					"CaptureTagCalls",
					func(
						ctx context.Context,
						in middleware.InitializeInput,
						next middleware.InitializeHandler,
					) (middleware.InitializeOutput, middleware.Metadata, error) {
						switch p := in.Parameters.(type) {
						case *svcsdk.AddTagsToStreamInput:
							c.add = p
							return middleware.InitializeOutput{
								Result: &svcsdk.AddTagsToStreamOutput{},
							}, middleware.Metadata{}, nil
						case *svcsdk.RemoveTagsFromStreamInput:
							c.remove = p
							return middleware.InitializeOutput{
								Result: &svcsdk.RemoveTagsFromStreamOutput{},
							}, middleware.Metadata{}, nil
						default:
							return middleware.InitializeOutput{}, middleware.Metadata{},
								fmt.Errorf("unexpected input type %T", in.Parameters)
						}
					},
				), middleware.After)
			},
		},
	})
}

func strPtr(s string) *string {
	return &s
}

func newStream(tags map[string]*string) *resource {
	return &resource{
		ko: &svcapitypes.Stream{
			Spec: svcapitypes.StreamSpec{
				Name: strPtr("test-stream"),
				Tags: tags,
			},
		},
	}
}

func TestResourceManager_syncTags(t *testing.T) {
	tests := []struct {
		name       string
		latest     map[string]*string
		desired    map[string]*string
		wantAdd    map[string]string
		wantRemove []string
	}{
		{
			name:       "mixed",
			latest:     map[string]*string{"keep": strPtr("1"), "old": strPtr("x")},
			desired:    map[string]*string{"keep": strPtr("2"), "new": strPtr("y")},
			wantAdd:    map[string]string{"keep": "2", "new": "y"},
			wantRemove: []string{"old"},
		},
		{
			name:    "add only",
			latest:  map[string]*string{},
			desired: map[string]*string{"a": strPtr("1")},
			wantAdd: map[string]string{"a": "1"},
		},
		{
			name:       "remove only",
			latest:     map[string]*string{"a": strPtr("1")},
			desired:    map[string]*string{},
			wantRemove: []string{"a"},
		},
		{
			name:    "identical",
			latest:  map[string]*string{"a": strPtr("1")},
			desired: map[string]*string{"a": strPtr("1")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := &capturedTagCalls{}
			rm := &resourceManager{
				sdkapi:  newCapturingClient(calls),
				metrics: ackmetrics.NewMetrics("kinesis"),
			}

			latest := newStream(tt.latest)
			desired := newStream(tt.desired)

			if err := rm.syncTags(context.Background(), latest, desired); err != nil {
				t.Fatalf("syncTags() returned unexpected error: %v", err)
			}

			if tt.wantAdd == nil {
				if calls.add != nil {
					t.Errorf("unexpected AddTagsToStream call with tags %v", calls.add.Tags)
				}
			} else {
				if calls.add == nil {
					t.Fatalf("expected AddTagsToStream call, got none")
				}
				if calls.add.StreamName == nil || *calls.add.StreamName != "test-stream" {
					t.Errorf("AddTagsToStream StreamName = %v, want %q", calls.add.StreamName, "test-stream")
				}
				if !maps.Equal(calls.add.Tags, tt.wantAdd) {
					t.Errorf("AddTagsToStream Tags = %v, want %v", calls.add.Tags, tt.wantAdd)
				}
			}

			if tt.wantRemove == nil {
				if calls.remove != nil {
					t.Errorf("unexpected RemoveTagsFromStream call with keys %v", calls.remove.TagKeys)
				}
			} else {
				if calls.remove == nil {
					t.Fatalf("expected RemoveTagsFromStream call, got none")
				}
				if calls.remove.StreamName == nil || *calls.remove.StreamName != "test-stream" {
					t.Errorf("RemoveTagsFromStream StreamName = %v, want %q", calls.remove.StreamName, "test-stream")
				}
				if !slices.Equal(calls.remove.TagKeys, tt.wantRemove) {
					t.Errorf("RemoveTagsFromStream TagKeys = %v, want %v", calls.remove.TagKeys, tt.wantRemove)
				}
			}
		})
	}
}
