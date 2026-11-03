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

package tags

import (
	"context"
	"errors"
	"maps"
	"slices"
	"testing"

	svcsdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
)

type fakeTagsClient struct {
	added     *svcsdk.AddTagsToStreamInput
	removed   *svcsdk.RemoveTagsFromStreamInput
	addErr    error
	removeErr error
}

func (c *fakeTagsClient) AddTagsToStream(
	_ context.Context,
	params *svcsdk.AddTagsToStreamInput,
	_ ...func(*svcsdk.Options),
) (*svcsdk.AddTagsToStreamOutput, error) {
	c.added = params
	return &svcsdk.AddTagsToStreamOutput{}, c.addErr
}

func (c *fakeTagsClient) ListTagsForStream(
	_ context.Context,
	_ *svcsdk.ListTagsForStreamInput,
	_ ...func(*svcsdk.Options),
) (*svcsdk.ListTagsForStreamOutput, error) {
	return &svcsdk.ListTagsForStreamOutput{}, nil
}

func (c *fakeTagsClient) RemoveTagsFromStream(
	_ context.Context,
	params *svcsdk.RemoveTagsFromStreamInput,
	_ ...func(*svcsdk.Options),
) (*svcsdk.RemoveTagsFromStreamOutput, error) {
	c.removed = params
	return &svcsdk.RemoveTagsFromStreamOutput{}, c.removeErr
}

type apiCall struct {
	opType string
	opID   string
	err    error
}

type fakeMetricsRecorder struct {
	calls []apiCall
}

func (r *fakeMetricsRecorder) RecordAPICall(opType, opID string, err error) {
	r.calls = append(r.calls, apiCall{opType, opID, err})
}

func strPtr(s string) *string {
	return &s
}

func TestSyncResourceTags(t *testing.T) {
	streamName := "test-stream"
	tests := []struct {
		name         string
		latest       map[string]*string
		desired      map[string]*string
		wantAdded    map[string]string
		wantRemoved  []string
		wantAPICalls []apiCall
	}{
		{
			name:    "no-op when tags are identical",
			latest:  map[string]*string{"a": strPtr("1")},
			desired: map[string]*string{"a": strPtr("1")},
		},
		{
			name:      "add new tags",
			latest:    map[string]*string{},
			desired:   map[string]*string{"a": strPtr("1")},
			wantAdded: map[string]string{"a": "1"},
			wantAPICalls: []apiCall{
				{"UPDATE", "AddTagsToStream", nil},
			},
		},
		{
			name:        "remove tags",
			latest:      map[string]*string{"a": strPtr("1"), "b": strPtr("2")},
			desired:     map[string]*string{},
			wantRemoved: []string{"a", "b"},
			wantAPICalls: []apiCall{
				{"UPDATE", "RemoveTagsFromStream", nil},
			},
		},
		{
			name:      "value change only adds",
			latest:    map[string]*string{"a": strPtr("1")},
			desired:   map[string]*string{"a": strPtr("2")},
			wantAdded: map[string]string{"a": "2"},
			wantAPICalls: []apiCall{
				{"UPDATE", "AddTagsToStream", nil},
			},
		},
		{
			name:        "add, remove and change together",
			latest:      map[string]*string{"a": strPtr("1"), "b": strPtr("2")},
			desired:     map[string]*string{"a": strPtr("9"), "c": strPtr("3")},
			wantAdded:   map[string]string{"a": "9", "c": "3"},
			wantRemoved: []string{"b"},
			wantAPICalls: []apiCall{
				{"UPDATE", "RemoveTagsFromStream", nil},
				{"UPDATE", "AddTagsToStream", nil},
			},
		},
		{
			name:    "nil value matches nil value without panic",
			latest:  map[string]*string{"a": nil},
			desired: map[string]*string{"a": nil},
		},
		{
			name:    "empty-string pointer in latest equals nil in desired",
			latest:  map[string]*string{"a": strPtr("")},
			desired: map[string]*string{"a": nil},
		},
		{
			name:      "nil desired value is added as empty string without panic",
			latest:    map[string]*string{},
			desired:   map[string]*string{"a": nil},
			wantAdded: map[string]string{"a": ""},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeTagsClient{}
			mr := &fakeMetricsRecorder{}
			err := SyncResourceTags(
				context.Background(),
				client,
				mr,
				&streamName,
				tc.latest,
				tc.desired,
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(tc.wantAdded) == 0 {
				if client.added != nil {
					t.Errorf("expected no AddTagsToStream call, got tags %v", client.added.Tags)
				}
			} else {
				if client.added == nil {
					t.Fatalf("expected AddTagsToStream call with tags %v, got none", tc.wantAdded)
				}
				if client.added.StreamName == nil || *client.added.StreamName != streamName {
					t.Errorf("AddTagsToStream StreamName = %v, want %q", client.added.StreamName, streamName)
				}
				if !maps.Equal(client.added.Tags, tc.wantAdded) {
					t.Errorf("added tags = %v, want %v", client.added.Tags, tc.wantAdded)
				}
			}

			if len(tc.wantRemoved) == 0 {
				if client.removed != nil {
					t.Errorf("expected no RemoveTagsFromStream call, got keys %v", client.removed.TagKeys)
				}
			} else {
				if client.removed == nil {
					t.Fatalf("expected RemoveTagsFromStream call with keys %v, got none", tc.wantRemoved)
				}
				if client.removed.StreamName == nil || *client.removed.StreamName != streamName {
					t.Errorf("RemoveTagsFromStream StreamName = %v, want %q", client.removed.StreamName, streamName)
				}
				gotKeys := slices.Clone(client.removed.TagKeys)
				wantKeys := slices.Clone(tc.wantRemoved)
				slices.Sort(gotKeys)
				slices.Sort(wantKeys)
				if !slices.Equal(gotKeys, wantKeys) {
					t.Errorf("removed keys = %v, want %v", client.removed.TagKeys, tc.wantRemoved)
				}
			}

			if tc.wantAPICalls != nil && !slices.Equal(mr.calls, tc.wantAPICalls) {
				t.Errorf("RecordAPICall calls = %+v, want %+v", mr.calls, tc.wantAPICalls)
			}
		})
	}
}

func TestSyncResourceTags_APIErrors(t *testing.T) {
	streamName := "test-stream"
	latest := map[string]*string{"a": strPtr("1"), "b": strPtr("2")}
	desired := map[string]*string{"a": strPtr("9"), "c": strPtr("3")}

	t.Run("RemoveTagsFromStream error stops AddTagsToStream", func(t *testing.T) {
		removeErr := errors.New("remove failed")
		client := &fakeTagsClient{removeErr: removeErr}
		mr := &fakeMetricsRecorder{}
		err := SyncResourceTags(context.Background(), client, mr, &streamName, latest, desired)
		if !errors.Is(err, removeErr) {
			t.Fatalf("expected error %v, got %v", removeErr, err)
		}
		if client.removed == nil {
			t.Errorf("RemoveTagsFromStream was not called")
		}
		if client.added != nil {
			t.Errorf("AddTagsToStream must not be called after RemoveTagsFromStream fails")
		}
		wantCalls := []apiCall{{"UPDATE", "RemoveTagsFromStream", removeErr}}
		if !slices.Equal(mr.calls, wantCalls) {
			t.Errorf("RecordAPICall calls = %+v, want %+v", mr.calls, wantCalls)
		}
	})

	t.Run("AddTagsToStream error is returned", func(t *testing.T) {
		addErr := errors.New("add failed")
		client := &fakeTagsClient{addErr: addErr}
		mr := &fakeMetricsRecorder{}
		err := SyncResourceTags(context.Background(), client, mr, &streamName, latest, desired)
		if !errors.Is(err, addErr) {
			t.Fatalf("expected error %v, got %v", addErr, err)
		}
		if client.removed == nil {
			t.Errorf("RemoveTagsFromStream was not called")
		}
		if client.added == nil {
			t.Errorf("AddTagsToStream was not called")
		}
		wantCalls := []apiCall{
			{"UPDATE", "RemoveTagsFromStream", nil},
			{"UPDATE", "AddTagsToStream", addErr},
		}
		if !slices.Equal(mr.calls, wantCalls) {
			t.Errorf("RecordAPICall calls = %+v, want %+v", mr.calls, wantCalls)
		}
	})
}
