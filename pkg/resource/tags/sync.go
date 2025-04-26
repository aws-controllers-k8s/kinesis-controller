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

	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	ackutil "github.com/aws-controllers-k8s/runtime/pkg/util"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
)

type metricsRecorder interface {
	RecordAPICall(opType string, opID string, err error)
}

type tagsClient interface {
	AddTagsToStream(ctx context.Context, params *svcsdk.AddTagsToStreamInput, optFns ...func(*svcsdk.Options)) (*svcsdk.AddTagsToStreamOutput, error)
	ListTagsForStream(ctx context.Context, params *svcsdk.ListTagsForStreamInput, optFns ...func(*svcsdk.Options)) (*svcsdk.ListTagsForStreamOutput, error)
	RemoveTagsFromStream(ctx context.Context, params *svcsdk.RemoveTagsFromStreamInput, optFns ...func(*svcsdk.Options)) (*svcsdk.RemoveTagsFromStreamOutput, error)
}

// GetResourceTags retrieves a resource list of tags.
func GetResourceTags(
	ctx context.Context,
	client tagsClient,
	mr metricsRecorder,
	streamName *string,
) (map[string]*string, error) {
	listTagsForResourceResponse, err := client.ListTagsForStream(
		ctx,
		&svcsdk.ListTagsForStreamInput{
			StreamName: streamName,
		},
	)
	mr.RecordAPICall("GET", "ListTagsForStream", err)
	if err != nil {
		return nil, err
	}
	tags := make(map[string]*string)
	for _, tag := range listTagsForResourceResponse.Tags {
		tags[*tag.Key] = tag.Value
	}
	return tags, nil
}

// SyncResourceTags uses TagResource and UntagResource API Calls to add, remove
// and update resource tags.
func SyncResourceTags(
	ctx context.Context,
	client tagsClient,
	mr metricsRecorder,
	streamName string,
	latestTags map[string]*string,
	desiredTags map[string]*string,
) error {
	var err error
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("common.SyncResourceTags")
	defer func() {
		exit(err)
	}()

	addedOrUpdated, removed := computeTagsDelta(latestTags, desiredTags)

	if len(removed) > 0 {
		_, err = client.RemoveTagsFromStream(
			ctx,
			&svcsdk.RemoveTagsFromStreamInput{
				StreamName: &streamName,
				TagKeys:    removed,
			},
		)
		mr.RecordAPICall("UPDATE", "RemoveTagsFromStream", err)
		if err != nil {
			return err
		}
	}

	if len(addedOrUpdated) > 0 {
		_, err = client.AddTagsToStream(
			ctx,
			&svcsdk.AddTagsToStreamInput{
				StreamName: &streamName,
				Tags:       addedOrUpdated,
			},
		)
		mr.RecordAPICall("UPDATE", "AddTagsToStream", err)
		if err != nil {
			return err
		}
	}
	return nil
}

// computeTagsDelta compares two Tag arrays and return two different list
// containing the addedOrupdated and removed tags. The removed tags array
// only contains the tags Keys.
func computeTagsDelta(
	latest map[string]*string,
	desired map[string]*string,
) (addedOrUpdated map[string]string, removed []string) {
	var visitedKeys []string
mainLoop:
	for latestKey, latestValue := range latest {
		visitedKeys = append(visitedKeys, latestKey)
		for desiredKey, desiredValue := range desired {
			if latestKey == desiredKey {
				if !equalStrings(latestValue, desiredValue) {
					addedOrUpdated[desiredKey] = *desiredValue
				}
				continue mainLoop
			}
		}
		removed = append(removed, latestKey)
	}
	for desiredKey, desiredValue := range desired {
		if !ackutil.InStrings(desiredKey, visitedKeys) {
			addedOrUpdated[desiredKey] = *desiredValue
		}
	}
	return addedOrUpdated, removed
}

func equalStrings(a, b *string) bool {
	if a == nil {
		return b == nil || *b == ""
	}
	return (*a == "" && b == nil) || *a == *b
}
