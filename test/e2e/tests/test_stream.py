# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
#	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Integration tests for the Kinesis Stream resource"""

import json
import logging
import time

import pytest

from acktest.aws.identity import get_region, get_account_id
from acktest.k8s import condition
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from acktest import tags
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_kinesis_resource
from e2e.replacement_values import REPLACEMENT_VALUES
from e2e import stream

RESOURCE_PLURAL = "streams"

DELETE_WAIT_AFTER_SECONDS = 10
CHECK_STATUS_WAIT_SECONDS = 10
MODIFY_WAIT_AFTER_SECONDS = 30


@service_marker
@pytest.mark.canary
class TestStream:
    def test_crud(self):
        stream_name = random_suffix_name("my-simple-stream", 24)
        shard_count = "1"

        replacements = REPLACEMENT_VALUES.copy()
        replacements['STREAM_NAME'] = stream_name
        replacements['SHARD_COUNT'] = shard_count

        resource_data = load_kinesis_resource(
            "stream_simple",
            additional_replacements=replacements,
        )

        ref = k8s.CustomResourceReference(
            CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
            stream_name, namespace="default",
        )
        k8s.create_custom_resource(ref, resource_data)
        cr = k8s.wait_resource_consumed_by_controller(ref)

        stream.wait_until_exists(stream_name)

        time.sleep(CHECK_STATUS_WAIT_SECONDS)

        assert cr["status"]["streamStatus"] == "ACTIVE"
        condition.assert_synced(ref)

        latest = stream.get(stream_name)
        assert latest is not None
        assert latest['StreamName'] == stream_name
        assert int(latest['OpenShardCount']) == int(shard_count)

        assert 'tags' in cr['spec']
        user_tags = cr['spec']['tags']

        response_tags = stream.get_tags(stream_name)

        tags.assert_ack_system_tags(
            tags=response_tags,
        )

        user_tags = [{"Key": key, "Value": value}  for key, value in user_tags.items()]
        tags.assert_equal_without_ack_tags(
            expected=user_tags,
            actual=response_tags,
        )

        # Test the code paths that update shard count
        shard_count = "2"
        updates = {
            "spec": {
                "shardCount": int(shard_count),
                "tags":
                    {
                        "another": "here",
                    }

            },

        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        cr = k8s.get_resource(ref)

        latest = stream.get(stream_name)
        assert latest is not None
        assert int(latest['OpenShardCount']) == int(shard_count)
        assert int(latest["OpenShardCount"]) == int(cr["status"]["openShardCount"])

        # validate tags
        assert 'tags' in cr['spec']
        user_tags = cr['spec']['tags']

        response_tags = stream.get_tags(stream_name)

        tags.assert_ack_system_tags(
            tags=response_tags,
        )

        user_tags = [{"Key": key, "Value": value}  for key, value in user_tags.items()]

        tags.assert_equal_without_ack_tags(
            expected=user_tags,
            actual=response_tags,
        )

        k8s.delete_custom_resource(ref)

        time.sleep(DELETE_WAIT_AFTER_SECONDS)

        stream.wait_until_deleted(stream_name)

    def test_resource_policy(self):
        stream_name = random_suffix_name("stream-with-policy", 32)
        shard_count = "1"
        account_id = get_account_id()
        region = get_region()

        resource_policy = {
            "Version": "2012-10-17",
            "Id": "ack-stream-with-policy",
            "Statement": [
                {
                    "Sid": "EnableResourcePolicyOnStream",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": f"arn:aws:iam::{account_id}:root"
                    },
                    "Action": [
                        "kinesis:GetRecords",
                        "kinesis:GetShardIterator",
                        "kinesis:DescribeStreamSummary"
                    ],
                    "Resource": f"arn:aws:kinesis:{region}:{account_id}:stream/{stream_name}"
                }
            ]
        }

        replacements = REPLACEMENT_VALUES.copy()
        replacements['STREAM_NAME'] = stream_name
        replacements['SHARD_COUNT'] = shard_count
        replacements['RESOURCE_POLICY'] = json.dumps(resource_policy)

        resource_data = load_kinesis_resource(
            "stream_resource_policy",
            additional_replacements=replacements,
        )

        ref = k8s.CustomResourceReference(
            CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
            stream_name, namespace="default",
        )
        k8s.create_custom_resource(ref, resource_data)
        cr = k8s.wait_resource_consumed_by_controller(ref)

        stream.wait_until_exists(stream_name)
        time.sleep(CHECK_STATUS_WAIT_SECONDS)

        assert cr is not None
        assert k8s.get_resource_exists(ref)

        # The resource policy is applied after the stream is created, so wait
        # until the controller reports the resource as synced.
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)

        cr = k8s.wait_resource_consumed_by_controller(ref)
        stream_arn = cr["status"]["ackResourceMetadata"]["arn"]

        # Verify the resource policy was attached to the stream
        policy = stream.get_resource_policy(stream_arn)
        assert policy is not None
        assert 'ack-stream-with-policy' in policy

        # Update the resource policy and verify the change is propagated
        resource_policy['Id'] = 'updated-stream-policy'
        updates = {
            "spec": {
                "resourcePolicy": json.dumps(resource_policy)
            }
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)

        policy = stream.get_resource_policy(stream_arn)
        assert policy is not None
        assert 'updated-stream-policy' in policy

        # Remove the resource policy by clearing the field
        cr = k8s.wait_resource_consumed_by_controller(ref)
        cr["spec"]["resourcePolicy"] = None
        k8s.patch_custom_resource(ref, cr)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=10)

        # Verify the resource policy was deleted
        assert stream.get_resource_policy(stream_arn) is None

        k8s.delete_custom_resource(ref)
        time.sleep(DELETE_WAIT_AFTER_SECONDS)
        stream.wait_until_deleted(stream_name)

    def test_enforce_consumer_deletion(self):
        stream_name = random_suffix_name("stream-enforce-del", 24)
        shard_count = "1"

        replacements = REPLACEMENT_VALUES.copy()
        replacements['STREAM_NAME'] = stream_name
        replacements['SHARD_COUNT'] = shard_count

        resource_data = load_kinesis_resource(
            "stream_enforce_consumer_deletion",
            additional_replacements=replacements,
        )

        ref = k8s.CustomResourceReference(
            CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
            stream_name, namespace="default",
        )
        k8s.create_custom_resource(ref, resource_data)
        cr = k8s.wait_resource_consumed_by_controller(ref)

        stream.wait_until_exists(stream_name)
        time.sleep(CHECK_STATUS_WAIT_SECONDS)

        assert cr is not None
        condition.assert_synced(ref)

        cr = k8s.get_resource(ref)
        assert cr["spec"]["enforceConsumerDeletion"] is True
        stream_arn = cr["status"]["ackResourceMetadata"]["arn"]

        # Register a consumer. With a registered consumer, a plain DeleteStream
        # would fail with ResourceInUseException; enforceConsumerDeletion=true
        # must force the consumer to be removed along with the stream.
        consumer_name = random_suffix_name("consumer", 24)
        stream.register_consumer(stream_arn, consumer_name)
        stream.wait_until_consumer_active(stream_arn, consumer_name)

        # Delete the stream CR; the enforced deletion should succeed.
        k8s.delete_custom_resource(ref)
        time.sleep(DELETE_WAIT_AFTER_SECONDS)
        stream.wait_until_deleted(stream_name)

        # The consumer should have been removed together with the stream.
        assert stream.get_consumer(stream_arn, consumer_name) is None

    def test_max_record_size(self):
        stream_name = random_suffix_name("stream-max-rec", 24)
        shard_count = "1"

        replacements = REPLACEMENT_VALUES.copy()
        replacements['STREAM_NAME'] = stream_name
        replacements['SHARD_COUNT'] = shard_count
        replacements['MAX_RECORD_SIZE'] = "1024"

        resource_data = load_kinesis_resource(
            "stream_max_record_size",
            additional_replacements=replacements,
        )

        ref = k8s.CustomResourceReference(
            CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
            stream_name, namespace="default",
        )
        k8s.create_custom_resource(ref, resource_data)
        cr = k8s.wait_resource_consumed_by_controller(ref)

        stream.wait_until_exists(stream_name)
        time.sleep(CHECK_STATUS_WAIT_SECONDS)

        assert cr is not None
        condition.assert_synced(ref)

        # The max record size should have been applied at creation time.
        latest = stream.get(stream_name)
        assert latest is not None
        assert int(latest['MaxRecordSizeInKiB']) == 1024

        # Update the max record size; this is applied via the dedicated
        # UpdateMaxRecordSize API rather than UpdateShardCount.
        updates = {
            "spec": {
                "maxRecordSizeInKiB": 2048
            }
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=15)

        latest = stream.get(stream_name)
        assert int(latest['MaxRecordSizeInKiB']) == 2048

        cr = k8s.get_resource(ref)
        assert int(cr["spec"]["maxRecordSizeInKiB"]) == 2048

        k8s.delete_custom_resource(ref)
        time.sleep(DELETE_WAIT_AFTER_SECONDS)
        stream.wait_until_deleted(stream_name)

    def test_shard_level_metrics(self):
        stream_name = random_suffix_name("stream-slm", 24)
        shard_count = "1"

        replacements = REPLACEMENT_VALUES.copy()
        replacements['STREAM_NAME'] = stream_name
        replacements['SHARD_COUNT'] = shard_count

        resource_data = load_kinesis_resource(
            "stream_shard_level_metrics",
            additional_replacements=replacements,
        )

        ref = k8s.CustomResourceReference(
            CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
            stream_name, namespace="default",
        )
        k8s.create_custom_resource(ref, resource_data)
        cr = k8s.wait_resource_consumed_by_controller(ref)

        stream.wait_until_exists(stream_name)
        time.sleep(CHECK_STATUS_WAIT_SECONDS)

        assert cr is not None
        # Shard-level metrics are not part of CreateStream; they are applied via
        # EnableEnhancedMonitoring on the reconcile after creation, so the
        # resource only reports Synced once they have been enabled.
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=15)

        assert stream.get_shard_level_metrics(stream_name) == {
            "IncomingBytes", "OutgoingBytes",
        }

        # Update the set: drop OutgoingBytes and add IncomingRecords. This
        # exercises both the Enable and Disable EnhancedMonitoring paths.
        updates = {
            "spec": {
                "shardLevelMetrics": ["IncomingBytes", "IncomingRecords"]
            }
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=15)

        assert stream.get_shard_level_metrics(stream_name) == {
            "IncomingBytes", "IncomingRecords",
        }

        # Clearing the field disables all shard-level metrics.
        updates = {
            "spec": {
                "shardLevelMetrics": None
            }
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)
        assert k8s.wait_on_condition(ref, "ACK.ResourceSynced", "True", wait_periods=15)

        assert stream.get_shard_level_metrics(stream_name) == set()

        k8s.delete_custom_resource(ref)
        time.sleep(DELETE_WAIT_AFTER_SECONDS)
        stream.wait_until_deleted(stream_name)
