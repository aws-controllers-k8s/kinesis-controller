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

import logging
import time

import pytest

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
        retention_hours = "48"

        replacements = REPLACEMENT_VALUES.copy()
        replacements['STREAM_NAME'] = stream_name
        replacements['SHARD_COUNT'] = shard_count
        replacements['RETENTION_PERIOD_HOURS'] = retention_hours
        

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
        assert int(latest['RetentionPeriodHours']) == int(retention_hours)

        # Test the code paths that update shard count, retention hours, and tags
        shard_count = "2"
        retention_hours = "72"
        updates = {
            "spec": {
                "shardCount": int(shard_count),
                "retentionPeriodHours": int(retention_hours),
                "tags": {
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
        assert int(latest['RetentionPeriodHours']) == int(retention_hours)

        # validate tags
        assert 'tags' in cr['spec']
        user_tags = cr['spec']['tags']

        response_tags = stream.get_tags(stream_name)

        tags.assert_ack_system_tags(
            tags=response_tags,
        )

        user_tags = [{"Key": key, "Value": value} for key, value in user_tags.items()]

        tags.assert_equal_without_ack_tags(
            expected=user_tags,
            actual=response_tags,
        )
        # Test the code path that sets RetentionPeriodHours to 23
        retention_hours = "23"
        updates = {
            "spec": {
                "retentionPeriodHours": int(retention_hours),
            },
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        cr = k8s.get_resource(ref)
        assert cr['status']['conditions'][0]['message'] == "the desired retention period must be between 24 and 8760 hours"

       
        # Test the code paths that update encryption type and key ID
        encryption_type = "NONE"
        key_id = ""
        retention_hours = "48" # reset the retention period to 48 hours
       
        updates = {
            "spec": {
                "encryptionType": encryption_type,
                "keyID": key_id,
                "retentionPeriodHours": int(retention_hours),
            },
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        cr = k8s.get_resource(ref)
        latest = stream.get(stream_name)
        assert latest is not None
        assert latest['EncryptionType'] == encryption_type

        # Test the code paths that update encryption type to KMS and without key ID
        encryption_type = "KMS"
        key_id = ""
       
        updates = {
            "spec": {
                "encryptionType": encryption_type,
                "keyID": key_id,
            },
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        cr = k8s.get_resource(ref)
        assert cr['status']['conditions'][0]['message'] == "KMS encryption type requires a KeyID"

        # Test the code paths that update encryption type to NONE and with key ID
        encryption_type = ""
        key_id = "arn:aws:kms:testRegion:testAccountId:key/testKeyId"
       
        updates = {
            "spec": {
                "encryptionType": encryption_type,
                "keyID": key_id,
            },
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        cr = k8s.get_resource(ref)
        assert cr['status']['conditions'][0]['message'] == "cannot specify KeyID with NONE encryption type"

         # Test the code paths that update encryption type and key ID
        encryption_type = "KMS"
        key_id = "arn:aws:kms:us-west-2:179783686121:key/a993c326-70c4-400e-84b8-d165773014aa"
       
        updates = {
            "spec": {
                "encryptionType": encryption_type,
                "keyID": key_id,
            },
        }
        k8s.patch_custom_resource(ref, updates)
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        cr = k8s.get_resource(ref)

        latest = stream.get(stream_name)
        assert latest is not None
        assert latest['EncryptionType'] == encryption_type
        assert latest['KeyId'] == key_id


        k8s.delete_custom_resource(ref)

        time.sleep(DELETE_WAIT_AFTER_SECONDS)

        stream.wait_until_deleted(stream_name)
