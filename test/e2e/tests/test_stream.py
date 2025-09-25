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
        condition.assert_ready(ref)

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
