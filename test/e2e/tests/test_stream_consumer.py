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

"""Integration tests for the Kinesis StreamConsumer resource"""

import logging
import time

import pytest

from acktest.k8s import condition
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name
from e2e import service_marker, CRD_GROUP, CRD_VERSION, load_kinesis_resource
from e2e.replacement_values import REPLACEMENT_VALUES
from e2e import stream
from e2e import stream_consumer

STREAM_RESOURCE_PLURAL = "streams"
RESOURCE_PLURAL = "streamconsumers"

DELETE_WAIT_AFTER_SECONDS = 10
CHECK_STATUS_WAIT_SECONDS = 10


@service_marker
@pytest.mark.canary
class TestStreamConsumer:
    def test_crud(self):
        stream_name = random_suffix_name("my-consumer-stream", 24)
        consumer_name = random_suffix_name("my-simple-consumer", 24)
        shard_count = "1"

        replacements = REPLACEMENT_VALUES.copy()
        replacements['STREAM_NAME'] = stream_name
        replacements['SHARD_COUNT'] = shard_count
        replacements['CONSUMER_NAME'] = consumer_name

        # Create the Stream that the consumer will be registered against
        stream_resource_data = load_kinesis_resource(
            "stream_simple",
            additional_replacements=replacements,
        )

        stream_ref = k8s.CustomResourceReference(
            CRD_GROUP, CRD_VERSION, STREAM_RESOURCE_PLURAL,
            stream_name, namespace="default",
        )
        k8s.create_custom_resource(stream_ref, stream_resource_data)
        k8s.wait_resource_consumed_by_controller(stream_ref)

        stream.wait_until_exists(stream_name)

        latest_stream = stream.get(stream_name)
        assert latest_stream is not None
        stream_arn = latest_stream['StreamARN']

        # Create the StreamConsumer, referencing the Stream by name via
        # spec.streamRef
        resource_data = load_kinesis_resource(
            "stream_consumer_simple",
            additional_replacements=replacements,
        )

        ref = k8s.CustomResourceReference(
            CRD_GROUP, CRD_VERSION, RESOURCE_PLURAL,
            consumer_name, namespace="default",
        )
        k8s.create_custom_resource(ref, resource_data)
        cr = k8s.wait_resource_consumed_by_controller(ref)

        stream_consumer.wait_until_exists(stream_arn, consumer_name)

        time.sleep(CHECK_STATUS_WAIT_SECONDS)

        cr = k8s.get_resource(ref)
        assert cr["status"]["consumerARN"] is not None
        condition.assert_synced(ref)

        latest = stream_consumer.get(stream_arn, consumer_name)
        assert latest is not None
        assert latest['ConsumerName'] == consumer_name
        assert latest['ConsumerARN'] == cr["status"]["consumerARN"]

        k8s.delete_custom_resource(ref)

        time.sleep(DELETE_WAIT_AFTER_SECONDS)

        stream_consumer.wait_until_deleted(stream_arn, consumer_name)

        # Clean up the Stream
        k8s.delete_custom_resource(stream_ref)

        time.sleep(DELETE_WAIT_AFTER_SECONDS)

        stream.wait_until_deleted(stream_name)
