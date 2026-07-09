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

"""Utilities for working with StreamConsumer resources"""

import datetime
import time

import boto3
import pytest

DEFAULT_WAIT_UNTIL_EXISTS_TIMEOUT_SECONDS = 60*10
DEFAULT_WAIT_UNTIL_EXISTS_INTERVAL_SECONDS = 15
DEFAULT_WAIT_UNTIL_DELETED_TIMEOUT_SECONDS = 60*10
DEFAULT_WAIT_UNTIL_DELETED_INTERVAL_SECONDS = 15


def wait_until_exists(
        stream_arn: str,
        consumer_name: str,
        timeout_seconds: int = DEFAULT_WAIT_UNTIL_EXISTS_TIMEOUT_SECONDS,
        interval_seconds: int = DEFAULT_WAIT_UNTIL_EXISTS_INTERVAL_SECONDS,
    ) -> None:
    """Waits until a StreamConsumer with a supplied name is returned from the
    Kinesis DescribeStreamConsumer API.

    Usage:
        from e2e.stream_consumer import wait_until_exists

        wait_until_exists(stream_arn, consumer_name)

    Raises:
        pytest.fail upon timeout
    """
    now = datetime.datetime.now()
    timeout = now + datetime.timedelta(seconds=timeout_seconds)

    while True:
        if datetime.datetime.now() >= timeout:
            pytest.fail(
                "Timed out waiting for StreamConsumer to exist "
                "in Kinesis API"
            )
        time.sleep(interval_seconds)

        latest = get(stream_arn, consumer_name)
        if latest is not None:
            break


def wait_until_deleted(
        stream_arn: str,
        consumer_name: str,
        timeout_seconds: int = DEFAULT_WAIT_UNTIL_DELETED_TIMEOUT_SECONDS,
        interval_seconds: int = DEFAULT_WAIT_UNTIL_DELETED_INTERVAL_SECONDS,
    ) -> None:
    """Waits until a StreamConsumer with a supplied name is no longer returned
    from the Kinesis DescribeStreamConsumer API.

    Usage:
        from e2e.stream_consumer import wait_until_deleted

        wait_until_deleted(stream_arn, consumer_name)

    Raises:
        pytest.fail upon timeout
    """
    now = datetime.datetime.now()
    timeout = now + datetime.timedelta(seconds=timeout_seconds)

    while True:
        if datetime.datetime.now() >= timeout:
            pytest.fail(
                "Timed out waiting for StreamConsumer to be "
                "deleted in Kinesis API"
            )
        time.sleep(interval_seconds)

        latest = get(stream_arn, consumer_name)
        if latest is None:
            break


def get(stream_arn, consumer_name):
    """Returns a dict containing the ConsumerDescription record from the
    Kinesis DescribeStreamConsumer API.

    If no such StreamConsumer exists, returns None.
    """
    c = boto3.client('kinesis')
    try:
        resp = c.describe_stream_consumer(
            StreamARN=stream_arn,
            ConsumerName=consumer_name,
        )
        return resp['ConsumerDescription']
    except c.exceptions.ResourceNotFoundException:
        return None
