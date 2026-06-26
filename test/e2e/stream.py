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

"""Utilities for working with Stream resources"""

import datetime
import time

import boto3
import pytest

DEFAULT_WAIT_UNTIL_EXISTS_TIMEOUT_SECONDS = 60*10
DEFAULT_WAIT_UNTIL_EXISTS_INTERVAL_SECONDS = 15
DEFAULT_WAIT_UNTIL_DELETED_TIMEOUT_SECONDS = 60*10
DEFAULT_WAIT_UNTIL_DELETED_INTERVAL_SECONDS = 15


def wait_until_exists(
        stream_name: str,
        timeout_seconds: int = DEFAULT_WAIT_UNTIL_EXISTS_TIMEOUT_SECONDS,
        interval_seconds: int = DEFAULT_WAIT_UNTIL_EXISTS_INTERVAL_SECONDS,
    ) -> None:
    """Waits until a Stream with a supplied name is returned from IAM GetStream
    API.

    Usage:
        from e2e.stream import wait_until_exists

        wait_until_exists(stream_name)

    Raises:
        pytest.fail upon timeout
    """
    now = datetime.datetime.now()
    timeout = now + datetime.timedelta(seconds=timeout_seconds)

    while True:
        if datetime.datetime.now() >= timeout:
            pytest.fail(
                "Timed out waiting for Stream to exist "
                "in IAM API"
            )
        time.sleep(interval_seconds)

        latest = get(stream_name)
        if latest is not None:
            break


def wait_until_deleted(
        stream_name: str,
        timeout_seconds: int = DEFAULT_WAIT_UNTIL_DELETED_TIMEOUT_SECONDS,
        interval_seconds: int = DEFAULT_WAIT_UNTIL_DELETED_INTERVAL_SECONDS,
    ) -> None:
    """Waits until a Stream with a supplied ID is no longer returned from
    the IAM API.

    Usage:
        from e2e.stream import wait_until_deleted

        wait_until_deleted(stream_name)

    Raises:
        pytest.fail upon timeout
    """
    now = datetime.datetime.now()
    timeout = now + datetime.timedelta(seconds=timeout_seconds)

    while True:
        if datetime.datetime.now() >= timeout:
            pytest.fail(
                "Timed out waiting for Stream to be "
                "deleted in IAM API"
            )
        time.sleep(interval_seconds)

        latest = get(stream_name)
        if latest is None:
            break


def get(stream_name):
    """Returns a dict containing the Stream record from the IAM API.

    If no such Stream exists, returns None.
    """
    c = boto3.client('kinesis')
    try:
        resp = c.describe_stream_summary(StreamName=stream_name)
        return resp['StreamDescriptionSummary']
    except c.exceptions.ResourceNotFoundException:
        return None

def get_shard_level_metrics(stream_name):
    """Returns the set of shard-level (enhanced) metrics currently enabled on
    the stream, or None if the stream does not exist.
    """
    summary = get(stream_name)
    if summary is None:
        return None
    metrics = set()
    for em in summary.get('EnhancedMonitoring', []):
        for m in em.get('ShardLevelMetrics', []):
            metrics.add(m)
    return metrics

def get_tags(stream_name):
    """Return tags for the stream

    If no such Stream exists, returns None.
    """
    c = boto3.client('kinesis')
    try:
        resp = c.list_tags_for_stream(StreamName=stream_name)
        return resp['Tags']
    except c.exceptions.ResourceNotFoundException:
        return None

def register_consumer(stream_arn, consumer_name):
    """Registers an enhanced fan-out consumer against the stream."""
    c = boto3.client('kinesis')
    return c.register_stream_consumer(
        StreamARN=stream_arn,
        ConsumerName=consumer_name,
    )


def get_consumer(stream_arn, consumer_name):
    """Returns the consumer description, or None if it does not exist."""
    c = boto3.client('kinesis')
    try:
        resp = c.describe_stream_consumer(
            StreamARN=stream_arn,
            ConsumerName=consumer_name,
        )
        return resp['ConsumerDescription']
    except c.exceptions.ResourceNotFoundException:
        return None


def wait_until_consumer_active(
        stream_arn: str,
        consumer_name: str,
        timeout_seconds: int = 60*5,
        interval_seconds: int = 15,
    ) -> None:
    """Waits until a registered stream consumer reports an ACTIVE status.

    Raises:
        pytest.fail upon timeout
    """
    now = datetime.datetime.now()
    timeout = now + datetime.timedelta(seconds=timeout_seconds)

    while True:
        if datetime.datetime.now() >= timeout:
            pytest.fail(
                "Timed out waiting for Stream consumer to become ACTIVE"
            )
        time.sleep(interval_seconds)

        consumer = get_consumer(stream_arn, consumer_name)
        if consumer is not None and consumer.get('ConsumerStatus') == 'ACTIVE':
            break


def get_resource_policy(stream_arn):
    """Returns the resource-based policy document attached to the stream with
    the supplied ARN, or None if no policy is attached.

    Kinesis has no dedicated PolicyNotFoundException; a stream with no attached
    policy surfaces as ResourceNotFoundException.
    """
    c = boto3.client('kinesis')
    try:
        resp = c.get_resource_policy(ResourceARN=stream_arn)
        if resp.get('Policy'):
            return resp['Policy']
        return None
    except c.exceptions.ResourceNotFoundException:
        return None