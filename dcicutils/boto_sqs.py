# The primary/initial purpose of this was to be able to use the SQS_URL environment variable to refer
# to a locally running ersatz version of SQS via an emulator like localstack (https://localstack.cloud).

import boto3
import os


def boto_sqs_client(**kwargs):
    """
    Creates and returns a boto3 sqs client object. If the SQS_URL environment variable is set then it
    will use that value as the endpoint_url for the boto3 sqs client, unless an explicit endpoint_url
    was passed in (via kwargs, per boto3.client convention) in which case that value will be used.
    """
    sqs_url = kwargs.get("endpoint_url") or os.environ.get("SQS_URL")
    return boto3.client("sqs", endpoint_url=sqs_url, **kwargs) if sqs_url else boto3.client("sqs", **kwargs)


def boto_sqs_resource(**kwargs):
    """
    Creates and returns a boto3 sqs resource object. If the SQS_URL environment variable is set then it
    will use that value as the endpoint_url for the boto3 sqs resource, unless an explicit endpoint_url
    was passed in (via kwargs, per boto3.resource convention) in which case that value will be used.
    """
    sqs_url = kwargs.get("endpoint_url") or os.environ.get("SQS_URL")
    return boto3.resource("sqs", endpoint_url=sqs_url, **kwargs) if sqs_url else boto3.resource("sqs", **kwargs)
