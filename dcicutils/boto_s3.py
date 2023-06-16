# The primary/initial purpose of this was to be able to use the S3_URL environment variable to refer
# to a locally running ersatz version of S3 via an emulator like localstack (https://localstack.cloud).

import boto3
import os


def boto_s3_client(**kwargs):
    """
    Creates and returns a boto3 s3 client object. If the S3_URL environment variable is set then it
    will use that value as the endpoint_url for the boto3 s3 client, unless an explicit endpoint_url
    was passed in (via kwargs, per boto3.client convention) in which case that value will be used.
    """
    s3_url = kwargs.get("endpoint_url") or os.environ.get("S3_URL")
    return boto3.client("s3", endpoint_url=s3_url, **kwargs) if s3_url else boto3.client("s3", **kwargs)


def boto_s3_resource(**kwargs):
    """
    Creates and returns a boto3 s3 resource object. If the S3_URL environment variable is set then it
    will use that value as the endpoint_url for the boto3 s3 resource, unless an explicit endpoint_url
    was passed in (via kwargs, per boto3.resource convention) in which case that value will be used.
    """
    s3_url = kwargs.get("endpoint_url") or os.environ.get("S3_URL")
    return boto3.resource("s3", endpoint_url=s3_url, **kwargs) if s3_url else boto3.resource("s3", **kwargs)
