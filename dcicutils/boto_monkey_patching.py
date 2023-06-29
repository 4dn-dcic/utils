# Module to monkey patch the boto3 client and resource functions to use a custom endpoint-url.
# Originally introduced June 2023 for overriding certain boto3 services (e.g. s3, sqs) to use the
# localstack utility, which provides a way to run some AWS services locally, for testing purposes.
# Currently only supported for S3 and SQS. To use this set the environment variables S3_URL and/or
# SQS_URL to the localstack URL, for example, http://localhost:4566.
# Reference: https://localstack.cloud

import boto3
import os
from typing import Optional


_boto_client_original = boto3.client
_boto_resource_original = boto3.resource
_boto_service_overrides_supported = [
    {"service": "s3", "env": "S3_URL"},
    {"service": "sqs", "env": "SQS_URL"}
]
# This will entirely disable this feature; for troubleshooting only.
_boto_monkey_patching_disabled = False

# For import only in test_boto_monkey_patching.
_boto_monkey_patching_services = [item["service"] for item in _boto_service_overrides_supported]


# For import only in test_boto_monkey_patching.
def _boto_monkey_patching_endpoint_url_environ_name(service: str) -> Optional[str]:
    for item in _boto_service_overrides_supported:
        if item["service"] == service:
            return item["env"]
    return None


def _setup_monkey_patching_kwargs(*args, **kwargs) -> dict:
    if not _boto_monkey_patching_disabled:
        endpoint_url = kwargs.get("endpoint_url")
        if not endpoint_url:
            for service_override in _boto_service_overrides_supported:
                if service_override["service"] in args:
                    endpoint_url = os.environ.get(service_override["env"])
                    if endpoint_url:
                        kwargs["endpoint_url"] = endpoint_url
                    break
    return kwargs


def _monkey_patched_boto_client(*args, **kwargs):
    kwargs = _setup_monkey_patching_kwargs(*args, **kwargs)
    return _boto_client_original(*args, **kwargs)


def _monkey_patched_boto_resource(*args, **kwargs):
    kwargs = _setup_monkey_patching_kwargs(*args, **kwargs)
    return _boto_resource_original(*args, **kwargs)


boto3.client = _monkey_patched_boto_client
boto3.resource = _monkey_patched_boto_resource
