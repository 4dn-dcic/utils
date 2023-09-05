import boto3
import os
from typing import Optional
from dcicutils.misc_utils import override_environ

# Reach into the implementation details of the boto_monkey_patching module
# to get list of boto3 services for which we support monkey patching, and to
# get the environment variable name used to do the associated endpoint-url override.
from dcicutils.boto_monkey_patching import _boto_monkey_patching_services
from dcicutils.boto_monkey_patching import _boto_monkey_patching_endpoint_url_environ_name


_override_endpoint_url = "http://localhost:4566"


def _is_default_aws_endpoint_url(endpoint_url: str, service: str):
    return (endpoint_url == f"https://{service}.amazonaws.com" or
            endpoint_url == f"https://{service}.{os.environ.get('AWS_DEFAULT_REGION')}.amazonaws.com")


def _environ_overrides(service: str, endpoint_url: Optional[str]) -> dict:
    return {
        _boto_monkey_patching_endpoint_url_environ_name(service): endpoint_url,
        "AWS_DEFAULT_REGION": "us-east-1"
    }


def _test_boto_monkey_patching_client_without_overriding(service: str):
    with override_environ(**_environ_overrides(service, None)):
        s3_client = boto3.client(service)
        assert _is_default_aws_endpoint_url(s3_client.meta.endpoint_url, service)


def _test_boto_monkey_patching_resource_without_overriding(service: str):
    with override_environ(**_environ_overrides(service, None)):
        s3_resource = boto3.resource(service)
        assert _is_default_aws_endpoint_url(s3_resource.meta.client._endpoint.host, service)


def _test_boto_monkey_patching_client_with_overriding(service: str):
    with override_environ(**_environ_overrides(service, _override_endpoint_url)):
        s3_client = boto3.client(service)
        assert s3_client.meta.endpoint_url == _override_endpoint_url


def _test_boto_monkey_patching_resource_with_overriding(service: str):
    with override_environ(**_environ_overrides(service, _override_endpoint_url)):
        s3_resource = boto3.resource(service)
        assert s3_resource.meta.client._endpoint.host == _override_endpoint_url


def test_boto_monkey_patching_client_without_overriding():
    for service in _boto_monkey_patching_services:
        _test_boto_monkey_patching_client_without_overriding(service)


def test_boto_monkey_patching_resource_without_overriding():
    for service in _boto_monkey_patching_services:
        _test_boto_monkey_patching_resource_without_overriding(service)


def test_boto_monkey_patching_client_with_overriding():
    for service in _boto_monkey_patching_services:
        _test_boto_monkey_patching_client_with_overriding(service)


def test_boto_monkey_patching_resource_with_overriding():
    for service in _boto_monkey_patching_services:
        _test_boto_monkey_patching_resource_with_overriding(service)
