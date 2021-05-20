import pytest
from unittest import mock
from dcicutils.ecs_utils import ECSUtils
from dcicutils.misc_utils import ignored


@pytest.fixture(scope='module')
def ecs_utils():
    return ECSUtils(cluster_name='dummy-cluster')


def mock_update_service(*, cluster, service, forceNewDeployment):  # noQA - AWS chose mixed case argument name
    """ Mock matching the relevant API signature for below (we don't actually want
        to trigger an ECS deploy in unit testing.
    """
    ignored(cluster, service, forceNewDeployment)
    return


@pytest.mark.parametrize('service_name', [
    ECSUtils.WSGI, ECSUtils.INDEXER, ECSUtils.INGESTER
])
def test_ecs_utils_basic_should_proceed(ecs_utils, service_name):
    with mock.patch.object(ecs_utils.client, 'update_service', mock_update_service):
        ecs_utils.update_ecs_service(service_name=service_name)


@pytest.mark.parametrize('service_name', [
    'badservicename', 5, None
])
def test_ecs_utils_basic_should_fail(ecs_utils, service_name):
    with pytest.raises(Exception):
        ecs_utils.update_ecs_service(service_name=service_name)
