import pytest
from unittest import mock
from dcicutils.ecs_utils import ECSUtils
from dcicutils.misc_utils import ignored


@pytest.fixture(scope='module')
def ecs_utils():
    return ECSUtils()


def mock_list_ecs_services(*, cluster):
    """ Mock API signature """
    ignored(cluster)
    return {
        'serviceArns':
            [
                'arn:aws:ecs:us-east-2:1234566777:service/c4-ecs-trial-alpha-stack-CGAPDockerCluster-Z4m1uYa2J11O/c4-ecs-trial-alpha-stack-CGAPIndexerService-YihcMquIc354',  # noQA: E501
                'arn:aws:ecs:us-east-2:1234566777:service/c4-ecs-trial-alpha-stack-CGAPDockerCluster-Z4m1uYa2J11O/c4-ecs-trial-alpha-stack-CGAPDeploymentService-NRyGGBTSnqbQ',  # noQA: E501
                'arn:aws:ecs:us-east-2:1234566777:service/c4-ecs-trial-alpha-stack-CGAPDockerCluster-Z4m1uYa2J11O/c4-ecs-trial-alpha-stack-CGAPIngesterService-QRcdjlE5ZJS1',  # noQA: E501
                'arn:aws:ecs:us-east-2:1234566777:service/c4-ecs-trial-alpha-stack-CGAPDockerCluster-Z4m1uYa2J11O/c4-ecs-trial-alpha-stack-CGAPWSGIService-oDZbeVVWjZMq'  # noQA: E501
            ],
        'ResponseMetadata':
            {'RequestId': 'not-a-uuid',
             'HTTPStatusCode': 200}
    }


def mock_update_service(*, cluster, service, forceNewDeployment):  # noQA - AWS chose mixed case argument name
    """ Mock matching the relevant API signature for below (we don't actually want
        to trigger an ECS deploy in unit testing.
    """
    ignored(cluster, service, forceNewDeployment)
    return


def test_ecs_utils_will_update_services(ecs_utils):
    """ Tests basic code interaction, no integrated testing right now. """
    with mock.patch.object(ecs_utils.client, 'list_services', mock_list_ecs_services):
        with mock.patch.object(ecs_utils.client, 'update_service', mock_update_service):
            ecs_utils.update_all_services(cluster_name='unused')
