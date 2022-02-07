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
            {
                'RequestId': 'not-a-uuid',
                'HTTPStatusCode': 200
            }
    }


def mock_list_ecs_tasks():
    """ list_task_definitions structure - for future tests """
    return {
        'taskDefinitionArns': [
            'arn:aws:ecs:us-east-1:262461168236:task-definition/c4-ecs-cgap-devtest-stack-CGAPDeployment-FwJgj7hSQA2p:1',  # noQA: E501
            'arn:aws:ecs:us-east-1:262461168236:task-definition/c4-ecs-cgap-devtest-stack-CGAPIndexer-iHDWcWOG5r9m:1',
            'arn:aws:ecs:us-east-1:262461168236:task-definition/c4-ecs-cgap-devtest-stack-CGAPIngester-tU6SCdUoTAT0:1',
            'arn:aws:ecs:us-east-1:262461168236:task-definition/c4-ecs-cgap-devtest-stack-CGAPInitialDeployment-FPYnleE9YwvH:1',  # noQA: E501
            'arn:aws:ecs:us-east-1:262461168236:task-definition/c4-ecs-cgap-devtest-stack-CGAPInitialDeployment-FPYnleE9YwvH:2',  # noQA: E501
            'arn:aws:ecs:us-east-1:262461168236:task-definition/c4-ecs-cgap-devtest-stack-CGAPportal-9kgkd5ZfVtxP:1',
            'arn:aws:ecs:us-east-1:262461168236:task-definition/c4-ecs-cgap-devtest-stack-CGAPportal-9kgkd5ZfVtxP:2',
            'arn:aws:ecs:us-east-1:262461168236:task-definition/c4-ecs-cgap-devtest-stack-CGAPportal-9kgkd5ZfVtxP:3',
        ],
        'ResponseMetadata': {
            'RequestId': '82c590ec-980e-4eea-8fea-2843b5ffdd6a',
            'HTTPStatusCode': 200,
            'HTTPHeaders': {
                'x-amzn-requestid': '82c590ec-980e-4eea-8fea-2843b5ffdd6a',
                'content-type': 'application/x-amz-json-1.1',
                'content-length': '1393',
                'date': 'Mon, 10 Jan 2022 19:52:06 GMT',
            },
            'RetryAttempts': 0,
        },
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
