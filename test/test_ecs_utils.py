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


def mock_describe_services_completed_deployment(*, cluster, services):
    """ Mocks the (important) structure of ecs.describe_services to simulate a completed
        deployment state.
    """
    ignored(cluster), ignored(services)
    return {
        'services': [
            {
                'serviceArn': 'dummy-arn',
                'deployments': [
                    {'id': 'ecs-svc/3239640454637807340', 'status': 'PRIMARY',
                     'taskDefinition': 'dummy-task-definition',
                     'desiredCount': 4, 'pendingCount': 0, 'runningCount': 4, 'failedTasks': 0,
                     'capacityProviderStrategy': [{'capacityProvider': 'FARGATE', 'weight': 0, 'base': 0},
                                                  {'capacityProvider': 'FARGATE_SPOT', 'weight': 1, 'base': 8}],
                     'platformVersion': '1.4.0', 'networkConfiguration': {
                        'awsvpcConfiguration': {'subnets': ['dummy-subnet1', 'dummy-subnet2'],
                                                'securityGroups': ['dummy-sg'],
                                                'assignPublicIp': 'DISABLED'}}, 'rolloutState': 'COMPLETED',
                     'rolloutStateReason': 'ECS deployment ecs-svc/3239640454637807340 completed.'}
                ]
            }
        ]
    }


def mock_describe_services_active_deployment(*, cluster, services):
    """ Mocks the (important) structure of ecs.describe_services to simulate an active
        deployment state. Must match signature of API as it is called in the method.
    """
    ignored(cluster), ignored(services)
    return {
        'services': [
            {
                'serviceArn': 'dummy-arn',
                'deployments': [
                    {'id': 'ecs-svc/3239640454637807340', 'status': 'PRIMARY',
                     'taskDefinition': 'dummy-task-definition',
                     'desiredCount': 4, 'pendingCount': 4, 'runningCount': 0, 'failedTasks': 0,
                     'capacityProviderStrategy': [{'capacityProvider': 'FARGATE', 'weight': 0, 'base': 0},
                                                  {'capacityProvider': 'FARGATE_SPOT', 'weight': 1, 'base': 8}],
                     'platformVersion': '1.4.0', 'networkConfiguration': {
                        'awsvpcConfiguration': {'subnets': ['dummy-subnet1', 'dummy-subnet2'],
                                                'securityGroups': ['dummy-sg'],
                                                'assignPublicIp': 'DISABLED'}}, 'rolloutState': 'Active',
                     'rolloutStateReason': 'ECS deployment ecs-svc/3239640454637807340 in progress.'}
                ]
            },
            {
                'serviceArn': 'dummy-arn',
                'deployments': [
                    {'id': 'ecs-svc/3239640454637807340', 'status': 'PRIMARY',
                     'taskDefinition': 'dummy-task-definition',
                     'desiredCount': 4, 'pendingCount': 0, 'runningCount': 4, 'failedTasks': 0,
                     'capacityProviderStrategy': [{'capacityProvider': 'FARGATE', 'weight': 0, 'base': 0},
                                                  {'capacityProvider': 'FARGATE_SPOT', 'weight': 1, 'base': 8}],
                     'platformVersion': '1.4.0', 'networkConfiguration': {
                        'awsvpcConfiguration': {'subnets': ['dummy-subnet1', 'dummy-subnet2'],
                                                'securityGroups': ['dummy-sg'],
                                                'assignPublicIp': 'DISABLED'}}, 'rolloutState': 'COMPLETED',
                     'rolloutStateReason': 'ECS deployment ecs-svc/3239640454637807340 completed.'}
                ]
            }
        ]
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


@pytest.mark.parametrize('mock_response,result', [
    (mock_describe_services_active_deployment, True),
    (mock_describe_services_completed_deployment, False)
])
def test_ecs_utils_active_deployment(ecs_utils, mock_response, result):
    """ Tests processing a mock responses from boto3 for an active deployment """
    with mock.patch.object(ecs_utils.client, 'describe_services', mock_response):
        assert ecs_utils.service_has_active_deployment(
            cluster_name='dummy-cluster',
            services=['dummy-service']) == result
