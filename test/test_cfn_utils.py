from unittest import mock
from dcicutils.cloudformation_utils import get_ecs_real_url


class MockedCfnClient:
    """ Mocks boto3.client('cloudformation').describe_stacks """
    def __init__(self):
        pass

    @staticmethod
    def describe_stacks():
        return {
            'Stacks': [
                {
                    'Outputs': [
                        {
                            'OutputKey': 'Blah1',
                            'OutputValue': 'Blah1'
                        },
                        {
                            'OutputKey': 'ECSApplicationURLcgaphotseat',
                            'OutputValue': 'http://dummy-url2.org'
                        }
                    ]
                },
                {
                    'Outputs': [
                        {
                            'OutputKey': 'Blah2',
                            'OutputValue': 'Blah2'
                        },
                        {
                            'OutputKey': 'ECSApplicationURLcgapmastertest',
                            'OutputValue': 'http://dummy-url.org'
                        }
                    ]
                },
            ]
        }


def test_cfn_utils_get_ecs_real_url():
    """ Tests get_ecs_real_url using the mocked response above. """
    with mock.patch('boto3.client', return_value=MockedCfnClient()):
        real_url = get_ecs_real_url('cgap-mastertest')
        assert real_url == 'http://dummy-url.org'
        real_url = get_ecs_real_url('cgapmastertest')
        assert real_url == 'http://dummy-url.org'
        real_url = get_ecs_real_url('cgap-hotseat')
        assert real_url == 'http://dummy-url2.org'
        real_url = get_ecs_real_url('cgaphotseat')
        assert real_url == 'http://dummy-url2.org'
