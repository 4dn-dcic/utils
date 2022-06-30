import os
import pytest

from dcicutils import cloudformation_utils
from dcicutils.ff_mocks import mocked_s3utils
from dcicutils.env_base import EnvBase
from dcicutils.misc_utils import override_environ
from dcicutils.qa_utils import (
    MockBoto3, MockBotoCloudFormationClient, MockBotoCloudFormationStack, MockBotoCloudFormationResourceSummary,
)
from unittest import mock


def test_camelize():

    assert cloudformation_utils.camelize('foo-bar') == 'FooBar'
    assert cloudformation_utils.camelize('-foo--bar-') == 'FooBar'
    assert cloudformation_utils.camelize('-foo7bar-baz-') == 'Foo7BarBaz'

    # This might not be best but it's what it does.  We're really only expecting to pass things with hyphens.
    assert cloudformation_utils.camelize('foo_bar') == 'Foo_Bar'


def test_dehyphenate():

    assert cloudformation_utils.dehyphenate('foo') == 'foo'
    assert cloudformation_utils.dehyphenate('foo-bar') == 'foobar'
    assert cloudformation_utils.dehyphenate('foo_bar') == 'foo_bar'
    assert cloudformation_utils.dehyphenate('-foo-bar--baz----') == 'foobarbaz'
    assert cloudformation_utils.dehyphenate('-foo123-bar7baz--quux----') == 'foo123bar7bazquux'


def test_hyphenify():

    assert cloudformation_utils.hyphenify('foo') == 'foo'
    assert cloudformation_utils.hyphenify('foo-bar') == 'foo-bar'
    assert cloudformation_utils.hyphenify('foo_bar') == 'foo-bar'
    assert cloudformation_utils.hyphenify('_foo_bar__baz____') == '-foo-bar--baz----'
    assert cloudformation_utils.hyphenify('_foo123-bar7baz__quux----') == '-foo123-bar7baz--quux----'


def test_make_key_for_ecs_application_url():

    expected = 'ECSApplicationURLcgapanytest'
    assert cloudformation_utils.make_required_key_for_ecs_application_url('cgap-anytest') == expected

    expected = 'ECSApplicationURLcgapfootest'
    assert cloudformation_utils.make_required_key_for_ecs_application_url('cgap-footest') == expected


def test_get_ecs_real_url():

    mocked_boto3 = MockBoto3()
    mastertest_url = 'http://C4EcsTrialAlphacgapmastertest-1234512345.us-east-1.elb.amazonaws.com'
    mastertest_outputs = [
        {
            'OutputKey': 'ECSApplicationURLcgapmastertest',
            'OutputValue': mastertest_url,
            'Description': 'URL of CGAP-Portal.',
        },
    ]
    supertest_url = 'http://cgap-supertest-11223344.us-east-1.elb.amazonaws.com'
    supertest_outputs = [
        {
            'OutputKey': 'ECSApplicationURLcgapsupertest',
            'OutputValue': supertest_url,
            'Description': 'URL of CGAP-Portal.',
        }
    ]
    mocked_stacks = [
        MockBotoCloudFormationStack('c4-iam-main-stack'),
        MockBotoCloudFormationStack('c4-network-main-stack'),
        MockBotoCloudFormationStack('c4-ecs-cgap-supertest-stack', mock_outputs=supertest_outputs),
        MockBotoCloudFormationStack('c4-ecs-cgap-mastertest-stack', mock_outputs=mastertest_outputs),
    ]
    MockBotoCloudFormationClient.setup_boto3_mocked_stacks(boto3=mocked_boto3, mocked_stacks=mocked_stacks)
    with mock.patch.object(cloudformation_utils, "boto3", mocked_boto3):

        assert cloudformation_utils.get_ecs_real_url('cgap-supertest') == supertest_url
        assert cloudformation_utils.get_ecs_real_url('cgap-mastertest') == mastertest_url


def test_get_ecr_repo_url_alpha():

    mocked_boto3 = MockBoto3()
    mastertest_repo = 'xxx6742.dkr.ecr.us-east-1.amazonaws.com/cgap-mastertest'
    mastertest_outputs = [
        {
            'OutputKey': 'C4EcrTrialAlphaECRRepoURL',
            'OutputValue': mastertest_repo,
            'Description': 'CGAPDocker Image Repository URL',
            'ExportName': 'c4-ecr-trial-alpha-stack-ECRRepoURL'
        },
    ]
    mocked_stacks = [
        MockBotoCloudFormationStack('c4-iam-trial-alpha-stack'),
        MockBotoCloudFormationStack('c4-network-trial-alpha-stack'),
        MockBotoCloudFormationStack('c4-ecr-trial-alpha-stack', mock_outputs=mastertest_outputs),
    ]
    MockBotoCloudFormationClient.setup_boto3_mocked_stacks(boto3=mocked_boto3, mocked_stacks=mocked_stacks)
    with mock.patch.object(cloudformation_utils, "boto3", mocked_boto3):

        assert cloudformation_utils.get_ecr_repo_url('cgap-mastertest') == mastertest_repo


def test_get_ecr_repo_url_kmp():  # Test for my test environment. -kmp 17-Aug-2021

    mocked_boto3 = MockBoto3()
    supertest_repo = 'xxx0312.dkr.ecr.us-east-1.amazonaws.com/main'
    supertest_outputs = [
        {
            'OutputKey': 'C4ECRMainRepoURL',
            'OutputValue': supertest_repo,
            'Description': 'CGAPDocker Image Repository URL',
            'ExportName': 'c4-ecr-main-stack-RepoURL'
        }
    ]
    mocked_stacks = [
        MockBotoCloudFormationStack('c4-iam-trial-alpha-stack'),
        MockBotoCloudFormationStack('c4-network-trial-alpha-stack'),
        MockBotoCloudFormationStack('c4-ecr-trial-alpha-stack', mock_outputs=supertest_outputs),
    ]
    MockBotoCloudFormationClient.setup_boto3_mocked_stacks(boto3=mocked_boto3, mocked_stacks=mocked_stacks)
    with mock.patch.object(cloudformation_utils, "boto3", mocked_boto3):

        assert cloudformation_utils.get_ecr_repo_url('cgap-supertest') == supertest_repo


@pytest.mark.parametrize('use_ecosystem_repo', [True, False])
def test_get_ecr_repo_url_hybrid(use_ecosystem_repo):

    main_repo = 'xxx6742.dkr.ecr.us-east-1.amazonaws.com/main'
    main_stack = 'c4-ecr-main-stack'
    main_outputs = [
        {
            'OutputKey': 'C4ECRMainRepoURL',
            'OutputValue': main_repo,
            'Description': 'CGAPDocker Image Repository URL',
            'ExportName': f'{main_stack}-RepoURL'
        },
    ]
    mocked_boto3 = MockBoto3()
    mastertest_repo = 'xxx6742.dkr.ecr.us-east-1.amazonaws.com/cgap-mastertest'
    mastertest_stack = 'c4-ecr-cgap-mastertest-stack'
    mastertest_outputs = [
        {
            'OutputKey': 'C4EcrCgapMastertestRepoURL',
            'OutputValue': mastertest_repo,
            'Description': 'CGAPDocker Image Repository URL',
            'ExportName': f'{mastertest_stack}-RepoURL'
        },
    ]
    hotseat_repo = 'xxx6742.dkr.ecr.us-east-1.amazonaws.com/cgap-hotseat'
    hotseat_stack = 'c4-ecr-cgap-hotseat-stack'
    hotseat_outputs = [
        {
            'OutputKey': 'C4EcrCgapHotseatRepoURL',
            'OutputValue': hotseat_repo,
            'Description': 'CGAPDocker Image Repository URL',
            'ExportName': f'{hotseat_stack}-RepoURL'
        },
    ]
    mocked_stacks = [
        MockBotoCloudFormationStack('c4-iam-trial-alpha-stack'),
        MockBotoCloudFormationStack('c4-network-trial-alpha-stack'),
        MockBotoCloudFormationStack(mastertest_stack, mock_outputs=mastertest_outputs),
        MockBotoCloudFormationStack(hotseat_stack, mock_outputs=hotseat_outputs),
    ]
    if use_ecosystem_repo:
        mocked_stacks.append(MockBotoCloudFormationStack(main_stack, mock_outputs=main_outputs))
    MockBotoCloudFormationClient.setup_boto3_mocked_stacks(boto3=mocked_boto3, mocked_stacks=mocked_stacks)
    with mock.patch.object(cloudformation_utils, "boto3", mocked_boto3):

        assert cloudformation_utils.get_ecr_repo_url('cgap-mastertest') == mastertest_repo
        assert cloudformation_utils.get_ecr_repo_url(env_name='cgap-hotseat') == hotseat_repo
        assert cloudformation_utils.get_ecr_repo_url(env_name='cgap-wolf') == (main_repo if use_ecosystem_repo else '')


def test_c4_orchestration_manager_get_stack_output():

    mocked_boto3 = MockBoto3()

    with mock.patch.object(cloudformation_utils, "boto3", mocked_boto3):

        manager = cloudformation_utils.C4OrchestrationManager()
        mock_outputs = [
            {'OutputKey': 'alpha', 'OutputValue': 'one'},
            {'OutputKey': 'beta', 'OutputValue': 'two'}
        ]
        s = MockBotoCloudFormationStack('foo', mock_outputs=mock_outputs)
        assert manager.get_stack_output(s, 'alpha') == 'one'
        assert manager.get_stack_output(s, 'beta') == 'two'
        assert manager.get_stack_output(s, 'gamma') is None


def test_c4_orchestration_manager_all_stacks():

    mocked_boto3 = MockBoto3()
    mock_stack_names = ['foo-bar-baz', 'awseb-e-123-stack', 'c4-foo-123', 'c4-foo-456']
    MockBotoCloudFormationClient.setup_boto3_mocked_stacks(boto3=mocked_boto3, mock_stack_names=mock_stack_names)
    with mock.patch.object(cloudformation_utils, "boto3", mocked_boto3):
        manager = cloudformation_utils.C4OrchestrationManager()
        all_stacks = manager.all_stacks()
        assert sorted(s.name for s in all_stacks) == ['c4-foo-123', 'c4-foo-456']


def test_c4_orchestration_manager_extract_stack_name_token():

    mocked_boto3 = MockBoto3()

    with mock.patch.object(cloudformation_utils, "boto3", mocked_boto3):

        def extract(stack_name):
            stack = MockBotoCloudFormationStack(stack_name)
            manager = cloudformation_utils.C4OrchestrationManager()
            return manager._extract_stack_name_token(stack)  # noQA - I know this is protected. Just testing it.

        assert extract('foo-bar') is None
        assert extract('c4-foo') == 'foo'
        assert extract('c4-foo-bar') == 'foo'
        assert extract('c4-foo-bar-baz') == 'foo'
        assert extract('c4-foo_bar-baz') == 'foo_bar'


def test_awseb_orchestration_manager_extract_stack_name_token():

    mocked_boto3 = MockBoto3()

    with mock.patch.object(cloudformation_utils, "boto3", mocked_boto3):

        def extract(stack_name):
            stack = MockBotoCloudFormationStack(stack_name)
            manager = cloudformation_utils.AwsebOrchestrationManager()
            return manager._extract_stack_name_token(stack)  # noQA - please don't tell me this is protected. Just testing

        assert extract('foo-bar') is None
        assert extract('c4-foo') is None
        assert extract('awseb-foo-bar') is None
        assert extract('awseb-x-bar') is None
        assert extract('awseb-e-bar') is None
        assert extract('awseb-e-bar-stack') == 'bar'
        assert extract('awseb-e-bar-baz-stack') == 'bar-baz'


def test_c4_orchestration_manager_find_stack():

    mocked_boto3 = MockBoto3()
    mock_stack_names = ['foo-bar-baz', 'awseb-e-foo-stack', 'c4-network-cgap-test-123', 'c4-iam-cgap-test-456']
    MockBotoCloudFormationClient.setup_boto3_mocked_stacks(boto3=mocked_boto3, mock_stack_names=mock_stack_names)
    with mock.patch.object(cloudformation_utils, "boto3", mocked_boto3):
        manager = cloudformation_utils.C4OrchestrationManager()
        network_stack = manager.find_stack('network')
        assert network_stack.name == 'c4-network-cgap-test-123'

    mocked_boto3 = MockBoto3()
    mock_stack_names = ['foo-bar-baz', 'awseb-e-foo-stack', 'c4-network-cgap-test-123', 'c4-network-cgap-prod-456']
    MockBotoCloudFormationClient.setup_boto3_mocked_stacks(boto3=mocked_boto3, mock_stack_names=mock_stack_names)
    with mock.patch.object(cloudformation_utils, "boto3", mocked_boto3):
        manager = cloudformation_utils.C4OrchestrationManager()
        with pytest.raises(ValueError, match="too many"):
            manager.find_stack('network')


def test_c4_orchestration_manager_find_stack_outputs():

    mocked_boto3 = MockBoto3()
    network_outputs = [
        {'OutputKey': 'rds', 'OutputValue': 'some-db-thing'},
        {'OutputKey': 'es', 'OutputValue': 'some-es-thing'},
    ]
    iam_outputs = [
        {'OutputKey': 'user1', 'OutputValue': 'Joe'},
        {'OutputKey': 'user2', 'OutputValue': 'Sally'},
    ]
    mocked_stacks = [
        MockBotoCloudFormationStack('c4-network-main-stack', mock_outputs=network_outputs),
        MockBotoCloudFormationStack('c4-iam-main-stack', mock_outputs=iam_outputs),
    ]
    MockBotoCloudFormationClient.setup_boto3_mocked_stacks(boto3=mocked_boto3, mocked_stacks=mocked_stacks)
    with mock.patch.object(cloudformation_utils, "boto3", mocked_boto3):

        manager = cloudformation_utils.C4OrchestrationManager()

        # Looking up a simple key by name
        assert manager.find_stack_outputs('user1') == {'user1': 'Joe'}
        assert manager.find_stack_outputs('user1', value_only=True) == ['Joe']

        assert manager.find_stack_outputs('rds') == {'rds': 'some-db-thing'}
        assert manager.find_stack_outputs('rds', value_only=True) == ['some-db-thing']

        assert manager.find_stack_outputs('not-there') == {}
        assert manager.find_stack_outputs('not-there', value_only=True) == []

        # Use of predicate to find several related keys
        assert manager.find_stack_outputs(lambda x: x.startswith("user")) == {'user1': 'Joe', 'user2': 'Sally'}
        assert sorted(manager.find_stack_outputs(lambda x: x.startswith("user"), value_only=True)) == ['Joe', 'Sally']


def test_c4_orchestration_manager_find_stack_output():

    mocked_boto3 = MockBoto3()
    network_outputs = [
        {'OutputKey': 'rds', 'OutputValue': 'some-db-thing'},
        {'OutputKey': 'es', 'OutputValue': 'some-es-thing'},
    ]
    iam_outputs = [
        {'OutputKey': 'user1', 'OutputValue': 'Joe'},
        {'OutputKey': 'user2', 'OutputValue': 'Sally'},
    ]
    mocked_stacks = [
        MockBotoCloudFormationStack('c4-network-main-stack', mock_outputs=network_outputs),
        MockBotoCloudFormationStack('c4-iam-main-stack', mock_outputs=iam_outputs),
    ]
    MockBotoCloudFormationClient.setup_boto3_mocked_stacks(boto3=mocked_boto3, mocked_stacks=mocked_stacks)
    with mock.patch.object(cloudformation_utils, "boto3", mocked_boto3):

        manager = cloudformation_utils.C4OrchestrationManager()

        # Looking up a simple key by name
        assert manager.find_stack_output('user1') == {'user1': 'Joe'}
        assert manager.find_stack_output('user1', value_only=True) == 'Joe'

        assert manager.find_stack_output('rds') == {'rds': 'some-db-thing'}
        assert manager.find_stack_output('rds', value_only=True) == 'some-db-thing'

        assert manager.find_stack_output('not-there') is None
        assert manager.find_stack_output('not-there', value_only=True) is None

        # Use of predicate to find several related keys
        with pytest.raises(Exception):
            manager.find_stack_output(lambda x: x.startswith("user"))
        with pytest.raises(Exception):
            manager.find_stack_output(lambda x: x.startswith("user"), value_only=True)


def test_c4_orchestration_manager_find_stack_resource():

    mocked_boto3 = MockBoto3()

    private_subnet_a = MockBotoCloudFormationResourceSummary(logical_id='MyPrivateSubnetA',
                                                             physical_resource_id='subnet-111111')

    private_subnet_b = MockBotoCloudFormationResourceSummary(logical_id='MyPrivateSubnetB',
                                                             physical_resource_id='subnet-222222')

    public_subnet_a = MockBotoCloudFormationResourceSummary(logical_id='MyPublicSubnetA',
                                                            physical_resource_id='subnet-333333')

    public_subnet_b = MockBotoCloudFormationResourceSummary(logical_id='MyPublicSubnetA',
                                                            physical_resource_id='subnet-444444')

    network_resource_summaries = [private_subnet_a, private_subnet_b, public_subnet_a, public_subnet_b]

    mocked_stacks = [
        MockBotoCloudFormationStack('c4-network-main-stack', mock_resource_summaries=network_resource_summaries),
        MockBotoCloudFormationStack('c4-iam-main-stack')
    ]

    MockBotoCloudFormationClient.setup_boto3_mocked_stacks(boto3=mocked_boto3, mocked_stacks=mocked_stacks)

    with mock.patch.object(cloudformation_utils, "boto3", mocked_boto3):

        manager = cloudformation_utils.C4OrchestrationManager()

        assert manager.find_stack_resource('network', 'MyPrivateSubnetA') == private_subnet_a
        assert manager.find_stack_resource('network', 'MyPrivateSubnetB') == private_subnet_b
        assert manager.find_stack_resource('network', 'MyPrivateSubnetC') is None

        assert manager.find_stack_resource('network', 'MyPrivateSubnetA', 'logical_id') == 'MyPrivateSubnetA'
        assert manager.find_stack_resource('network', 'MyPrivateSubnetC', 'logical_id') is None

        assert manager.find_stack_resource('network', 'MyPrivateSubnetA', 'physical_resource_id') == 'subnet-111111'

        assert manager.find_stack_resource('network', 'MyPrivateSubnetC', 'physical_resource_id') is None
        assert manager.find_stack_resource('network', 'MyPrivateSubnetC', 'physical_resource_id', 'foo') == 'foo'


MOCKED_DEV_CHECK_RUNNER = "c4-foursight-development-stack-CheckRunner-XvXOKPYFFIye"
MOCKED_PRD_CHECK_RUNNER = "c4-foursight-fourfront-production-stac-CheckRunner-MW4VHuCIsDXc"

MOCKED_LAMBDA_NAMES = [
    # The intent is to provide a bunch of different items, including enough
    # to force things into pagination because the AWS endpoint will give a max
    # of 50 items even if you ask for a larger MaxItems. -kmp 24-Jun-2022
    "c4-foursight-development-stack-HourlyChecks3-B0dVPPczSind"
    "c4-foursight-development-stack-MorningChecks3-U8C7kjQcai7a",
    "c4-foursight-development-stack-ThirtyMinChecks-MFWFKtAiWKil",
    "c4-foursight-fourfront-pro-FridayAutoscalingChecks-gqqwcoQYmsC8",
    "c4-foursight-fourfront-production--ThirtyMinChecks-VQDQXvsDHTJj",
    "c4-foursight-fourfront-production-s-MorningChecks1-X5oI034JbLze",
    "c4-foursight-fourfront-production-s-MorningChecks4-0ndpdpBpMyFU",
    # This next is an actual lambda function but not the one we want.
    # We use the tokens 'development' and 'CheckRunner'.
    "foursight-cgap-dev-check_runner",
    MOCKED_PRD_CHECK_RUNNER,
    "c4-foursight-fourfront-production-stack-APIHandler-CoLjJ3aRtcFh",
    "c4-foursight-fourfront-production-stack-APIHandler-CoLjJ3aRtcFh",
] + [f"decoy-{i}" for i in range(50)] + [
    "c4-foursight-development-s-MondayAutoscalingChecks-PNk0IOw1hKIO",
    MOCKED_DEV_CHECK_RUNNER,
    "c4-foursight-development-stack-TenMinChecks-0mWRApze1Vy5"
]


def test_abstract_orchestration_manager_discover_foursight_check_runner_name():

    with EnvBase.global_env_bucket_named('foursight-envs'):
        with mocked_s3utils(environments=['fourfront-mastertest']) as b3:
            with mock.patch.object(cloudformation_utils, "boto3", b3):

                b3.client('lambda').register_lambdas_for_testing(
                    {
                        name: {}
                        for name in MOCKED_LAMBDA_NAMES
                    }
                )

                # We only need the class for this test, not the instance
                abstract_manager = cloudformation_utils.AbstractOrchestrationManager

                with override_environ(CHECK_RUNNER=None):
                    print("-" * 20)
                    print(f"CHECK_RUNNER={os.environ.get('CHECK_RUNNER')}")

                    dev_runner = abstract_manager.discover_foursight_check_runner_name('dev', encache=False)
                    print(f"dev_runner={dev_runner}")
                    assert dev_runner == MOCKED_DEV_CHECK_RUNNER
                    assert os.environ.get('CHECK_RUNNER') is None
                    print(f"CHECK_RUNNER={os.environ.get('CHECK_RUNNER')}")

                    dev_runner = abstract_manager.discover_foursight_check_runner_name('dev', encache=True)
                    print(f"dev_runner={dev_runner}")
                    assert dev_runner == MOCKED_DEV_CHECK_RUNNER
                    assert os.environ.get('CHECK_RUNNER') == dev_runner
                    print(f"CHECK_RUNNER={os.environ.get('CHECK_RUNNER')}")

                with override_environ(CHECK_RUNNER=None):
                    print("-" * 20)
                    print(f"CHECK_RUNNER={os.environ.get('CHECK_RUNNER')}")

                    prd_runner = abstract_manager.discover_foursight_check_runner_name('prod', encache=False)
                    print(f"prd_runner={prd_runner}")
                    assert prd_runner == MOCKED_PRD_CHECK_RUNNER
                    assert os.environ.get('CHECK_RUNNER') is None
                    print(f"CHECK_RUNNER={os.environ.get('CHECK_RUNNER')}")

                    prd_runner = abstract_manager.discover_foursight_check_runner_name('prod', encache=True)
                    print(f"prd_runner={prd_runner}")
                    assert prd_runner == MOCKED_PRD_CHECK_RUNNER
                    assert os.environ.get('CHECK_RUNNER') == prd_runner
                    print(f"CHECK_RUNNER={os.environ.get('CHECK_RUNNER')}")

                with override_environ(CHECK_RUNNER=None):
                    print("-" * 20)
                    print(f"CHECK_RUNNER={os.environ.get('CHECK_RUNNER')}")

                    tst_runner = abstract_manager.discover_foursight_check_runner_name('test', encache=False)
                    print(f"tst_runner={tst_runner}")
                    assert tst_runner == MOCKED_DEV_CHECK_RUNNER
                    assert os.environ.get('CHECK_RUNNER') is None
                    print(f"CHECK_RUNNER={os.environ.get('CHECK_RUNNER')}")

                    tst_runner = abstract_manager.discover_foursight_check_runner_name('test', encache=True)
                    print(f"tst_runner={tst_runner}")
                    assert tst_runner == MOCKED_DEV_CHECK_RUNNER
                    assert os.environ.get('CHECK_RUNNER') == tst_runner
                    print(f"CHECK_RUNNER={os.environ.get('CHECK_RUNNER')}")

                with override_environ(CHECK_RUNNER=None):
                    print("-" * 20)
                    print(f"CHECK_RUNNER={os.environ.get('CHECK_RUNNER')}")

                    foo_runner = abstract_manager.discover_foursight_check_runner_name('foo', encache=False)
                    print(f"foo_runner={foo_runner}")
                    assert foo_runner == MOCKED_DEV_CHECK_RUNNER
                    assert os.environ.get('CHECK_RUNNER') is None
                    print(f"CHECK_RUNNER={os.environ.get('CHECK_RUNNER')}")

                    foo_runner = abstract_manager.discover_foursight_check_runner_name('foo', encache=True)
                    print(f"foo_runner={foo_runner}")
                    assert foo_runner == MOCKED_DEV_CHECK_RUNNER
                    assert os.environ.get('CHECK_RUNNER') == foo_runner
                    print(f"CHECK_RUNNER={os.environ.get('CHECK_RUNNER')}")
