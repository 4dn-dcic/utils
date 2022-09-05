import json
import pytest

from unittest import mock
from dcicutils import ecr_utils as ecr_utils_module
from dcicutils.ecr_utils import ECRUtils, ECRTagWatcher
from dcicutils.docker_utils import DockerUtils
from dcicutils.misc_utils import ignored, ignorable, filtered_warnings
from dcicutils.qa_utils import MockBoto3, MockBotoECR
from .helpers import using_fresh_cgap_state_for_testing


REPO_URL = '123456789.dkr.ecr.us-east-2.amazonaws.com/cgap-mastertest'  # dummy URL


def mocked_describe_respositories():
    """ Mocks the important info from the describe_repositories API call. """
    return {'repositories': [{
        'repositoryUri': REPO_URL,
       }]}


def mocked_ecr_login(*, username, password, registry):
    ignored(username, password, registry)
    return


@using_fresh_cgap_state_for_testing()
def test_ecr_utils_basic():
    """ Tests something simple for now, more tests to be added later. """
    # init args no longer default. -kmp 14-Jul-2022
    cli = ECRUtils(env_name='cgap-mastertest', local_repository='cgap-wsgi')
    with mock.patch.object(cli.ecr_client, 'describe_repositories', mocked_describe_respositories):
        url = cli.resolve_repository_uri()
        assert url == REPO_URL


@pytest.mark.skipif(not DockerUtils.docker_is_running(), reason="Docker is not running.")
@using_fresh_cgap_state_for_testing()
def test_ecr_utils_workflow():
    """ Tests URL + Login via Docker_cli"""
    # init args no longer default. -kmp 14-Jul-2022
    ecr_cli = ECRUtils(env_name='cgap-mastertest', local_repository='cgap-wsgi')
    docker_cli = DockerUtils()
    with mock.patch.object(ecr_cli.ecr_client, 'describe_repositories', mocked_describe_respositories):
        ecr_cli.resolve_repository_uri()
        ecr_user, ecr_pass = 'dummy', 'dummy'  # XXX: integrated test for this mechanism
        with mock.patch.object(docker_cli.client, 'login', mocked_ecr_login):
            ignorable(filtered_warnings)
            with filtered_warnings("ignore", category=DeprecationWarning):
                # e.g., as of docker 4.4.4, we see in site-packages/docker/utils/utils.py, lines 59-60:
                #  DeprecationWarning: distutils Version classes are deprecated. Use packaging.version instead.
                #    s1 = StrictVersion(v1)
                #  DeprecationWarning: distutils Version classes are deprecated. Use packaging.version instead.
                #    s2 = StrictVersion(v2)
                docker_cli.login(ecr_repo_uri=ecr_cli.url,
                                 ecr_user=ecr_user, ecr_pass=ecr_pass)
            # XXX: integrated test the remaining?


def test_ecr_utils_get_uri():
    print()  # start output on fresh line
    mock_boto3 = MockBoto3()
    with mock.patch.object(ecr_utils_module, "boto3", mock_boto3):
        ecr_client: MockBotoECR = mock_boto3.client('ecr')
        for repo in ['main', 'decoy-1', 'fourfront-foo', 'decoy-2']:
            print(f"Adding image repository"
                  f" {json.dumps(ecr_client.add_image_repository_for_testing(repo), indent=2, default=str)}")
        print("---- Setup complete. Beginning test. ----")
        ecr_utils = ECRUtils(env_name='fourfront-foo')
        assert ecr_utils.url is None
        with pytest.raises(Exception):
            ecr_utils.get_uri()  # This intentionally raises an error if there is no URL
        ecr_utils.resolve_repository_uri()
        print(f"ecr_utils.url = {ecr_utils.url}")
        assert ecr_utils.url is not None
        print(f"ecr_utils.get_uri() = {ecr_utils.get_uri()}")
        assert ecr_utils.get_uri() is not None


def test_ecr_utils_resolve_repository_uri():
    print()  # start output on fresh line
    mock_boto3 = MockBoto3()
    with mock.patch.object(ecr_utils_module, "boto3", mock_boto3):
        ecr_client: MockBotoECR = mock_boto3.client('ecr')
        for repo in ['main', 'decoy-1', 'fourfront-foo', 'decoy-2']:
            print(f"Adding image repository"
                  f" {json.dumps(ecr_client.add_image_repository_for_testing(repo), indent=2, default=str)}")
        print("---- Setup complete. Beginning test. ----")
        ecr_utils = ECRUtils(env_name='fourfront-foo')
        print(f"ecr_utils.url = {ecr_utils.url}")
        assert ecr_utils.url is None
        print("resolving repository URI")
        ecr_utils.resolve_repository_uri()
        print(f"ecr_utils.url = {ecr_utils.url}")
        assert ecr_utils.url is not None


def test_ecr_tag_watcher():
    print()  # start output on fresh line
    mock_boto3 = MockBoto3()
    with mock.patch.object(ecr_utils_module, "boto3", mock_boto3):
        ecr_client: MockBotoECR = mock_boto3.client('ecr')
        ecr_client.add_image_repository_for_testing('main')
        for i in range(5):
            last_from_loop = ecr_client.add_image_metadata_for_testing('main', tags=['latest'])
        watcher = ECRTagWatcher()
        first_found = watcher.get_current_image_digest()
        print(f"first_found = {first_found}")
        assert first_found == last_from_loop['imageDigest']
        to_deploy = watcher.check_for_new_image_to_deploy()
        assert not to_deploy
        print(f"to_deploy =   {to_deploy}")
        assert first_found == watcher.get_current_image_digest()
        print(f"Simulating deploy.")
        even_newer = ecr_client.add_image_metadata_for_testing('main', tags=['latest'])
        next_found = watcher.get_current_image_digest()
        print(f"next_found =  {next_found}")
        assert next_found != first_found
        assert next_found == even_newer['imageDigest']
        assert next_found == watcher.get_current_image_digest()  # It stays this until next deploy
        to_deploy = watcher.check_for_new_image_to_deploy()
        print(f"to_deploy =   {to_deploy}")
        assert to_deploy
        assert to_deploy == next_found
        to_deploy = watcher.check_for_new_image_to_deploy()
        print(f"to_deploy =   {to_deploy}")
        assert not to_deploy
