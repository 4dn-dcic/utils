import pytest

from unittest import mock
from dcicutils.ecr_utils import ECRUtils
from dcicutils.docker_utils import DockerUtils
from dcicutils.misc_utils import ignored, ignorable, filtered_warnings
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


def test_ecr_utils_integrated():
    """ Write me! """
    pass
