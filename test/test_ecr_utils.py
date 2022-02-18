import pytest

from unittest import mock
from dcicutils.ecr_utils import ECRUtils
from dcicutils.docker_utils import DockerUtils
from dcicutils.misc_utils import ignored


REPO_URL = '123456789.dkr.ecr.us-east-2.amazonaws.com/cgap-mastertest'  # dummy URL


def mocked_describe_respositories():
    """ Mocks the important info from the describe_repositories API call. """
    return {'repositories': [{
        'repositoryUri': REPO_URL,
       }]}


def mocked_ecr_login(*, username, password, registry):
    ignored(username, password, registry)
    return


def test_ecr_utils_basic():
    """ Tests something simple for now, more tests to be added later. """
    cli = ECRUtils()  # default args ok
    with mock.patch.object(cli.client, 'describe_repositories', mocked_describe_respositories):
        url = cli.resolve_repository_uri()
        assert url == REPO_URL


@pytest.mark.skipif(not DockerUtils.docker_is_running(), reason="Docker is not running.")
def test_ecr_utils_workflow():
    """ Tests URL + Login via Docker_cli"""
    ecr_cli = ECRUtils()
    docker_cli = DockerUtils()
    with mock.patch.object(ecr_cli.client, 'describe_repositories', mocked_describe_respositories):
        ecr_cli.resolve_repository_uri()
        ecr_user, ecr_pass = 'dummy', 'dummy'  # XXX: integrated test for this mechanism
        with mock.patch.object(docker_cli.client, 'login', mocked_ecr_login):
            docker_cli.login(ecr_repo_uri=ecr_cli.url,
                             ecr_user=ecr_user, ecr_pass=ecr_pass)
            # XXX: integrated test the remaining?


def test_ecr_utils_integrated():
    """ Write me! """
    pass
