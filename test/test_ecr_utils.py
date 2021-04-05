from unittest import mock
from dcicutils.ecr_utils import ECRUtils


REPO_URL = '123456789.dkr.ecr.us-east-2.amazonaws.com/cgap-mastertest'


def mocked_describe_respositories():
    """ Mocks the important info from the describe_repositories API call. """
    return {'repositories': [{
        'repositoryUri': REPO_URL,
       }]}


def test_ecr_utils_basic():
    """ Tests something simple for now, more tests to be added later. """
    cli = ECRUtils()  # default args ok
    with mock.patch.object(cli.client, 'describe_repositories', mocked_describe_respositories):
        url = cli.resolve_repository_uri()
        assert url == REPO_URL
