from dcicutils import ff_mocks as ff_mocks_module
from dcicutils.ff_mocks import AbstractIntegratedFixture
from unittest import mock


def test_abstract_integrated_fixture_no_server_fixtures():

    with mock.patch.object(ff_mocks_module, "NO_SERVER_FIXTURES"):  # too late to set env variable, but this'll do.
        assert AbstractIntegratedFixture._initialize_class() == 'NO_SERVER_FIXTURES'  # noQA - yes, it's protected
        assert AbstractIntegratedFixture.verify_portal_access('not-a-dictionary') == 'NO_SERVER_FIXTURES'
