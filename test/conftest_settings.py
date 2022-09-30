import os

from dcicutils.ff_mocks import IntegratedFixture


TEST_DIR = os.path.join(os.path.dirname(__file__))

_integrated_fixture = IntegratedFixture('integrated_ff')

INTEGRATED_ENV = _integrated_fixture.ENV_NAME
INTEGRATED_ENV_INDEX_NAMESPACE = _integrated_fixture.ENV_INDEX_NAMESPACE
INTEGRATED_ENV_PORTAL_URL = _integrated_fixture.ENV_PORTAL_URL
INTEGRATED_ES = _integrated_fixture.ES_URL

REPOSITORY_ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
