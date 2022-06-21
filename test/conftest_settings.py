import os

from dcicutils.ff_mocks import IntegratedFixture


TEST_DIR = os.path.join(os.path.dirname(__file__))

INTEGRATED_ENV = IntegratedFixture.ENV_NAME
INTEGRATED_ENV_INDEX_NAMESPACE = IntegratedFixture.ENV_INDEX_NAMESPACE
INTEGRATED_ENV_PORTAL_URL = IntegratedFixture.ENV_PORTAL_URL
INTEGRATED_ES = IntegratedFixture.ES_URL