import pytest

from dcicutils import base
from dcicutils.base import compute_prd_env_for_env, compute_stg_env_for_env
from dcicutils.common import APP_CGAP, APP_FOURFRONT
from dcicutils.env_utils import EnvUtils, EnvNames


FAUXFRONT_ENV = {
    EnvNames.DEV_ENV_DOMAIN_SUFFIX: ".dev.fauxfront.org",
    EnvNames.PUBLIC_URL_TABLE: [{'environment': 'fauxfront-prd', 'url': 'https://fauxfront.org'}],
    EnvNames.FULL_ENV_PREFIX: 'fauxfront-',
    EnvNames.ORCHESTRATED_APP: APP_FOURFRONT,
    EnvNames.PRD_ENV_NAME: 'fauxfront-prd',
    EnvNames.STAGE_MIRRORING_ENABLED: "true",
    EnvNames.STG_ENV_NAME: 'fauxfront-stg',
}

SEAGAP_ENV = {
    EnvNames.DEV_ENV_DOMAIN_SUFFIX: ".hahvahd.edu",
    EnvNames.PUBLIC_URL_TABLE: [{'environment': 'seagap-prd', 'url': 'https://seagap.hahvahd.edu'}],
    EnvNames.FULL_ENV_PREFIX: 'seagap-',
    EnvNames.ORCHESTRATED_APP: APP_CGAP,
    EnvNames.PRD_ENV_NAME: 'seagap-prd',
}


def test_get_beanstalk_real_url_ff_prd_data_containerized():
    with EnvUtils.locally_declared_data(FAUXFRONT_ENV):
        env = 'fauxfront-prd'
        assert base.get_beanstalk_real_url(env) == 'https://fauxfront.org'


def test_get_beanstalk_real_url_ff_dev_data_containerized():
    with EnvUtils.locally_declared_data(FAUXFRONT_ENV):
        env = 'fauxfront-webdev'
        assert base.get_beanstalk_real_url(env) == 'http://webdev.dev.fauxfront.org'


def test_get_beanstalk_real_url_cgap_prd_data_containerized():
    with EnvUtils.locally_declared_data(SEAGAP_ENV):
        env = 'seagap-prd'
        assert base.get_beanstalk_real_url(env) == 'https://seagap.hahvahd.edu'


def test_get_beanstalk_real_url_cgap_dev_data_containerized():
    with EnvUtils.locally_declared_data(SEAGAP_ENV):
        env = 'seagap-webdev'
        assert base.get_beanstalk_real_url(env) == 'https://seagap-webdev.hahvahd.edu'


def test_compute_ff_prd_env():

    with EnvUtils.locally_declared_data(FAUXFRONT_ENV):
        computed = base.compute_ff_prd_env()
        assert computed == 'fauxfront-prd'


def test_compute_ff_stg_env():

    with EnvUtils.locally_declared_data(FAUXFRONT_ENV):
        computed = base.compute_ff_stg_env()
        assert computed == 'fauxfront-stg'


def test_compute_cgap_prd_env():

    with EnvUtils.locally_declared_data(SEAGAP_ENV):
        computed = base.compute_cgap_prd_env()
        assert computed == 'seagap-prd'


def test_compute_cgap_stg_env():

    with EnvUtils.locally_declared_data(SEAGAP_ENV):
        computed = base.compute_cgap_stg_env()
        assert computed is None


def test_compute_prd_env_for_env_ff_containerized():

    with EnvUtils.locally_declared_data(FAUXFRONT_ENV):
        assert compute_prd_env_for_env('fauxfront-mastertest') == 'fauxfront-prd'

    with EnvUtils.locally_declared_data(FAUXFRONT_ENV):
        assert compute_prd_env_for_env('fauxfront-prd') == 'fauxfront-prd'

    with EnvUtils.locally_declared_data(FAUXFRONT_ENV):
        with pytest.raises(ValueError):  # Unknown environment: fourfront-mastertest
            compute_prd_env_for_env('fourfront-mastertest')

    with EnvUtils.locally_declared_data(FAUXFRONT_ENV):
        with pytest.raises(ValueError):  # Unknown environment: cgap-webdev
            compute_prd_env_for_env('cgap-webdev')


def test_compute_stg_env_for_env_ff_containerized():

    with EnvUtils.locally_declared_data(FAUXFRONT_ENV):
        assert compute_stg_env_for_env('fauxfront-mastertest') == 'fauxfront-stg'

    with EnvUtils.locally_declared_data(FAUXFRONT_ENV):
        assert compute_stg_env_for_env('fauxfront-prd') == 'fauxfront-stg'

    with EnvUtils.locally_declared_data(FAUXFRONT_ENV):
        with pytest.raises(ValueError):  # Unknown environment: fourfront-mastertest
            compute_stg_env_for_env('fourfront-mastertest')

    with EnvUtils.locally_declared_data(FAUXFRONT_ENV):
        with pytest.raises(ValueError):  # Unknown environment: cgap-webdev
            compute_stg_env_for_env('cgap-webdev')


def test_compute_prd_env_for_env_cgap_containerized():

    with EnvUtils.locally_declared_data(SEAGAP_ENV):
        assert compute_prd_env_for_env('seagap-webdev') == 'seagap-prd'

    with EnvUtils.locally_declared_data(SEAGAP_ENV):
        assert compute_prd_env_for_env('seagap-prd') == 'seagap-prd'

    with EnvUtils.locally_declared_data(SEAGAP_ENV):
        with pytest.raises(ValueError):  # Unknown environment: fourfront-mastertest
            compute_prd_env_for_env('fourfront-mastertest')

    with EnvUtils.locally_declared_data(SEAGAP_ENV):
        with pytest.raises(ValueError):  # Unknown environment: cgap-webdev
            compute_prd_env_for_env('cgap-webdev')


def test_compute_stg_env_for_env_cgap_containerized():

    with EnvUtils.locally_declared_data(SEAGAP_ENV):
        assert compute_stg_env_for_env('seagap-webdev') is None

    with EnvUtils.locally_declared_data(SEAGAP_ENV):
        assert compute_stg_env_for_env('seagap-prd') is None

    with EnvUtils.locally_declared_data(SEAGAP_ENV):
        with pytest.raises(ValueError):  # Unknown environment: fourfront-mastertest
            compute_stg_env_for_env('fourfront-mastertest')

    with EnvUtils.locally_declared_data(SEAGAP_ENV):
        with pytest.raises(ValueError):  # Unknown environment: cgap-webdev
            compute_stg_env_for_env('cgap-webdev')
