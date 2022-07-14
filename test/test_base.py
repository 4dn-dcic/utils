import boto3
import json
import pytest
import socket

from collections import defaultdict
from dcicutils import base, env_utils, compute_prd_env_for_env
from dcicutils.env_utils import (
    is_fourfront_env, is_cgap_env, is_stg_or_prd_env,
    FF_ENV_PRODUCTION_GREEN, FF_ENV_PRODUCTION_BLUE,
    CGAP_ENV_PRODUCTION_GREEN_NEW, CGAP_ENV_PRODUCTION_BLUE_NEW, CGAP_ENV_WEBPROD, _CGAP_MGB_PUBLIC_URL_PRD,
)
from dcicutils.exceptions import NotBeanstalkEnvironment
from dcicutils.qa_utils import mock_not_called
from unittest import mock


def _mocked_beanstalk_info(env):
    return {'CNAME': 'blah-%s.blahblah.us-east-1.elasticbeanstalk.com' % env}


def test_get_beanstalk_real_url_blue_data():
    with mock.patch.object(base, '_compute_prd_env_for_project') as mock_compute_prd_env_for_project:
        with mock.patch.object(base, 'beanstalk_info') as mock_beanstalk_info:
            mock_compute_prd_env_for_project.return_value = 'fourfront-blue'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info
            url = base.get_beanstalk_real_url('fourfront-blue')
            assert url == 'https://data.4dnucleome.org'


def test_get_beanstalk_real_url_green_data():
    with mock.patch.object(base, '_compute_prd_env_for_project') as mock_compute_prd_env_for_project:
        with mock.patch.object(base, 'beanstalk_info') as mock_beanstalk_info:
            mock_compute_prd_env_for_project.return_value = 'fourfront-green'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info
            url = base.get_beanstalk_real_url('fourfront-green')
            assert url == 'https://data.4dnucleome.org'


def test_get_beanstalk_real_url_blue_staging():
    with mock.patch.object(base, '_compute_prd_env_for_project') as mock_compute_prd_env_for_project:
        with mock.patch.object(base, 'beanstalk_info') as mock_beanstalk_info:
            mock_compute_prd_env_for_project.return_value = 'fourfront-green'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info
            url = base.get_beanstalk_real_url('fourfront-blue')
            assert url == 'http://staging.4dnucleome.org'


def test_get_beanstalk_real_url_green_staging():
    with mock.patch.object(base, '_compute_prd_env_for_project') as mock_compute_prd_env_for_project:
        with mock.patch.object(base, 'beanstalk_info') as mock_beanstalk_info:
            mock_compute_prd_env_for_project.return_value = 'fourfront-blue'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info
            url = base.get_beanstalk_real_url('fourfront-green')
            assert url == 'http://staging.4dnucleome.org'


# Non-production environments will do this:

def test_get_beanstalk_real_url_other():
    with mock.patch.object(base, '_compute_prd_env_for_project') as mock_compute_prd_env_for_project:
        with mock.patch.object(base, 'beanstalk_info') as mock_beanstalk_info:
            mock_compute_prd_env_for_project.side_effect = mock_not_called('_compute_prd_env_for_project')
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info
            url = base.get_beanstalk_real_url('beanstalk-name')
            assert url == 'http://blah-beanstalk-name.blahblah.us-east-1.elasticbeanstalk.com'


# These will fail in CGAP, which Soo reported (C4-101):

def test_get_beanstalk_real_url_cgap():
    with mock.patch.object(base, '_compute_prd_env_for_project') as mock_compute_prd_env_for_project:
        with mock.patch.object(base, 'beanstalk_info') as mock_beanstalk_info:
            mock_compute_prd_env_for_project.return_value = 'fourfront-cgap'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info
            url = base.get_beanstalk_real_url('fourfront-cgap')
            assert url == 'https://cgap-mgb.hms.harvard.edu' == _CGAP_MGB_PUBLIC_URL_PRD


def _ip_addresses(hostname):
    return sorted(socket.gethostbyname_ex(hostname)[2])


@pytest.mark.skip("Broken, hopefully for benign reasons, by WAF changes?")
def test_magic_cnames_by_production_ip_address():
    # This simple check just makes sure the obvious truths are checked.
    assert _ip_addresses(base._FF_MAGIC_CNAME) == _ip_addresses("data.4dnucleome.org")
    assert _ip_addresses(base._CGAP_MAGIC_CNAME) == _ip_addresses("cgap.hms.harvard.edu")


@pytest.mark.skip("Broken, hopefully for benign reasons, by WAF changes?")
def test_magic_cnames_by_cname_consistency():

    # These tests are highly specific and will have to change if we make something else be magic.
    # But such is magic. The values should not be casually changed, and such overhead is appropriate.
    # It's good to have tests that will catch unwanted tinkering, typos, etc.

    ff_magic_envs = [FF_ENV_PRODUCTION_GREEN, FF_ENV_PRODUCTION_BLUE]
    cgap_magic_envs = [CGAP_ENV_WEBPROD]

    assert ff_magic_envs == ['fourfront-green', 'fourfront-blue']
    assert cgap_magic_envs == ['fourfront-cgap']

    client = boto3.client('elasticbeanstalk', region_name=base.REGION)
    res = base.describe_beanstalk_environments(client, ApplicationName='4dn-web')
    envs = res['Environments']
    roles = defaultdict(lambda: [])
    for env in envs:
        env_name = env.get('EnvironmentName')
        cname = env.get('CNAME')
        ip = socket.gethostbyname(cname)
        note = ""
        if cname == base._FF_MAGIC_CNAME:
            assert env_name in ff_magic_envs
            ff_prod = "data.4dnucleome.org"
            ff_prod_ips = _ip_addresses(ff_prod)
            assert _ip_addresses(cname) == ff_prod_ips
            note = "\n => FF PRODUCTION (%s @ %s)" % (ff_prod, ",".join(ff_prod_ips))
            roles['FF_PRODUCTION'].append(env_name)
            assert env_utils.is_stg_or_prd_env(env_name)
        elif cname == base._CGAP_MAGIC_CNAME:
            assert env_name in cgap_magic_envs
            cgap_prod = "cgap.hms.harvard.edu"
            cgap_prod_ips = _ip_addresses(cgap_prod)
            assert _ip_addresses(cname) == cgap_prod_ips
            note = "\n => CGAP PRODUCTION (%s @ %s)" % (cgap_prod, ",".join(cgap_prod_ips))
            roles['CGAP_PRODUCTION'].append(env_name)
            assert env_utils.is_stg_or_prd_env(env_name)
        elif env_utils.is_stg_or_prd_env(env_name):
            if env_utils.is_cgap_env(env_name):
                note = "\n => CGAP STAGING ???"  # An error about this is reported later
                roles['CGAP_STAGING'].append(env_name)
                assert env_utils.is_stg_or_prd_env(env_name)
            else:
                ff_staging = "staging.4dnucleome.org"
                ff_staging_ips = _ip_addresses(ff_staging)
                assert _ip_addresses(cname) == ff_staging_ips
                note = "\n => FF STAGING (%s @ %s)" % (ff_staging, ",".join(ff_staging_ips))
                roles['FF_STAGING'].append(env_name)
                assert env_utils.is_stg_or_prd_env(env_name)
        print("%s (%s) = %s %s" % (env_name, ip, cname, note))
    print("roles =", json.dumps(roles, indent=2))
    assert len(roles['FF_PRODUCTION']) == 1
    assert len(roles['FF_STAGING']) == 1
    assert len(roles['CGAP_PRODUCTION']) == 1
    assert len(roles['CGAP_STAGING']) == 0  # CGAP does not expect to have a production mirror
    assert env_utils.get_standard_mirror_env(roles['FF_PRODUCTION'][0]) == roles['FF_STAGING'][0]
    assert env_utils.get_standard_mirror_env(roles['FF_STAGING'][0]) == roles['FF_PRODUCTION'][0]
    #
    # Uncommenting the following assertion will fail the test but first will provide useful
    # debugging information about configurations.
    #
    # assert False, "PASSED"
    #
    # e.g, you might see something like:
    #
    #   fourfront-webdev (52.200.253.241) = fourfront-webdev.9wzadzju3p.us-east-1.elasticbeanstalk.com
    #   fourfront-hotseat (52.86.129.23) = fourfront-hotseat.9wzadzju3p.us-east-1.elasticbeanstalk.com
    #   fourfront-mastertest (52.205.221.36) = fourfront-mastertest.9wzadzju3p.us-east-1.elasticbeanstalk.com
    #   fourfront-green (52.72.155.131) = fourfront-blue.9wzadzju3p.us-east-1.elasticbeanstalk.com
    #    => FF STAGING (staging.4dnucleome.org @ 3.221.101.235,52.72.155.131)
    #   fourfront-blue (52.21.221.35) = fourfront-green.us-east-1.elasticbeanstalk.com
    #    => FF PRODUCTION (data.4dnucleome.org @ 34.200.12.21,52.21.221.35)
    #   fourfront-cgapwolf (3.88.80.118) = fourfront-cgapwolf.9wzadzju3p.us-east-1.elasticbeanstalk.com
    #   fourfront-cgaptest (54.173.249.199) = fourfront-cgaptest.9wzadzju3p.us-east-1.elasticbeanstalk.com
    #   fourfront-cgapdev (52.3.110.38) = fourfront-cgapdev.9wzadzju3p.us-east-1.elasticbeanstalk.com
    #   fourfront-cgap (52.21.134.96) = fourfront-cgap.9wzadzju3p.us-east-1.elasticbeanstalk.com
    #    => CGAP PRODUCTION (cgap.hms.harvard.edu @ 34.194.115.62,52.21.134.96)
    #   roles = {
    #     "FF_STAGING": [
    #       "fourfront-green"
    #     ],
    #     "FF_PRODUCTION": [
    #       "fourfront-blue"
    #     ],
    #     "CGAP_PRODUCTION": [
    #       "fourfront-cgap"
    #     ]
    #   }


_FF_BLUEGREEN_CNAMES = {
    env_info['EnvironmentName']: env_info['CNAME']
    for env_info in boto3.client('elasticbeanstalk',
                                 region_name=base.REGION).describe_environments(
                                                             ApplicationName='4dn-web',
                                                             EnvironmentNames=['fourfront-blue',
                                                                               'fourfront-green'])['Environments']
}


_FF_PRODUCTION_IPS = socket.gethostbyname_ex('data.4dnucleome.org')[2]


def _ff_production_env_for_testing():
    for env_name, cname in _FF_BLUEGREEN_CNAMES.items():
        if str(socket.gethostbyname(cname)) in _FF_PRODUCTION_IPS:
            return env_name
    raise RuntimeError("Could not find Fourfront production environment.")


def test_compute_ff_prod_env_by_alternate_computation():
    assert base.compute_ff_prd_env() == _ff_production_env_for_testing()


def test_compute_ff_and_cgap_prd_and_stg_envs():

    def mocked_compute_prd_env_for_project(project):
        return project + "-prd-env"

    def mocked_standard_mirror_env(envname):
        assert envname.endswith("-prd-env"), "mocked_standard_mirror_env does nto handle %r." % envname
        if env_utils.is_fourfront_env(envname):
            return "fourfront-stg-env"
        elif env_utils.is_cgap_env(envname):
            return None
        else:
            raise AssertionError("mocked_standard_mirror_env does not handle %r." % envname)

    with mock.patch.object(base, "_compute_prd_env_for_project") as mock_compute:
        with mock.patch.object(base, "get_standard_mirror_env") as mock_mirror:
            mock_compute.side_effect = mocked_compute_prd_env_for_project
            mock_mirror.side_effect = mocked_standard_mirror_env

            assert base.compute_ff_prd_env() == 'fourfront-prd-env'
            assert base.compute_ff_stg_env() == 'fourfront-stg-env'

            assert base.compute_cgap_prd_env() == 'cgap-prd-env'
            assert base.compute_cgap_stg_env() is None


def test_compute_ff_stg_env_by_alternate_means():
    # NOTE: base.compute_ff_prd_env is tested elsewhere in this file. If that test fails, debug it first!
    actual_ff_prd = base.compute_ff_prd_env()
    expected_prd_options = {FF_ENV_PRODUCTION_BLUE, FF_ENV_PRODUCTION_GREEN}
    assert actual_ff_prd in expected_prd_options
    assert base.compute_ff_stg_env() == (expected_prd_options - {actual_ff_prd}).pop()


def test_compute_cgap_stg_env_by_alternate_means():
    # NOTE: base.compute_cgap_prd_env is tested elsewhere in this file. If that test fails, debug it first!
    actual_cgap_prd = base.compute_cgap_prd_env()
    if actual_cgap_prd == 'fourfront-cgap':
        assert env_utils.get_standard_mirror_env('fourfront-cgap') is None
        assert base.compute_cgap_stg_env() is None
    else:
        expected_prd_options = {CGAP_ENV_PRODUCTION_BLUE_NEW, CGAP_ENV_PRODUCTION_GREEN_NEW}
        assert actual_cgap_prd in expected_prd_options
        assert base.compute_cgap_stg_env() == (expected_prd_options - {actual_cgap_prd}).pop()


def test_compute_prd_env_for_env():

    computed_ff_prd = compute_prd_env_for_env('fourfront-mastertest')
    assert is_fourfront_env(computed_ff_prd)
    assert is_stg_or_prd_env(computed_ff_prd)

    computed_cgap_prd = compute_prd_env_for_env('fourfront-cgapwolf')
    assert is_cgap_env(computed_cgap_prd)
    assert is_stg_or_prd_env(computed_cgap_prd)

    with pytest.raises(NotBeanstalkEnvironment):
        compute_prd_env_for_env('fourfront-production-blue')


def _mocked_describe_beanstalk_environments(*args, **kwargs):
    print("Ignoring args=", args, "kwargs=", kwargs)
    return {
        'Environments': [
            {
                "CNAME": "not." + base._CGAP_MAGIC_CNAME,  # noQA - for testing, don't fuss access protected member
                "EnvironmentName": "cgap-env-1"
            },
            {
                "CNAME": base._CGAP_MAGIC_CNAME,  # noQA - for testing, don't fuss access protected member
                "EnvironmentName": "cgap-env-2"
            },
            {
                "CNAME": "also-not." + base._CGAP_MAGIC_CNAME,  # noQA - for testing, don't fuss access protected member
                "EnvironmentName": "cgap-env-3"
            },
            {
                "CNAME": "not." + base._FF_MAGIC_CNAME,  # noQA - for testing, don't fuss access protected member
                "EnvironmentName": "ff-env-1"
            },
            {
                "CNAME": base._FF_MAGIC_CNAME,  # noQA - for testing, don't fuss access protected member
                "EnvironmentName": "ff-env-2"
            },
            {
                "CNAME": "also-not." + base._FF_MAGIC_CNAME,  # noQA - for testing, don't fuss access protected member
                "EnvironmentName": "ff-env-3"
            },
        ]
    }


def test_compute_prd_env_for_project():
    with mock.patch("boto3.client"):
        with mock.patch.object(base, "describe_beanstalk_environments") as mock_describer:
            mock_describer.side_effect = _mocked_describe_beanstalk_environments
            assert base._compute_prd_env_for_project('cgap') == 'cgap-env-2'
            assert base._compute_prd_env_for_project('ff') == 'ff-env-2'
