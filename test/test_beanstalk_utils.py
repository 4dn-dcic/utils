import boto3
import io
import json
import os
import socket
from collections import defaultdict
from dcicutils import beanstalk_utils as bs, env_utils, source_beanstalk_env_vars, compute_prd_env_for_env
from dcicutils.env_utils import is_fourfront_env, is_cgap_env, is_stg_or_prd_env
from dcicutils.qa_utils import mock_not_called
from dcicutils.misc_utils import ignored
from unittest import mock


def _mocked_beanstalk_info(env):
    return {'CNAME': 'blah-%s.blahblah.us-east-1.elasticbeanstalk.com' % env}


def test_get_beanstalk_real_url_blue_data():
    with mock.patch('dcicutils.beanstalk_utils._compute_prd_env_for_project') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'fourfront-blue'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info
            url = bs.get_beanstalk_real_url('fourfront-blue')
            assert url == 'https://data.4dnucleome.org'


def test_get_beanstalk_real_url_green_data():
    with mock.patch('dcicutils.beanstalk_utils._compute_prd_env_for_project') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'fourfront-green'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info
            url = bs.get_beanstalk_real_url('fourfront-green')
            assert url == 'https://data.4dnucleome.org'


def test_get_beanstalk_real_url_blue_staging():
    with mock.patch('dcicutils.beanstalk_utils._compute_prd_env_for_project') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'fourfront-green'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info
            url = bs.get_beanstalk_real_url('fourfront-blue')
            assert url == 'http://staging.4dnucleome.org'


def test_get_beanstalk_real_url_green_staging():
    with mock.patch('dcicutils.beanstalk_utils._compute_prd_env_for_project') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'fourfront-blue'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info
            url = bs.get_beanstalk_real_url('fourfront-green')
            assert url == 'http://staging.4dnucleome.org'


# Non-production environments will do this:

def test_get_beanstalk_real_url_other():
    with mock.patch('dcicutils.beanstalk_utils._compute_prd_env_for_project') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.side_effect = mock_not_called('dcicutils.beanstalk_utils._compute_prd_env_for_project')
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info
            url = bs.get_beanstalk_real_url('beanstalk-name')
            assert url == 'http://blah-beanstalk-name.blahblah.us-east-1.elasticbeanstalk.com'


# These will fail in CGAP, which Soo reported (C4-101):

def test_get_beanstalk_real_url_cgap():
    with mock.patch('dcicutils.beanstalk_utils._compute_prd_env_for_project') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'fourfront-cgap'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info
            url = bs.get_beanstalk_real_url('fourfront-cgap')
            assert url == 'https://cgap.hms.harvard.edu'


def test_source_beanstalk_env_vars_no_config_file():
    # subprocess.Popen gets called only if config file exists and AWS_ACCESS_KEY_ID environment variable does not.
    # This tests that if config file does not exist and AWS_ACCESS_KEY_ID does not, it doesn't get called.
    with mock.patch("os.path.exists") as mock_exists:
        with mock.patch.object(os, "environ", {}):
            with mock.patch("subprocess.Popen") as mock_popen:
                mock_exists.return_value = False
                mock_popen.side_effect = mock_not_called("subprocess.Popen")
                source_beanstalk_env_vars()


def test_source_beanstalk_env_vars_aws_access_key_id():
    # subprocess.Popen gets called only if config file exists and AWS_ACCESS_KEY_ID environment variable does not.
    # This tests that if config file exists and AWS_ACCESS_KEY_ID does, it doesn't get called.
    with mock.patch("os.path.exists") as mock_exists:
        with mock.patch.object(os, "environ", {"AWS_ACCESS_KEY_ID": "something"}):
            with mock.patch("subprocess.Popen") as mock_popen:
                mock_exists.return_value = True
                mock_popen.side_effect = mock_not_called("subprocess.Popen")
                source_beanstalk_env_vars()


def test_source_beanstalk_env_vars_normal():
    # subprocess.Popen gets called only if config file exists and AWS_ACCESS_KEY_ID environment variable does not.
    # In the normal case, both of those conditions are true, and so it opens the file and parses it,
    # setting os.environ to hold the relevant values.
    with mock.patch("os.path.exists") as mock_exists:
        fake_env = {}
        with mock.patch.object(os, "environ", fake_env):
            with mock.patch("subprocess.Popen") as mock_popen:
                mock_exists.return_value = True

                class FakeSubprocessPipe:

                    def __init__(self, *args, **kwargs):
                        ignored(args, kwargs)
                        self.stdout = io.StringIO(
                            'AWS_ACCESS_KEY_ID=12345\n'
                            'AWS_FAKE_SECRET=amazon\n'
                        )

                    def communicate(self):
                        pass
                mock_popen.side_effect = FakeSubprocessPipe
                source_beanstalk_env_vars()
                assert fake_env == {
                    'AWS_ACCESS_KEY_ID': '12345',
                    'AWS_FAKE_SECRET': 'amazon'
                }


def _ip_addresses(hostname):
    return sorted(socket.gethostbyname_ex(hostname)[2])


def test_magic_cnames_by_production_ip_address():
    # This simple check just makes sure the obvious truths are checked.
    assert _ip_addresses(bs.FF_MAGIC_CNAME) == _ip_addresses("data.4dnucleome.org")
    assert _ip_addresses(bs.CGAP_MAGIC_CNAME) == _ip_addresses("cgap.hms.harvard.edu")


def test_magic_cnames_by_cname_consistency():

    # These tests are highly specific and will have to change if we make something else be magic.
    # But such is magic. The values should not be casually changed, and such overhead is appropriate.
    # It's good to have tests that will catch unwanted tinkering, typos, etc.

    ff_magic_envs = [env_utils.FF_ENV_PRODUCTION_GREEN, env_utils.FF_ENV_PRODUCTION_BLUE]
    cgap_magic_envs = [env_utils.CGAP_ENV_WEBPROD]

    assert ff_magic_envs == ['fourfront-green', 'fourfront-blue']
    assert cgap_magic_envs == ['fourfront-cgap']

    client = boto3.client('elasticbeanstalk', region_name=bs.REGION)
    res = bs.describe_beanstalk_environments(client, ApplicationName='4dn-web')
    envs = res['Environments']
    roles = defaultdict(lambda: [])
    for env in envs:
        env_name = env.get('EnvironmentName')
        cname = env.get('CNAME')
        ip = socket.gethostbyname(cname)
        note = ""
        if cname == bs.FF_MAGIC_CNAME:
            assert env_name in ff_magic_envs
            ff_prod = "data.4dnucleome.org"
            ff_prod_ips = _ip_addresses(ff_prod)
            assert _ip_addresses(cname) == ff_prod_ips
            note = "\n => FF PRODUCTION (%s @ %s)" % (ff_prod, ",".join(ff_prod_ips))
            roles['FF_PRODUCTION'].append(env_name)
            assert env_utils.is_stg_or_prd_env(env_name)
        elif cname == bs.CGAP_MAGIC_CNAME:
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
                                 region_name=bs.REGION).describe_environments(
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


def test_deprecated_whodaman_by_alternate_computation():
    assert bs.whodaman() == _ff_production_env_for_testing()


def test_deprecated_whodaman():
    # This just makes sure that the old name is properly retained, since it's used in a lot of other repos.
    assert bs.whodaman is bs.compute_ff_prd_env


def test_compute_ff_prod_env_by_alternate_computation():
    assert bs.compute_ff_prd_env() == _ff_production_env_for_testing()


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

    with mock.patch("dcicutils.beanstalk_utils._compute_prd_env_for_project") as mock_compute:
        with mock.patch("dcicutils.beanstalk_utils.get_standard_mirror_env") as mock_mirror:
            mock_compute.side_effect = mocked_compute_prd_env_for_project
            mock_mirror.side_effect = mocked_standard_mirror_env

            assert bs.compute_ff_prd_env() == 'fourfront-prd-env'
            assert bs.compute_ff_stg_env() == 'fourfront-stg-env'

            assert bs.compute_cgap_prd_env() == 'cgap-prd-env'
            assert bs.compute_cgap_stg_env() is None


def test_compute_ff_stg_env_by_alternate_means():
    # NOTE: bs.compute_ff_prd_env is tested elsewhere in this file. If that test is failing, debug that problem first!
    actual_ff_prd = bs.compute_ff_prd_env()
    expected_prd_options = {env_utils.FF_ENV_PRODUCTION_BLUE, env_utils.FF_ENV_PRODUCTION_GREEN}
    assert actual_ff_prd in expected_prd_options
    assert bs.compute_ff_stg_env() == (expected_prd_options - {actual_ff_prd}).pop()


def test_compute_cgap_stg_env_by_alternate_means():
    # NOTE: bs.compute_cgap_prd_env is tested elsewhere in this file. If that test is failing, debug that problem first!
    actual_cgap_prd = bs.compute_cgap_prd_env()
    if actual_cgap_prd == 'fourfront-cgap':
        assert env_utils.get_standard_mirror_env('fourfront-cgap') is None
        assert bs.compute_cgap_stg_env() is None
    else:
        expected_prd_options = {env_utils.CGAP_ENV_PRODUCTION_BLUE_NEW, env_utils.CGAP_ENV_PRODUCTION_GREEN_NEW}
        assert actual_cgap_prd in expected_prd_options
        assert bs.compute_cgap_stg_env() == (expected_prd_options - {actual_cgap_prd}).pop()


def test_compute_prd_env_for_env():

    computed_ff_prd = compute_prd_env_for_env('fourfront-mastertest')
    assert is_fourfront_env(computed_ff_prd)
    assert is_stg_or_prd_env(computed_ff_prd)

    computed_cgap_prd = compute_prd_env_for_env('fourfront-cgapwolf')
    assert is_cgap_env(computed_cgap_prd)
    assert is_stg_or_prd_env(computed_cgap_prd)



def _mocked_describe_beanstalk_environments(*args, **kwargs):
    print("Ignoring args=", args, "kwargs=", kwargs)
    return {
        'Environments': [
            {
                "CNAME": "not." + bs.CGAP_MAGIC_CNAME,
                "EnvironmentName": "cgap-env-1"
            },
            {
                "CNAME": bs.CGAP_MAGIC_CNAME,
                "EnvironmentName": "cgap-env-2"
            },
            {
                "CNAME": "also-not." + bs.CGAP_MAGIC_CNAME,
                "EnvironmentName": "cgap-env-3"
            },
            {
                "CNAME": "not." + bs.FF_MAGIC_CNAME,
                "EnvironmentName": "ff-env-1"
            },
            {
                "CNAME": bs.FF_MAGIC_CNAME,
                "EnvironmentName": "ff-env-2"
            },
            {
                "CNAME": "also-not." + bs.FF_MAGIC_CNAME,
                "EnvironmentName": "ff-env-3"
            },
        ]
    }


def test_compute_prd_env_for_project():
    with mock.patch("boto3.client"):
        with mock.patch("dcicutils.beanstalk_utils.describe_beanstalk_environments") as mock_describer:
            mock_describer.side_effect = _mocked_describe_beanstalk_environments
            assert bs._compute_prd_env_for_project('cgap') == 'cgap-env-2'
            assert bs._compute_prd_env_for_project('ff') == 'ff-env-2'
