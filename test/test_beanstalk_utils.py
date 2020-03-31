import io
import os
from dcicutils import beanstalk_utils as bs, source_beanstalk_env_vars
from dcicutils.qa_utils import mock_not_called
from unittest import mock


def _mocked_beanstalk_info(env):
    return {'CNAME': 'blah-%s.blahblah.us-east-1.elasticbeanstalk.com' % env}


# Some of the legacy tests for get_beanstalk_real_url don't actually test the way prod works,
# since we don't name anything webprod-1 or weprod-2.  I'm leaving those test in place for
# historical stability (putting "fake" in the test name), but also adding some additional tests
# that test the real names just to be sure. -kmp 30-Mar-2020

def test_get_beanstalk_real_url_fake_prod():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'webprod-1'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info  # mock_not_called('dcicutils.beanstalk_utils.beanstalk_info')
            url = bs.get_beanstalk_real_url('webprod-1')
            assert url == 'https://data.4dnucleome.org'


def test_get_beanstalk_real_url_fake_staging():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'webprod-2'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info  # mock_not_called('dcicutils.beanstalk_utils.beanstalk_info')
            url = bs.get_beanstalk_real_url('webprod-1')
            assert url == 'http://staging.4dnucleome.org'


# These are more like the way it really works in the webprod/webprod2 space:

def test_get_beanstalk_real_url_webprod_data():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'fourfront-webprod'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info  # mock_not_called('dcicutils.beanstalk_utils.beanstalk_info')
            url = bs.get_beanstalk_real_url('fourfront-webprod')
            assert url == 'https://data.4dnucleome.org'


def test_get_beanstalk_real_url_webprod2_data():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'fourfront-webprod2'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info  # mock_not_called('dcicutils.beanstalk_utils.beanstalk_info')
            url = bs.get_beanstalk_real_url('fourfront-webprod2')
            assert url == 'https://data.4dnucleome.org'


def test_get_beanstalk_real_url_webprod_staging():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'fourfront-webprod2'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info  # mock_not_called('dcicutils.beanstalk_utils.beanstalk_info')
            url = bs.get_beanstalk_real_url('fourfront-webprod')
            assert url == 'http://staging.4dnucleome.org'


def test_get_beanstalk_real_url_webprod2_staging():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'fourfront-webprod'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info  # mock_not_called('dcicutils.beanstalk_utils.beanstalk_info')
            url = bs.get_beanstalk_real_url('fourfront-webprod2')
            assert url == 'http://staging.4dnucleome.org'


# These are what the new environments will do in Fourfront:

def test_get_beanstalk_real_url_blue_data():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'fourfront-blue'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info  # mock_not_called('dcicutils.beanstalk_utils.beanstalk_info')
            url = bs.get_beanstalk_real_url('fourfront-blue')
            assert url == 'https://data.4dnucleome.org'


def test_get_beanstalk_real_url_green_data():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'fourfront-green'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info  # mock_not_called('dcicutils.beanstalk_utils.beanstalk_info')
            url = bs.get_beanstalk_real_url('fourfront-green')
            assert url == 'https://data.4dnucleome.org'


def test_get_beanstalk_real_url_blue_staging():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'fourfront-green'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info  # mock_not_called('dcicutils.beanstalk_utils.beanstalk_info')
            url = bs.get_beanstalk_real_url('fourfront-blue')
            assert url == 'http://staging.4dnucleome.org'


def test_get_beanstalk_real_url_green_staging():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'fourfront-blue'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info  # mock_not_called('dcicutils.beanstalk_utils.beanstalk_info')
            url = bs.get_beanstalk_real_url('fourfront-green')
            assert url == 'http://staging.4dnucleome.org'


# Non-production environments will do this:

def test_get_beanstalk_real_url_other():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.side_effect = mock_not_called('dcicutils.beanstalk_utils.whodaman')
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info
            url = bs.get_beanstalk_real_url('beanstalk-name')
            assert url == 'http://blah-beanstalk-name.blahblah.us-east-1.elasticbeanstalk.com'


# These will fail in CGAP, which Soo reported (C4-101):

def test_get_beanstalk_real_url_cgap():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as mock_whodaman:
        with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as mock_beanstalk_info:
            mock_whodaman.return_value = 'fourfront-cgap'
            mock_beanstalk_info.side_effect = _mocked_beanstalk_info  # mock_not_called('dcicutils.beanstalk_utils.beanstalk_info')
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
