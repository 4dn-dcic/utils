import io
import os
from dcicutils import beanstalk_utils as bs, source_beanstalk_env_vars
from unittest import mock


def test_get_beanstalk_prod_url():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as man_not_hot:
        man_not_hot.return_value = 'webprod-1'
        url = bs.get_beanstalk_real_url('webprod-1')
        assert url == 'https://data.4dnucleome.org'


def test_get_beanstalk_staging_url():
    with mock.patch('dcicutils.beanstalk_utils.whodaman') as man_not_hot:
        man_not_hot.return_value = 'webprod-2'
        url = bs.get_beanstalk_real_url('webprod-1')
        assert url == 'http://staging.4dnucleome.org'


def test_get_beanstalk_normal_url():
    with mock.patch('dcicutils.beanstalk_utils.beanstalk_info') as man_not_hot:
        man_not_hot.return_value = {'CNAME': 'take-of-your-jacket'}
        url = bs.get_beanstalk_real_url('take-of-your-jacket')
        assert url == 'http://take-of-your-jacket'


def _mock_not_called(name):
    def mock_not_called(*args, **kwargs):
        raise AssertionError("%s was called where not expected." % name)
    return mock_not_called


def test_source_beanstalk_env_vars_no_config_file():
    # subprocess.Popen gets called only if config file exists and AWS_ACCESS_KEY_ID environment variable does not.
    # This tests that if config file does not exist and AWS_ACCESS_KEY_ID does not, it doesn't get called.
    with mock.patch("os.path.exists") as mock_exists:
        with mock.patch.object(os, "environ", {}):
            with mock.patch("subprocess.Popen") as mock_popen:
                mock_exists.return_value = False
                mock_popen = _mock_not_called("subprocess.Popen")
                source_beanstalk_env_vars()


def test_source_beanstalk_env_vars_aws_access_key_id():
    # subprocess.Popen gets called only if config file exists and AWS_ACCESS_KEY_ID environment variable does not.
    # This tests that if config file exists and AWS_ACCESS_KEY_ID does, it doesn't get called.
    with mock.patch("os.path.exists") as mock_exists:
        with mock.patch.object(os, "environ", {"AWS_ACCESS_KEY_ID": "something"}):
            with mock.patch("subprocess.Popen") as mock_popen:
                mock_exists.return_value = True
                mock_popen.side_effect = _mock_not_called("subprocess.Popen")
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
