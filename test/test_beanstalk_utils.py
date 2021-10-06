import pytest
import io
import os

from dcicutils import beanstalk_utils
from dcicutils.beanstalk_utils import (
    ENV_VARIABLE_NAMESPACE,
    compute_ff_prd_env, get_beanstalk_environment_variables, source_beanstalk_env_vars,
)
from dcicutils.qa_utils import mock_not_called
from dcicutils.misc_utils import ignored
from unittest import mock
from .test_base import _ff_production_env_for_testing


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


def test_compute_ff_prd_env_by_alternate_computation():
    assert compute_ff_prd_env() == _ff_production_env_for_testing()


@pytest.mark.parametrize('options, expected', [
    ([
         {
            'Namespace': ENV_VARIABLE_NAMESPACE,
            'OptionName': 'super_secret',
            'Value': 'i am secret'
         }
     ], {'super_secret': 'i am secret'}),
    ([
         {
            'Namespace': ENV_VARIABLE_NAMESPACE,
            'OptionName': 'super_secret',
            'Value': 'i am secret'
         },
         {
            'Namespace': 'something else',
            'OptionName': 'not_secret',
            'Value': 'i dont care about this value'
         }
     ], {'super_secret': 'i am secret'}),
    ([
         {
            'Namespace': 'identifier',
            'OptionName': 'something',
            'Value': 'important'
         },
         {
            'Namespace': 'something else',
            'OptionName': 'not_secret',
            'Value': 'i dont care about this value'
         }
     ], {}),
    ([
         {
            'Namespace': ENV_VARIABLE_NAMESPACE,
            'OptionName': 'super_secret',
            'Value': 'i am secret'
         },
         {
            'Namespace': ENV_VARIABLE_NAMESPACE,
            'OptionName': 'not_secret',
            'Value': 'this one shows up too though'
         }
     ], {'super_secret': 'i am secret', 'not_secret': 'this one shows up too though'}),
])
def test_get_beanstalk_env_variables(options, expected):
    with mock.patch.object(beanstalk_utils, '_get_beanstalk_configuration_settings') as mock_api:
        mock_api.return_value = options  # do not call out to AWS
        actual = get_beanstalk_environment_variables('unused')
        assert actual == expected
