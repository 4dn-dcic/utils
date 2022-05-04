from dcicutils.exceptions import (
    ExpectedErrorNotSeen, WrongErrorSeen, UnexpectedErrorAfterFix, WrongErrorSeenAfterFix,
    ConfigurationError, SynonymousEnvironmentVariablesMismatched, InferredBucketConflict,
    CannotInferEnvFromNoGlobalEnvs, CannotInferEnvFromManyGlobalEnvs, MissingGlobalEnv, GlobalBucketAccessError,
    InvalidParameterError, AppKeyMissing, AppServerKeyMissing, AppEnvKeyMissing
)
from dcicutils.creds_utils import CGAPKeyManager


def test_expected_error_not_seen():

    e = ExpectedErrorNotSeen()
    assert e.bug_name == "a bug"
    assert str(e) == "A bug did not occur where expected. It may have been fixed."

    e = ExpectedErrorNotSeen(jira_ticket="ABC-001")
    assert e.bug_name == "Bug ABC-001"
    assert str(e) == "Bug ABC-001 did not occur where expected. It may have been fixed."


def test_wrong_error_seen():

    error_seen = ValueError("Not an integer.")

    e = WrongErrorSeen(expected_class=RuntimeError, error_seen=error_seen)
    assert e.bug_name == "a bug"
    assert e.jira_ticket is None
    assert e.error_seen == error_seen
    assert e.expected_class == RuntimeError
    assert str(e) == (
        'A bug did not occur where expected. An error of class ValueError was thrown'
        ' where one of class RuntimeError was expected. The bug may have been fixed'
        ' or it may be manifesting in an unexpected way: Not an integer.'
    )

    e = WrongErrorSeen(jira_ticket="ABC-001", expected_class=RuntimeError, error_seen=error_seen)
    assert e.bug_name == "Bug ABC-001"
    assert e.jira_ticket == "ABC-001"
    assert e.error_seen == error_seen
    assert e.expected_class == RuntimeError
    assert str(e) == (
        'Bug ABC-001 did not occur where expected. An error of class ValueError was thrown'
        ' where one of class RuntimeError was expected. The bug may have been fixed'
        ' or it may be manifesting in an unexpected way: Not an integer.'
    )


def test_unexpected_error_after_fix():

    error_seen = RuntimeError("Something went wrong.")

    e = UnexpectedErrorAfterFix(expected_class=RuntimeError, error_seen=error_seen)
    assert e.bug_name == "a bug"
    assert e.jira_ticket is None
    assert e.error_seen == error_seen
    assert e.expected_class == RuntimeError
    assert str(e) == (
        'An error of class RuntimeError was thrown where we expected one of class RuntimeError was fixed.'
        ' This may be a regression in the fix for a bug: Something went wrong.'
    )

    e = UnexpectedErrorAfterFix(jira_ticket="ABC-001", expected_class=RuntimeError, error_seen=error_seen)
    assert e.bug_name == "Bug ABC-001"
    assert e.jira_ticket == "ABC-001"
    assert e.error_seen == error_seen
    assert e.expected_class == RuntimeError
    assert str(e) == (
        'An error of class RuntimeError was thrown where we expected one of class RuntimeError was fixed.'
        ' This may be a regression in the fix for Bug ABC-001: Something went wrong.'
    )


def test_wrong_error_seen_after_fix():

    error_seen = ValueError("Not an integer.")

    e = WrongErrorSeenAfterFix(expected_class=RuntimeError, error_seen=error_seen)
    assert e.bug_name == "a bug"
    assert e.jira_ticket is None
    assert e.error_seen == error_seen
    assert e.expected_class == RuntimeError
    assert str(e) == (
        'A bug did not occur, but an error of class ValueError was thrown'
        ' where one of class RuntimeError was expected to be fixed.'
        ' This may be another bug showing through or a regression of some sort: Not an integer.'
    )

    e = WrongErrorSeenAfterFix(jira_ticket="ABC-001", expected_class=RuntimeError, error_seen=error_seen)
    assert e.bug_name == "Bug ABC-001"
    assert e.jira_ticket == "ABC-001"
    assert e.error_seen == error_seen
    assert e.expected_class == RuntimeError
    assert str(e) == (
        'Bug ABC-001 did not occur, but an error of class ValueError was thrown'
        ' where one of class RuntimeError was expected to be fixed.'
        ' This may be another bug showing through or a regression of some sort: Not an integer.'
    )


def test_configuration_error():
    message = "some config error"
    e = ConfigurationError(message)
    assert str(e) == message
    assert isinstance(e, ConfigurationError)
    assert isinstance(e, ValueError)
    assert isinstance(e, Exception)


def test_synonymous_environment_variables_mismatched():
    e = SynonymousEnvironmentVariablesMismatched(var1="twelve", val1="6+6", var2="dozen", val2="10+2")
    assert str(e) == ('The environment variables twelve and dozen are synonyms but have '
                      'inconsistent values: If you supply values for both, they must be the same '
                      "value. You supplied: twelve='6+6' dozen='10+2'")
    assert isinstance(e, SynonymousEnvironmentVariablesMismatched)
    assert isinstance(e, ConfigurationError)
    assert isinstance(e, ValueError)
    assert isinstance(e, Exception)


def test_inferred_bucket_conflict():
    e = InferredBucketConflict(kind="foo", specified="abc", inferred="def")
    assert str(e) == 'Specified foo bucket, abc, and foo bucket inferred from health page, def, do not match.'
    assert isinstance(e, InferredBucketConflict)
    assert isinstance(e, ConfigurationError)
    assert isinstance(e, ValueError)
    assert isinstance(e, Exception)


def test_cannot_infer_env_from_no_global_envs():
    e = CannotInferEnvFromNoGlobalEnvs(global_bucket="my-envs")
    assert str(e) == 'No envs were found in the global env bucket, my-envs, so no env can be inferred.'
    assert isinstance(e, CannotInferEnvFromNoGlobalEnvs)
    assert isinstance(e, ConfigurationError)
    assert isinstance(e, ValueError)
    assert isinstance(e, Exception)


def test_cannot_infer_env_from_many_global_envs():
    e = CannotInferEnvFromManyGlobalEnvs(global_bucket="my-envs", keys=['myorg-prod', 'myorg-dev', 'myorg-test'])
    assert str(e) == ("Too many keys were found in the global env bucket, my-envs,"
                      " for a particular env to be inferred: ['myorg-prod', 'myorg-dev', 'myorg-test']")
    assert isinstance(e, CannotInferEnvFromManyGlobalEnvs)
    assert isinstance(e, ConfigurationError)
    assert isinstance(e, ValueError)
    assert isinstance(e, Exception)


def test_missing_global_env():
    e = MissingGlobalEnv(global_bucket="my-envs", keys=['myorg-prod', 'myorg-dev', 'myorg-test'], env='my-demo')
    assert str(e) == ("No matches for global env bucket: my-envs;"
                      " keys: ['myorg-prod', 'myorg-dev', 'myorg-test'];"
                      " desired env: my-demo")
    assert isinstance(e, MissingGlobalEnv)
    assert isinstance(e, ConfigurationError)
    assert isinstance(e, ValueError)
    assert isinstance(e, Exception)


def test_global_bucket_access_error():
    e = GlobalBucketAccessError(global_bucket="my-envs", status=403)
    assert str(e) == "Could not access global env bucket my-envs: status: 403"
    assert isinstance(e, GlobalBucketAccessError)
    assert isinstance(e, ConfigurationError)
    assert isinstance(e, ValueError)
    assert isinstance(e, Exception)


def test_invalid_parameter_error_with_options_by_argument():

    e = InvalidParameterError(parameter='letter', value='charlie', options=['alpha', 'beta', 'gamma'])
    assert str(e) == "The value of letter, 'charlie', was not valid. Valid values are 'alpha', 'beta' and 'gamma'."

    e = InvalidParameterError(parameter='letter', value='charlie', options=['alpha', 'beta'])
    assert str(e) == "The value of letter, 'charlie', was not valid. Valid values are 'alpha' and 'beta'."

    e = InvalidParameterError(parameter='letter', value='charlie', options=['alpha'])
    assert str(e) == "The value of letter, 'charlie', was not valid. The only valid value is 'alpha'."

    e = InvalidParameterError(parameter='letter', value='charlie', options=[])
    assert str(e) == "The value of letter, 'charlie', was not valid. There are no valid values."

    e = InvalidParameterError(value='charlie', options=[])
    assert str(e) == "The value, 'charlie', was not valid. There are no valid values."

    e = InvalidParameterError(parameter='letter', options=[])
    assert str(e) == "The value of letter was not valid. There are no valid values."

    e = InvalidParameterError(options=[])
    assert str(e) == "The value was not valid. There are no valid values."

    e = InvalidParameterError()
    assert str(e) == "The value was not valid. There are no valid values."

    assert isinstance(e, InvalidParameterError)
    assert isinstance(e, ValueError)
    assert isinstance(e, Exception)


def test_invalid_parameter_error_with_static_options():

    class AlphabetError(InvalidParameterError):
        VALID_OPTIONS = ['alpha', 'beta', 'gamma']

    e = AlphabetError(parameter='letter', value='charlie')
    assert str(e) == "The value of letter, 'charlie', was not valid. Valid values are 'alpha', 'beta' and 'gamma'."

    e = AlphabetError(parameter='letter')
    assert str(e) == "The value of letter was not valid. Valid values are 'alpha', 'beta' and 'gamma'."

    e = AlphabetError(parameter='letter', value=InvalidParameterError.SUPPRESSED)
    assert str(e) == "The value of letter was not valid. Valid values are 'alpha', 'beta' and 'gamma'."

    e = AlphabetError(value='charlie')
    assert str(e) == "The value, 'charlie', was not valid. Valid values are 'alpha', 'beta' and 'gamma'."

    e = AlphabetError()
    assert str(e) == "The value was not valid. Valid values are 'alpha', 'beta' and 'gamma'."

    e = AlphabetError(value=InvalidParameterError.SUPPRESSED)
    assert str(e) == "The value was not valid. Valid values are 'alpha', 'beta' and 'gamma'."

    assert isinstance(e, AlphabetError)
    assert isinstance(e, InvalidParameterError)
    assert isinstance(e, ValueError)
    assert isinstance(e, Exception)


def test_invalid_parameter_error_with_dynamic_options():

    n = 0

    class StretchyError(InvalidParameterError):

        def compute_valid_options(self):
            return [f"val{x}" for x in range(n)]

    e0a = StretchyError()
    e0b = StretchyError()
    assert str(e0a) == "The value was not valid. There are no valid values."
    n += 1  # n == 1. Valid values for newly created errors = ['val0']
    assert str(e0a) == "The value was not valid. There are no valid values."  # created when n == 0
    assert str(e0b) == "The value was not valid. There are no valid values."  # ditto

    e1a = StretchyError()
    e1b = StretchyError()
    assert str(e1a) == "The value was not valid. The only valid value is 'val0'."
    n += 1  # n == 2. Valid values for newly created errors are now ['val0', 'val1']
    assert str(e1a) == "The value was not valid. The only valid value is 'val0'."  # created when n == 1
    assert str(e1b) == "The value was not valid. The only valid value is 'val0'."  # ditto

    e2a = StretchyError()
    e2b = StretchyError()
    assert str(e2a) == "The value was not valid. Valid values are 'val0' and 'val1'."
    n += 1  # n == 3. Valid values for newly created errors = ['val0', 'val1', 'val2']
    assert str(e2a) == "The value was not valid. Valid values are 'val0' and 'val1'."  # created when n == 2
    assert str(e2b) == "The value was not valid. Valid values are 'val0' and 'val1'."  # ditto


def test_app_key_missing():

    error = AppKeyMissing(context="testing", key_manager=CGAPKeyManager())

    assert isinstance(error, RuntimeError)
    assert isinstance(error, AppKeyMissing)

    assert str(error) == "Missing credential in file %s for testing." % CGAPKeyManager._default_keys_file()


def test_app_env_key_missing():
    some_env = 'fourfront-cgapsomething'

    error = AppEnvKeyMissing(env=some_env, key_manager=CGAPKeyManager())

    assert isinstance(error, RuntimeError)
    assert isinstance(error, AppKeyMissing)

    assert str(error) == ("Missing credential in file %s for CGAP environment %s."
                          % (CGAPKeyManager._default_keys_file(), some_env))


def test_app_server_key_missing():

    some_server = "http://127.0.0.1:5000"

    error = AppServerKeyMissing(server=some_server, key_manager=CGAPKeyManager())

    assert isinstance(error, RuntimeError)
    assert isinstance(error, AppKeyMissing)

    assert str(error) == ("Missing credential in file %s for CGAP server %s."
                          % (CGAPKeyManager._default_keys_file(), some_server))
