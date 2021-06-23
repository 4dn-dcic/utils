from dcicutils.exceptions import (
    ExpectedErrorNotSeen, WrongErrorSeen, UnexpectedErrorAfterFix, WrongErrorSeenAfterFix,
    ConfigurationError, SynonymousEnvironmentVariablesMismatched, InferredBucketConflict,
    CannotInferEnvFromNoGlobalEnvs, CannotInferEnvFromManyGlobalEnvs, MissingGlobalEnv, GlobalBucketAccessError,
)


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
