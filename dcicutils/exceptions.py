# Exceptions can be put here to get them out of the way of the main flow of things,
# and because once in a while we may want them to be shared or to have shared parents.

from .misc_utils import full_object_name, full_class_name, capitalize1


class KnownBugError(AssertionError):

    def set_jira_ticket(self, jira_ticket):
        self.jira_ticket = jira_ticket
        self.bug_name = "a bug" if jira_ticket is None else "Bug %s" % jira_ticket


class UnfixedBugError(KnownBugError):
    pass


class WrongErrorSeen(UnfixedBugError):

    def __init__(self, *, expected_class, error_seen, jira_ticket=None):
        self.set_jira_ticket(jira_ticket)
        self.expected_class = expected_class
        self.error_seen = error_seen
        super().__init__("%s did not occur where expected."
                         " An error of class %s was thrown where one of class %s was expected."
                         " The bug may have been fixed or it may be manifesting in an unexpected way: %s"
                         % (capitalize1(self.bug_name),
                            full_class_name(error_seen),
                            full_object_name(expected_class),
                            error_seen))


class ExpectedErrorNotSeen(UnfixedBugError):

    def __init__(self, *, jira_ticket=None):
        self.set_jira_ticket(jira_ticket)
        super().__init__("%s did not occur where expected. It may have been fixed." % capitalize1(self.bug_name))


class FixedBugError(KnownBugError):
    pass


class WrongErrorSeenAfterFix(FixedBugError):

    def __init__(self, *, expected_class, error_seen, jira_ticket=None):
        self.set_jira_ticket(jira_ticket)
        self.expected_class = expected_class
        self.error_seen = error_seen
        super().__init__("%s did not occur,"
                         " but an error of class %s was thrown where one of class %s was expected to be fixed."
                         " This may be another bug showing through or a regression of some sort: %s"
                         % (capitalize1(self.bug_name),
                            full_class_name(error_seen),
                            full_object_name(expected_class),
                            error_seen))


class UnexpectedErrorAfterFix(FixedBugError):

    def __init__(self, *, expected_class, error_seen, jira_ticket=None):
        self.set_jira_ticket(jira_ticket)
        self.expected_class = expected_class
        self.error_seen = error_seen
        super().__init__("An error of class %s was thrown where we expected one of class %s was fixed."
                         " This may be a regression in the fix for %s: %s"
                         % (full_class_name(error_seen), full_object_name(expected_class), self.bug_name, error_seen))


class ConfigurationError(ValueError):
    pass


class SynonymousEnvironmentVariablesMismatched(ConfigurationError):

    def __init__(self, var1, val1, var2, val2):
        self.var1 = var1
        self.val1 = val1
        self.var2 = var2
        self.val2 = val2
        super().__init__("The environment variables {var1} and {var2} are synonyms but have inconsistent values:"
                         " If you supply values for both, they must be the same value."
                         " You supplied: {var1}={val1!r} {var2}={val2!r}"
                         .format(var1=var1, val1=val1, var2=var2, val2=val2))


class InferredBucketConflict(ConfigurationError):

    def __init__(self, *, kind, specified, inferred):
        self.kind = kind
        self.specified = specified
        self.inferred = inferred
        super().__init__("Specified {kind} bucket, {specified},"
                         " and {kind} bucket inferred from health page, {inferred}, do not match."
                         .format(kind=kind, specified=specified, inferred=inferred))


class CannotInferEnvFromNoGlobalEnvs(ConfigurationError):

    def __init__(self, *, global_bucket):
        self.global_bucket = global_bucket
        super().__init__("No envs were found in the global env bucket, {global_bucket}, so no env can be inferred."
                         .format(global_bucket=global_bucket))


class CannotInferEnvFromManyGlobalEnvs(ConfigurationError):

    def __init__(self, *, global_bucket, keys):
        self.global_bucket = global_bucket
        self.keys = keys
        super().__init__("Too many keys were found in the global env bucket, {global_bucket},"
                         " for a particular env to be inferred: {keys}"
                         .format(global_bucket=global_bucket, keys=keys))


class MissingGlobalEnv(ConfigurationError):

    def __init__(self, *, global_bucket, keys, env):
        self.global_bucket = global_bucket
        self.keys = keys
        self.env = env
        super().__init__("No matches for global env bucket: {global_bucket}; keys: {keys}; desired env: {env}"
                         .format(global_bucket=global_bucket, keys=keys, env=env))


class GlobalBucketAccessError(ConfigurationError):

    def __init__(self, *, global_bucket, status):
        self.global_bucket = global_bucket
        self.status = status
        super().__init__("Could not access global env bucket {global_bucket}: status: {status}"
                         .format(global_bucket=global_bucket, status=status))
