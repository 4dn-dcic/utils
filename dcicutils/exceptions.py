# Exceptions can be put here to get them out of the way of the main flow of things,
# and because once in a while we may want them to be shared or to have shared parents.

from dcicutils.misc_utils import full_object_name, full_class_name, capitalize1


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
