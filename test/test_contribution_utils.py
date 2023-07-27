import contextlib
import os
import pytest

from dcicutils import contribution_utils as contribution_utils_module
from dcicutils.contribution_utils import Contributor, Contributions, GitAnalysis
from dcicutils.misc_utils import make_counter  # , override_environ
from dcicutils.qa_utils import MockId
from typing import List, Optional
from unittest import mock


class MockGitActor:
    def __init__(self, name: Optional[str], email: str):
        self.name = name
        self.email = email


class MockGitCommit:
    def __init__(self, hexsha: str, authored_datetime: str, author: dict,
                 message: str, co_authors: Optional[List[dict]] = None):
        self.hexsha = hexsha
        self.authored_datetime = authored_datetime
        self.author = MockGitActor(**author)
        self.co_authors = [MockGitActor(**co_author) for co_author in (co_authors or [])]
        self.message = message


class MockGitRepo:

    def __init__(self, name, path, mocked_commits=None):
        self.name = name
        self.path = path
        self.mocked_commits = mocked_commits or []

    def iter_commits(self):
        for json_data in self.mocked_commits:
            yield MockGitCommit(**json_data)


class MockGitModule:

    def __init__(self, mocked_commits=None):
        if mocked_commits is None:
            mocked_commits = {}
        self._mocked_commits = mocked_commits

    def Repo(self, path):
        repo_name = os.path.basename(path)
        return MockGitRepo(name=repo_name, path=path, mocked_commits=self._mocked_commits.get(repo_name, []))


SAMPLE_USER_HOME = "/home/jdoe"
SAMPLE_PROJECT_HOME = f"{SAMPLE_USER_HOME}/repos"
SAMPLE_FOO_HOME = f"{SAMPLE_PROJECT_HOME}/foo"


@contextlib.contextmanager
def git_context(project_home=SAMPLE_PROJECT_HOME, mocked_commits=None):
    with mock.patch.object(contribution_utils_module, "git", MockGitModule(mocked_commits=mocked_commits)):
        with mock.patch.object(contribution_utils_module, "PROJECT_HOME", project_home):
            yield


def test_git_analysis_find_repo():

    with git_context():
        foo_repo = GitAnalysis.find_repo("foo")
        assert isinstance(foo_repo, MockGitRepo)
        assert foo_repo.name == 'foo'
        assert foo_repo.path == SAMPLE_FOO_HOME
        assert foo_repo.mocked_commits == []


def test_git_analysis_git_commits():

    with git_context(mocked_commits={"foo": [
        {
            "hexsha": "aaaa",
            "authored_datetime": "2020-01-01 01:23:45",
            "author": {"name": "Jdoe", "email": "jdoe@foo"},
            "message": "something"
        },
        {
            "hexsha": "bbbb",
            "authored_datetime": "2020-01-02 12:34:56",
            "author": {"name": "Sally", "email": "ssmith@foo"},
            "message": "something else"
        }
    ]}):
        foo_repo = GitAnalysis.find_repo('foo')
        foo_commits = list(foo_repo.iter_commits())
        assert len(foo_commits) == 2
        assert all(isinstance(commit, MockGitCommit) for commit in foo_commits)
        assert GitAnalysis.json_for_actor(foo_commits[0].author) == {'name': 'Jdoe', 'email': 'jdoe@foo'}
        assert GitAnalysis.json_for_actor(foo_commits[1].author) == {'name': 'Sally', 'email': 'ssmith@foo'}
        assert foo_commits[0].hexsha == 'aaaa'
        assert foo_commits[1].hexsha == 'bbbb'

        assert [GitAnalysis.json_for_commit(commit) for commit in foo_commits] == list(GitAnalysis.git_commits('foo'))


def test_git_analysis_iter_commits_scenario():  # Tests .iter_commits, .json_for_commit, .json_for_actor, .git_commits

    with git_context(mocked_commits={"foo": [
        {
            "hexsha": "aaaa",
            "authored_datetime": "2020-01-01 01:23:45",
            "author": {"name": "Jdoe", "email": "jdoe@foo"},
            "message": "something"
        },
        {
            "hexsha": "bbbb",
            "authored_datetime": "2020-01-02 12:34:56",
            "author": {"name": "Sally", "email": "ssmith@foo"},
            "co_authors": [{"name": "William Simmons", "email": "bill@someplace"}],
            "message": "something else"
        }
    ]}):
        foo_repo = GitAnalysis.find_repo("foo")
        foo_commits_as_json = [GitAnalysis.json_for_commit(commit) for commit in foo_repo.iter_commits()]
        assert foo_commits_as_json == [
            {
                'author': {'email': 'jdoe@foo', 'name': 'Jdoe'},
                'coauthors': [],
                'commit': 'aaaa',
                'date': '2020-01-01 01:23:45',
                'message': 'something'
            },
            {
                'author': {'email': 'ssmith@foo', 'name': 'Sally'},
                'coauthors': [{'email': 'bill@someplace', 'name': 'William Simmons'}],
                'commit': 'bbbb',
                'date': '2020-01-02 12:34:56',
                'message': 'something else'
            }
        ]

        assert foo_commits_as_json == list(GitAnalysis.git_commits('foo'))


def test_contributor_str():

    with mock.patch.object(contribution_utils_module, "id", MockId(1000)):

        john = Contributor(names={"John Doe", "jdoe"}, email="john@whatever")

        assert str(john) == "<Contributor emails='john@whatever' names='jdoe','John Doe' 1000>"

        jdoe = Contributor(names={"John Doe", "jdoe"}, primary_name="jdoe", email="john@whatever")

        assert str(jdoe) == "<Contributor 'jdoe' emails='john@whatever' names='jdoe','John Doe' 1001>"

        # Edge cases...

        with pytest.raises(ValueError) as exc:
            Contributor()
        assert str(exc.value) == "One of email= or emails= is required."

        with pytest.raises(ValueError) as exc:
            Contributor(email='foo@bar', emails={'foo@bar'})
        assert str(exc.value) == 'Only one of email= and emails= may be provided.'

        with pytest.raises(ValueError) as exc:
            Contributor(name='John', names={'John'}, email="foo@bar")
        assert str(exc.value) == 'Only one of name= and names= may be provided.'


def test_contributor_set_primary_name():

    john = Contributor(names={"John Doe", "jdoe"}, email="john@whatever")
    assert john.primary_name == "John Doe"

    john.names.add("John Q Doe")
    assert john.primary_name == "John Q Doe"

    john.set_primary_name("John Doe")
    assert john.primary_name == "John Doe"

    john.names.add("John Quincy Doe")
    assert john.primary_name == "John Doe"

    assert "Quincy" not in john.names
    john.set_primary_name("Quincy")
    assert john.primary_name == "Quincy"
    assert "Quincy" in john.names


def test_contributor_copy():

    john = Contributor(name="john", email="john@foo")
    jack = john.copy()

    assert john.names == {"john"}
    assert john.emails == {"john@foo"}
    assert john.primary_name == "john"

    jack.names.add("jack")
    jack.emails.add("jack@bar")
    jack.set_primary_name("jack")

    assert john.names == {"john"}
    assert john.emails == {"john@foo"}
    assert john.primary_name == "john"

    assert jack.names == {"john", "jack"}
    assert jack.emails == {"john@foo", "jack@bar"}
    assert jack.primary_name == "jack"


def test_contributor_primary_name():

    email_counter = make_counter()

    def make_contributor(names, primary_name=None):
        some_email = f"user{email_counter()}@something.foo"  # unique but we don't care about the specific value
        return Contributor(names=set(names), email=some_email, primary_name=primary_name)

    assert make_contributor({"John Doe", "jdoe"}).primary_name == "John Doe"
    assert make_contributor({"jdoe", "John Doe"}).primary_name == "John Doe"
    assert make_contributor({"jdoe", "John Doe", "jdoe123456789"}).primary_name == "John Doe"
    assert make_contributor({"jdoe123456789", "John Doe", "jdoe"}).primary_name == "John Doe"

    assert make_contributor({"jdoe123456789", "John Doe", "John Q Doe", "jdoe"}).primary_name == "John Q Doe"
    assert make_contributor({"jdoe123456789", "John Q Doe", "John Doe", "jdoe"}).primary_name == "John Q Doe"

    assert make_contributor({"John Doe", "jdoe"}, primary_name="jdoe").primary_name == "jdoe"
    assert make_contributor({"jdoe", "John Doe"}, primary_name="jdoe").primary_name == "jdoe"
    assert make_contributor({"jdoe", "John Doe", "jdoe123456789"}, primary_name="jdoe").primary_name == "jdoe"
    assert make_contributor({"jdoe123456789", "John Doe", "jdoe"}, primary_name="jdoe").primary_name == "jdoe"


def test_contributor_notice_mention_as():

    john = Contributor(name="john", email="john@foo")

    assert john.names == {"john"}
    assert john.emails == {"john@foo"}

    john.notice_mention_as(name="jack", email="john@foo")

    assert john.names == {"john", "jack"}
    assert john.emails == {"john@foo"}

    john.notice_mention_as(name="john", email="jack@bar")

    assert john.names == {"john", "jack"}
    assert john.emails == {"john@foo", "jack@bar"}

    john.notice_mention_as(name="john", email="john@foo")

    assert john.names == {"john", "jack"}
    assert john.emails == {"john@foo", "jack@bar"}


def test_contributor_as_dict():

    john = Contributor(name="john", email="john@foo")

    assert john.as_dict() == {
        "names": ["john"],
        "emails": ["john@foo"]
    }

    john.notice_mention_as(name="jack", email="john@foo")

    assert john.as_dict() == {
        "names": ["jack", "john"],
        "emails": ["john@foo"]
    }

    john.notice_mention_as(name="Richard", email="john@foo")

    assert john.as_dict() == {
        # As it happens, our sort is case-sensitive. See notes in source, but that's just because it was easiest.
        # This test is just to notice on a simple example if that changes, since bigger examples could be harder
        # to debug. -kmp 27-Jul-2023
        "names": ["Richard", "jack", "john"],
        "emails": ["john@foo"]
    }


def test_contributor_index_by_primary_name():

    john = Contributor(names={"John Doe", "jdoe"}, emails={"jdoe@a.foo"})
    jane = Contributor(names={"jsmith", "Jane Smith"}, emails={"jsmith@b.foo"})

    idx1 = {
        "John Doe": john,
        "Jane Smith": jane,
        "jdoe": john,
        "jsmith": jane,
    }

    assert list(Contributions.contributor_index_by_primary_name(idx1).items()) == [
        ("Jane Smith", jane),
        ("John Doe", john),
    ]

    idx2 = {
        "jdoe": john,
        "jsmith": jane,
    }

    assert list(Contributions.contributor_index_by_primary_name(idx2).items()) == [
        ("Jane Smith", jane),
        ("John Doe", john),
    ]
