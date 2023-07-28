import contextlib
import datetime
import git
import io
import json
import os
import pytest
import re

from dcicutils import contribution_utils as contribution_utils_module
from dcicutils.contribution_utils import Contributor, BasicContributions, Contributions, GitAnalysis
from dcicutils.misc_utils import make_counter  # , override_environ
from dcicutils.qa_utils import MockId, MockFileSystem, printed_output
from typing import List, Optional
from unittest import mock


class MockGitActor:
    def __init__(self, name: Optional[str], email: str):
        self.name = name
        self.email = email


class MockGitCommit:
    def __init__(self, hexsha: str, committed_datetime: str, author: dict,
                 message: str, co_authors: Optional[List[dict]] = None):
        self.hexsha = hexsha
        self.committed_datetime = datetime.datetime.fromisoformat(committed_datetime)
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

    Actor = MockGitActor


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
            "committed_datetime": "2020-01-01 01:23:45",
            "author": {"name": "Jdoe", "email": "jdoe@foo"},
            "message": "something"
        },
        {
            "hexsha": "bbbb",
            "committed_datetime": "2020-01-02 12:34:56",
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
            "committed_datetime": "2020-01-01T01:23:45-05:00",
            "author": {"name": "Jdoe", "email": "jdoe@foo"},
            "message": "something"
        },
        {
            "hexsha": "bbbb",
            "committed_datetime": "2020-01-02T12:34:56-05:00",
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
                'date': '2020-01-01T01:23:45-05:00',
                'message': 'something'
            },
            {
                'author': {'email': 'ssmith@foo', 'name': 'Sally'},
                'coauthors': [{'email': 'bill@someplace', 'name': 'William Simmons'}],
                'commit': 'bbbb',
                'date': '2020-01-02T12:34:56-05:00',
                'message': 'something else'
            }
        ]

        assert foo_commits_as_json == list(GitAnalysis.git_commits('foo'))


def test_contributor_create():

    # This should execute without error
    c1 = Contributor(name="John Doe", email="john@whatever")

    with mock.patch.object(git, "Actor", MockGitActor):

        c2 = Contributor.create(author=git.Actor(name="John Doe", email="john@whatever"))

    assert c1.as_dict() == c2.as_dict()


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


def test_contributor_from_dict():

    joe_json = {'names': ["Joe"], "emails": ["joe@wherever"]}

    joe_obj = Contributor.from_dict(joe_json)

    assert isinstance(joe_obj, Contributor)
    assert list(joe_obj.emails) == joe_json['emails']
    assert list(joe_obj.names) == joe_json['names']

    assert joe_obj.as_dict() == joe_json

    joe_obj2 = Contributor.from_dict(joe_json, key="Joseph")
    assert joe_obj.names != joe_obj2.names
    assert joe_obj.names | {'Joseph'} == joe_obj2.names


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


def test_contributor_values_as_objects():

    assert Contributions.contributor_values_as_objects(None) is None

    idx_json = {'jdoe': {'names': ['jdoe'], 'emails': ['jdoe@somewhere']}}
    idx = Contributions.contributor_values_as_objects(idx_json)
    jdoe = idx['jdoe']

    assert isinstance(jdoe, Contributor)
    assert jdoe.names == set(idx_json['jdoe']['names'])
    assert jdoe.emails == set(idx_json['jdoe']['emails'])


def test_contributor_values_as_dicts():

    assert Contributions.contributor_values_as_dicts(None) is None

    jdoe = Contributor(names={'jdoe'}, emails={'jdoe@somewhere'})
    idx = {'jdoe': jdoe}
    idx_json = Contributions.contributor_values_as_dicts(idx)
    jdoe_json = idx_json['jdoe']

    assert isinstance(jdoe_json, dict)
    assert set(jdoe_json['names']) == jdoe.names
    assert set(jdoe_json['emails']) == jdoe.emails


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

    idx3 = {
        "John Doe": john,
        "jdoe": john.copy(),  # if the info is here twice, it must be in a shared pointer, not a separate object
        "jsmith": jane,
    }

    with pytest.raises(Exception) as exc:
        # This should notice the improper duplication.
        Contributions.contributor_index_by_primary_name(idx3)

    assert str(exc.value).startswith("Name improperly shared")


def test_contributions_by_name_from_by_email():

    email_a = "a@somewhere"
    email_alpha = "alpha@somewhere"

    email_b = "b@somewhere"
    email_beta = "beta@somewhere"

    name_ajones = "ajones"
    name_art = "Art Jones"

    name_bsmith = "bsmith"
    name_betty = "Betty Smith"

    art_jones = Contributor(names={name_art, name_ajones}, emails={email_a, email_alpha})
    betty_smith = Contributor(names={name_betty, name_bsmith}, emails={email_b, email_beta})

    # We should really just need one entry per each person

    by_name_contributors_short = {
        name_art: art_jones,
        name_betty: betty_smith,
    }

    assert Contributions.by_email_from_by_name(by_name_contributors_short) == {
        email_a: art_jones,
        email_alpha: art_jones,
        email_b: betty_smith,
        email_beta: betty_smith,
    }

    # It's OK for there to be more than one entry, as long as the entries share a single contributor object
    # (or contributor json dictionary)

    by_name_contributors = {
        name_ajones: art_jones,
        name_art: art_jones,
        name_bsmith: betty_smith,
        name_betty: betty_smith,
    }

    assert Contributions.by_email_from_by_name(by_name_contributors) == {
        email_a: art_jones,
        email_alpha: art_jones,
        email_b: betty_smith,
        email_beta: betty_smith,
    }

    # It works for the targets of the dictionary to be either Contributors or JSON represented Contributors.

    art_jones_json = art_jones.as_dict()
    betty_smith_json = betty_smith.as_dict()

    by_name_json = {
        name_ajones: art_jones_json,
        name_art: art_jones_json,
        name_bsmith: betty_smith_json,
        name_betty: betty_smith_json,
    }

    assert Contributions.by_email_from_by_name(by_name_json) == {
        email_a: art_jones_json,
        email_alpha: art_jones_json,
        email_b: betty_smith_json,
        email_beta: betty_smith_json,
    }

    # You can't have conflicts between email addresses. That's supposed to have been resolved earlier by unification.

    by_name_json_with_dups = {
        "Art Jones": {"names": ["Art Jones"], "emails": ["ajones@somewhere"]},
        "Arthur Jones": {"names": ["Arthur Jones"], "emails": ["ajones@somewhere"]}
    }

    with pytest.raises(Exception) as exc:
        Contributions.by_email_from_by_name(by_name_json_with_dups)
    assert str(exc.value) == "email address ajones@somewhere is used more than once."


def test_contributions_traverse_terminal_node():

    mariela = Contributor(names={'mari', 'mariela'}, emails={'mariela@somewhere', 'mari@elsewhere'})
    mari = Contributor(names={'mari'}, emails={'mariela@somewhere', 'mari@elsewhere'})

    seen = {mari, mariela}
    originally_seen = seen.copy()

    Contributions.traverse(root=mariela, contributors_by_name={}, contributors_by_email={},
                           # We're testing the cursor=None case
                           seen=seen, cursor=mari)

    assert seen == originally_seen


def test_contributions_pretty_email():

    assert Contributions.pretty_email('joe@foo.com') == 'joe@foo.com'
    assert Contributions.pretty_email('joe@users.noreply.github.com') == 'joe@github'
    assert Contributions.pretty_email('12345+joe@users.noreply.github.com') == 'joe@github'


def test_notice_reference_time():

    timestamps = {}
    timestamp0 = datetime.datetime(2020, 7, 1, 12, 0, 0)
    timestamp1 = datetime.datetime(2020, 7, 1, 12, 0, 1)
    timestamp2 = datetime.datetime(2020, 7, 1, 12, 0, 2)
    key = 'joe'

    Contributions.notice_reference_time(key=key, timestamp=timestamp1, timestamps=timestamps)
    assert timestamps == {key: timestamp1}

    Contributions.notice_reference_time(key=key, timestamp=timestamp0, timestamps=timestamps)
    assert timestamps == {key: timestamp1}

    Contributions.notice_reference_time(key=key, timestamp=timestamp2, timestamps=timestamps)
    assert timestamps == {key: timestamp2}


def test_existing_contributors_json_file():

    mfs = MockFileSystem()

    contributions = BasicContributions(repo='foo')  # don't need all the git history, etc. loaded for this test

    with mfs.mock_exists_open_remove():

        assert contributions.existing_contributors_json_file() is None

        cache_file = contributions.contributors_json_file()
        print("cache_file=", cache_file)
        with io.open(cache_file, 'w') as fp:
            fp.write('something')

        assert contributions.existing_contributors_json_file() == cache_file


def test_contributions_email_reference_time():

    contributions = BasicContributions()
    now = datetime.datetime.now()
    then = now - datetime.timedelta(seconds=10)
    contributions.email_timestamps = {'foo@somewhere': now, 'bar@elsewhere': then}

    assert contributions.email_reference_time('foo@somewhere') == now
    assert contributions.email_reference_time('bar@elsewhere') == then
    assert contributions.email_reference_time('baz@anywhere') is None


def test_contributions_name_reference_time():

    contributions = BasicContributions()
    now = datetime.datetime.now()
    then = now - datetime.timedelta(seconds=10)
    contributions.name_timestamps = {'foo': now, 'bar': then}

    assert contributions.name_reference_time('foo') == now
    assert contributions.name_reference_time('bar') == then
    assert contributions.name_reference_time('baz') is None


def test_file_cache_error_reporting():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():

        with io.open(Contributions.CONTRIBUTORS_CACHE_FILE, 'w') as fp:
            fp.write("{bad json}")

        with printed_output() as printed:
            with pytest.raises(json.JSONDecodeError) as exc:
                Contributions.get_contributors_json_from_file_cache(Contributions.CONTRIBUTORS_CACHE_FILE,)
            assert re.match("Expecting.*line 1 column 2.*", str(exc.value))
            assert printed.lines == ["Error while reading data from 'CONTRIBUTORS.json'."]

