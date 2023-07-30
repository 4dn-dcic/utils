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
from dcicutils.misc_utils import make_counter, file_contents  # , override_environ
from dcicutils.qa_utils import MockId, MockFileSystem, printed_output
from typing import Dict, List, Optional
from unittest import mock


class MockGitActor:

    def __init__(self, name: Optional[str], email: str):
        self.name = name
        self.email = email

    def __str__(self):
        return f"<~Actor {self.name} ({self.email})>"


class MockGitCommit:

    def __init__(self, hexsha: str, committed_datetime: str, author: dict,
                 message: str, co_authors: Optional[List[dict]] = None):
        self.hexsha = hexsha
        self.committed_datetime = datetime.datetime.fromisoformat(committed_datetime)
        self.author = MockGitActor(**author)
        self.co_authors = [MockGitActor(**co_author) for co_author in (co_authors or [])]
        self.message = message

    def __str__(self):
        return (f"<~Commit {self.hexsha}"
                f" {self.committed_datetime.isoformat()}"
                f" {self.author.email}"
                f" {','.join(c.email for c in self.co_authors)}>")


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

    mocked_commits = [
        {
            "hexsha": "bbbb",
            "committed_datetime": "2020-01-02 12:34:56",
            "author": {"name": "Sally", "email": "ssmith@foo"},
            "message": "something else"
        },
        {
            "hexsha": "aaaa",
            "committed_datetime": "2020-01-01 01:23:45",
            "author": {"name": "Jdoe", "email": "jdoe@foo"},
            "message": "something"
        },
    ]

    with git_context(mocked_commits={"foo": mocked_commits}):
        foo_repo = GitAnalysis.find_repo('foo')
        foo_commits = list(foo_repo.iter_commits())
        assert len(foo_commits) == 2
        assert all(isinstance(commit, MockGitCommit) for commit in foo_commits)
        assert GitAnalysis.json_for_actor(foo_commits[0].author) == {'name': 'Sally', 'email': 'ssmith@foo'}
        assert GitAnalysis.json_for_actor(foo_commits[1].author) == {'name': 'Jdoe', 'email': 'jdoe@foo'}
        assert foo_commits[0].hexsha == 'bbbb'
        assert foo_commits[1].hexsha == 'aaaa'

        assert [GitAnalysis.json_for_commit(commit) for commit in foo_commits] == list(GitAnalysis.git_commits('foo'))


def test_git_analysis_iter_commits_scenario():  # Tests .iter_commits, .json_for_commit, .json_for_actor, .git_commits

    mocked_commits = [
        {
            "hexsha": "bbbb",
            "committed_datetime": "2020-01-02T12:34:56-05:00",
            "author": {"name": "Sally", "email": "ssmith@foo"},
            "co_authors": [{"name": "William Simmons", "email": "bill@someplace"}],
            "message": "something else"
        },
        {
            "hexsha": "aaaa",
            "committed_datetime": "2020-01-01T01:23:45-05:00",
            "author": {"name": "Jdoe", "email": "jdoe@foo"},
            "message": "something"
        },
    ]

    with git_context(mocked_commits={"foo": mocked_commits}):
        foo_repo = GitAnalysis.find_repo("foo")
        foo_commits_as_json = [GitAnalysis.json_for_commit(commit) for commit in foo_repo.iter_commits()]
        assert foo_commits_as_json == [
            {
                'author': {'email': 'ssmith@foo', 'name': 'Sally'},
                'coauthors': [{'email': 'bill@someplace', 'name': 'William Simmons'}],
                'commit': 'bbbb',
                'date': '2020-01-02T12:34:56-05:00',
                'message': 'something else'
            },
            {
                'author': {'email': 'jdoe@foo', 'name': 'Jdoe'},
                'coauthors': [],
                'commit': 'aaaa',
                'date': '2020-01-01T01:23:45-05:00',
                'message': 'something'
            },
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


def test_save_contributor_data():

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove_abspath_getcwd_chdir():

        contributions = BasicContributions()
        cache_file = contributions.save_contributor_data()

        assert file_contents(cache_file) == (
            '{\n'
            '  "forked_at": null,\n'
            '  "pre_fork_contributors_by_name": null,\n'
            '  "contributors_by_name": null\n'
            '}\n'
        )

        cache_file_2 = contributions.save_contributor_data('some.file')
        assert cache_file_2 == 'some.file'
        assert file_contents(cache_file_2) == (
            '{\n'
            '  "forked_at": null,\n'
            '  "pre_fork_contributors_by_name": null,\n'
            '  "contributors_by_name": null\n'
            '}\n'
        )


def test_repo_contributor_names():

    contributions = BasicContributions()
    tony = Contributor(names={"Tony", "Anthony"}, emails={"tony@foo"})
    juan = Contributor(names={"Juan"}, emails={"juan@foo"})
    contributions.contributors_by_name = {
        "Tony": tony,
        "Juan": juan,
    }
    assert list(contributions.repo_contributor_names(with_email=False)) == [
        "Tony",
        "Juan",
    ]
    assert list(contributions.repo_contributor_names(with_email=True)) == [
        "Tony (tony@foo)",
        "Juan (juan@foo)",
    ]
    # We assume de-duplication has already occurred, so something like this will happen.
    # This behavior is not a feature, but then again, there's no reason to check/redo what should be done earlier.
    contributions.contributors_by_name = {
        "Tony": tony,
        "Juan": juan,
        "Anthony": tony,
    }
    assert list(contributions.repo_contributor_names(with_email=True)) == [
        "Tony (tony@foo)",
        "Juan (juan@foo)",
        "Anthony (tony@foo)",
    ]
    # Note, too, that it's trusting the key because our save code puts the primary_key in the dictionary key,
    # and if it's been edited by a human, we'll prefer that.
    contributions.contributors_by_name = {
        "Tony": tony,
        "John": juan,
    }
    assert list(contributions.repo_contributor_names(with_email=True)) == [
        "Tony (tony@foo)",
        "John (juan@foo)",
    ]


def test_resummarize_discrepancies():

    assert Contributions.resummarize_discrepancies({
        'added': ['a', 'b'],
        'changed': ['c'],
        'removed': ['d', 'e'],
    }) == {
        'to_add': ['a', 'b'],
        'to_change': ['c'],
        'to_remove': ['d', 'e'],
    }


def test_show_repo_contributors_file_missing():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with printed_output() as printed:
            with git_context(mocked_commits={'foo': []}):

                expected_message = 'Need to create a CONTRIBUTORS.json file for foo.'

                Contributions(repo='foo').show_repo_contributors(error_class=None)
                assert printed.lines == [expected_message]

                with pytest.raises(AssertionError) as exc:
                    Contributions(repo='foo').show_repo_contributors(error_class=AssertionError)
                assert str(exc.value) == expected_message


def test_contributions_init_with_cached_pre_fork_names():

    print()  # start on a fresh line

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove_abspath_getcwd_chdir():

        os.chdir(SAMPLE_FOO_HOME)

        with io.open(os.path.abspath(Contributions.CONTRIBUTORS_CACHE_FILE), 'w') as fp:
            cache_data = {
                "forked_at": "2020-01-02T12:34:56-05:00",
                "pre_fork_contributors_by_name": {"Jessica": {"names": ["Jessica"], "emails": ["jdoe@foo"]}},
                "contributors_by_name": {}
            }
            json.dump(cache_data, fp=fp)

        mocked_commits = [
            {
                "hexsha": "bbbb",
                "committed_datetime": "2020-01-02T12:34:56-05:00",
                "author": {"name": "Sally", "email": "ssmith@foo"},
                "co_authors": [{"name": "William Simmons", "email": "bill@someplace"}],
                "message": "something else"
            },
            {
                "hexsha": "aaaa",
                "committed_datetime": "2020-01-01T01:23:45-05:00",
                "author": {"name": "Jessica", "email": "jdoe@foo"},
                "message": "something"
            },
        ]

        with git_context(mocked_commits={"foo": mocked_commits}):

            contributions = Contributions(repo='foo')

            assert contributions.contributor_values_as_dicts(contributions.pre_fork_contributors_by_email) == {
                "jdoe@foo": {"names": ["Jessica"], "emails": ["jdoe@foo"]}
            }
            assert contributions.contributor_values_as_dicts(contributions.pre_fork_contributors_by_name) == {
                "Jessica": {"names": ["Jessica"], "emails": ["jdoe@foo"]}
            }

            assert contributions.contributor_values_as_dicts(contributions.contributors_by_email) == {
                "ssmith@foo": {"names": ["Sally"], "emails": ["ssmith@foo"]},
                "bill@someplace": {"names": ["William Simmons"], "emails": ["bill@someplace"]},
            }
            assert contributions.contributor_values_as_dicts(contributions.contributors_by_name) == {
                "Sally": {"names": ["Sally"], "emails": ["ssmith@foo"]},
                "William Simmons": {"names": ["William Simmons"], "emails": ["bill@someplace"]},
            }


def test_contributions_init_with_fork_and_no_cache():

    print()  # start on a fresh line

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove_abspath_getcwd_chdir():

        os.chdir(SAMPLE_FOO_HOME)

        mocked_foo_commits: List[Dict] = [
            {
                "hexsha": "ffff",
                "committed_datetime": "2020-01-05T12:34:56-05:00",
                "author": {"name": None, "email": "sal@foo"},
                "message": "something else"
            },
            {
                "hexsha": "eeee",
                "committed_datetime": "2020-01-05T12:34:56-05:00",
                "author": {"name": "Sal", "email": "ssmith@foo"},
                "message": "something else"
            },
            {
                "hexsha": "dddd",
                "committed_datetime": "2020-01-04T12:34:56-05:00",
                "author": {"name": "Sally Smith", "email": "ssmith@foo"},
                "message": "something else"
            },
            {
                "hexsha": "cccc",
                "committed_datetime": "2020-01-03T12:34:56-05:00",
                "author": {"name": "Sally Smith", "email": "sally.smith@foo"},
                "message": "something else"
            },
            {
                "hexsha": "bbbb",
                "committed_datetime": "2020-01-02T12:34:56-05:00",
                "author": {"name": "Sally", "email": "ssmith@foo"},
                "co_authors": [{"name": "William Simmons", "email": "bill@someplace"}],
                "message": "something else"
            },
        ]

        mocked_old_foo_commits: List[Dict] = [
            {
                "hexsha": "aaaa",
                "committed_datetime": "2020-01-01T01:23:45-05:00",
                "author": {"name": "Jessica", "email": "jdoe@foo"},
                "message": "something"
            },
        ]

        with git_context(mocked_commits={
            "old-foo": mocked_old_foo_commits,
            "foo": mocked_foo_commits + mocked_old_foo_commits,
        }):

            with printed_output() as printed:
                contributions = Contributions(repo='foo', exclude_fork='old-foo')
                assert printed.lines == [
                    'Created old-foo at 2020-01-01 01:23:45-05:00 by jdoe@foo',
                    'Forked old-foo as foo at 2020-01-02 12:34:56-05:00 by ssmith@foo',
                ]

            assert contributions.contributor_values_as_dicts(contributions.pre_fork_contributors_by_email) == {
                "jdoe@foo": {"names": ["Jessica"], "emails": ["jdoe@foo"]},
            }
            assert contributions.contributor_values_as_dicts(contributions.pre_fork_contributors_by_name) == {
                "Jessica": {"names": ["Jessica"], "emails": ["jdoe@foo"]},
            }

            assert contributions.contributor_values_as_dicts(contributions.contributors_by_email) == {
                "sal@foo": {"names": ["sal"], "emails": ["sal@foo"]},
                "sally.smith@foo": {"names": ["Sal", "Sally", "Sally Smith"],
                                    "emails": ["sally.smith@foo", "ssmith@foo"]},
                "ssmith@foo": {"names": ["Sal", "Sally", "Sally Smith"], "emails": ["sally.smith@foo", "ssmith@foo"]},
                "bill@someplace": {"names": ["William Simmons"], "emails": ["bill@someplace"]},
            }
            assert contributions.contributor_values_as_dicts(contributions.contributors_by_name) == {
                "sal": {"names": ["sal"], "emails": ["sal@foo"]},
                "Sally Smith": {"names": ["Sal", "Sally", "Sally Smith"], "emails": ["sally.smith@foo", "ssmith@foo"]},
                "William Simmons": {"names": ["William Simmons"], "emails": ["bill@someplace"]},
            }


@pytest.mark.parametrize('obsolete_key', ['excluded_fork', 'pre_fork_contributors_by_email', 'contributors_by_email'])
def test_contributions_init_with_cached_obsolete_keys(obsolete_key):
    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove_abspath_getcwd_chdir():
        os.chdir(SAMPLE_FOO_HOME)
        with io.open(os.path.abspath(Contributions.CONTRIBUTORS_CACHE_FILE), 'w') as fp:
            cache_data = {
                "forked_at": "2020-01-02T12:34:56-05:00",
                obsolete_key: None
            }
            json.dump(cache_data, fp=fp)
        with git_context(mocked_commits={"foo": []}):  # mocked_commits
            with pytest.raises(ValueError) as exc:
                Contributions(repo='foo')
            assert str(exc.value) == f'"{obsolete_key}" is no longer supported.'
