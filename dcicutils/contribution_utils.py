import datetime
import git
import io
import json
import os
import re
import warnings

from collections import defaultdict
from dcicutils.diff_utils import DiffManager
from dcicutils.lang_utils import n_of
from dcicutils.misc_utils import PRINT, ignored, environ_bool
from typing import Dict, List, Optional, Set, Type


DEBUG_CONTRIBUTIONS = environ_bool("DEBUG_CONTRIBUTIONS")

GITHUB_USER_REGEXP = re.compile('(?:[0-9]+[+])?(.*)[@]users[.]noreply[.]github[.]com')
PROJECT_HOME = os.environ.get('PROJECT_HOME', os.path.dirname(os.path.abspath(os.curdir)))


class GitAnalysis:

    @classmethod
    def find_repo(cls, repo_name: str) -> git.Repo:
        repo_path = os.path.join(PROJECT_HOME, repo_name)
        repo = git.Repo(repo_path)
        return repo

    @classmethod
    def git_commits(cls, repo_name) -> List[git.Commit]:
        repo = cls.find_repo(repo_name)
        commit: git.Commit
        for commit in repo.iter_commits():
            yield cls.json_for_commit(commit)

    @classmethod
    def author_name(cls, actor: git.Actor) -> str:
        return actor.name or actor.email.split('@')[0]

    @classmethod
    def json_for_actor(cls, actor: git.Actor) -> Dict:
        return {
            "name": cls.author_name(actor),
            "email": actor.email,
        }

    @classmethod
    def json_for_commit(cls, commit: git.Commit) -> Dict:
        return {
            'commit': commit.hexsha,
            'date': commit.committed_datetime.isoformat(),
            'author': cls.json_for_actor(commit.author),
            'coauthors': [cls.json_for_actor(co_author) for co_author in commit.co_authors],
            'message': commit.message,
        }


class Contributor:

    @classmethod
    def create(cls, *, author: git.Actor) -> 'Contributor':
        return Contributor(email=author.email, name=GitAnalysis.author_name(author))

    def __init__(self, *, email: Optional[str] = None, name: Optional[str] = None,
                 emails: Optional[Set[str]] = None, names: Optional[Set[str]] = None,
                 primary_name: Optional[str] = None):
        # Both email and name are required keyword arguments, though name is allowed to be None,
        # even though email is not. The primary_name is not required, and defaults to None, so will
        # be heuristically computed based on available names.
        if not email and not emails:
            raise ValueError("One of email= or emails= is required.")
        if email and emails:
            raise ValueError("Only one of email= and emails= may be provided.")
        if not emails:
            emails = {email}
        if name and names:
            raise ValueError("Only one of name= and names= may be provided.")
        if name and not names:
            names = {name}
        self.emails: Set[str] = emails
        self.names: Set[str] = names or set()
        self._primary_name = primary_name
        if primary_name:
            self.names.add(primary_name)

    def __str__(self):
        maybe_primary = f" {self._primary_name!r}" if self._primary_name else ""
        emails = ",".join(sorted(map(repr, self.emails), key=lambda x: x.lower()))
        names = ",".join(sorted(map(repr, self.names), key=lambda x: x.lower()))
        return f"<{self.__class__.__name__}{maybe_primary} emails={emails} names={names} {id(self)}>"

    def copy(self):
        return Contributor(emails=self.emails.copy(), names=self.names.copy(), primary_name=self._primary_name)

    @property
    def primary_name(self):
        if self._primary_name:
            return self._primary_name
        return sorted(self.names, key=self.name_variation_spelling_sort_key, reverse=True)[0]

    def set_primary_name(self, primary_name: str):
        self.names.add(primary_name)
        self._primary_name = primary_name

    def notice_mention_as(self, *, email: Optional[str] = None, name: Optional[str] = None):
        if email is not None and email not in self.emails:
            self.emails.add(email)
        if name is not None and name not in self.names:
            self.names.add(name)

    def as_dict(self):
        # Note that the sort is case-sensitive alphabetic just because that's easiest here.
        # Making it be case-insensitive would require a special case for non-strings,
        # and all we really care about is some easy degree of determinism for testing.
        data = {
            "emails": sorted(self.emails),
            "names": sorted(self.names),
        }
        return data

    @classmethod
    def from_dict(cls, data: Dict) -> 'Contributor':
        emails = data["emails"]
        names = data["names"]
        contributor = Contributor(email=emails[0], name=names[0])
        contributor.emails = set(emails)
        contributor.names = set(names)
        return contributor

    @classmethod
    def name_variation_spelling_sort_key(cls, name):
        return (
            ' ' in name,  # we prefer names like 'jane doe' over jdoe123
            len(name),  # longer names are usually more formal; william smith vs will smith
            name,  # this names by alphabetical order not because one is better, but to make sort results deterministic
        )


ContributorIndex = Optional[Dict[str, Contributor]]


class BasicContributions(GitAnalysis):

    VERBOSE = False

    def __init__(self, *, repo: Optional[str] = None,
                 verbose: Optional[bool] = None):
        self.email_timestamps: Dict[str, datetime.datetime] = {}
        self.name_timestamps: Dict[str, datetime.datetime] = {}
        if not repo:
            # Doing it this way gets around an ambiguity about '/foo/' vs '/foo' since both
            # os.path.join('/foo/', 'bar') and os.path.join('/foo', 'bar') yield '/foo/bar',
            # and from there one can do os.path.basename(os.path.dirname(...)) to get 'foo' out.
            cache_file = os.path.join(os.path.abspath(os.path.curdir), self.CONTRIBUTORS_CACHE_FILE)
            dir = os.path.dirname(cache_file)
            repo = os.path.basename(dir)
        self.repo: str = repo
        self.forked_at: Optional[datetime.datetime] = None
        self.contributors_by_name: Optional[ContributorIndex] = None
        self.contributors_by_email: Optional[ContributorIndex] = None
        self.pre_fork_contributors_by_email: Optional[ContributorIndex] = None
        self.pre_fork_contributors_by_name: Optional[ContributorIndex] = None
        self.loaded_contributor_data = None
        self.cache_discrepancies: Optional[dict] = None
        self.verbose = self.VERBOSE if verbose is None else verbose

    CONTRIBUTORS_CACHE_FILE = 'CONTRIBUTORS.json'

    def contributors_json_file(self):
        """
        Returns the name of the CONTRIBUTORS.json file for the repo associated with this class.
        """
        return os.path.join(PROJECT_HOME, self.repo, self.CONTRIBUTORS_CACHE_FILE)

    def existing_contributors_json_file(self):
        """
        Returns the name of the CONTRIBUTORS.json file for the repo associated with this class if that file exists,
        or None if there is no such file.
        """
        file = self.contributors_json_file()
        if os.path.exists(file):
            return file
        else:
            return None

    @classmethod
    def notice_reference_time(cls, key: str, timestamp: datetime.datetime, timestamps: Dict[str, datetime.datetime]):
        reference_timestamp: datetime.datetime = timestamps.get(key)
        if not reference_timestamp:
            timestamps[key] = timestamp
        elif timestamp > reference_timestamp:
            timestamps[key] = timestamp

    def email_reference_time(self, email):
        return self.email_timestamps.get(email)

    def name_reference_time(self, name):
        return self.name_timestamps.get(name)

    @classmethod
    def contributor_values_as_dicts(cls, contributor_index: Optional[ContributorIndex]):
        if contributor_index is None:
            return None
        else:
            return {
                key: contributor.as_dict()
                for key, contributor in contributor_index.items()
            }

    @classmethod
    def contributor_values_as_objects(cls, contributor_index: Optional[Dict]) -> Optional[ContributorIndex]:
        if contributor_index is None:
            return None
        else:
            return {
                key: Contributor.from_dict(value)
                for key, value in contributor_index.items()
            }

    def checkpoint_state(self):
        return self.as_dict()

    def as_dict(self):

        data = {}

        forked_at = self.forked_at.isoformat() if self.forked_at else None
        if forked_at is not None:
            data["forked_at"] = forked_at

        pre_fork_contributors_by_name = self.contributor_values_as_dicts(self.pre_fork_contributors_by_name)
        if pre_fork_contributors_by_name is not None:
            data["pre_fork_contributors_by_name"] = pre_fork_contributors_by_name

        contributors_by_name = self.contributor_values_as_dicts(self.contributors_by_name)
        if contributors_by_name is not None:
            data["contributors_by_name"] = contributors_by_name

        return data

    def save_contributor_data(self, filename: Optional[str] = None) -> str:
        if filename is None:
            filename = self.contributors_json_file()
        with io.open(filename, 'w') as fp:
            PRINT(json.dumps(self.as_dict(), indent=2), file=fp)
        return filename

    def repo_contributor_names(self, with_email=False):
        for name, contributor in self.contributors_by_name.items():
            if with_email:
                yield f"{name} ({', '.join([self.pretty_email(email) for email in contributor.emails])})"
            else:
                yield name

    def show_repo_contributors(self, analyze_discrepancies: bool = True, with_email: bool = True,
                               error_class: Optional[Type[BaseException]] = None):
        for author_name in self.repo_contributor_names(with_email=with_email):
            PRINT(author_name)
        if analyze_discrepancies:
            file = self.existing_contributors_json_file()
            if not file:
                message = f"Need to create a {self.CONTRIBUTORS_CACHE_FILE} file for {self.repo}."
                if error_class:
                    raise error_class(message)
                else:
                    PRINT(message)
            elif self.cache_discrepancies:
                message = "There are contributor cache discrepancies."
                PRINT(f"===== {message.rstrip('.').upper()} =====")
                for action, items in self.cache_discrepancies.items():
                    action: str
                    PRINT(f"{action.replace('_', ' ').title()}:")
                    for item in items:
                        PRINT(f" * {item}")
                if error_class:
                    raise error_class(message)

    @classmethod
    def pretty_email(cls, email):
        m = GITHUB_USER_REGEXP.match(email)
        if m:
            user_name = m.group(1)
            return f"{user_name}@github"
        else:
            return email

    @classmethod
    def get_contributors_json_from_file_cache(cls, filename):
        try:
            with io.open(filename, 'r') as fp:
                data = json.load(fp)
        except Exception:
            PRINT(f"Error while reading data from {filename!r}.")
            raise
        return data

    @classmethod
    def contributor_index_by_primary_name(cls, contributors_by_name: ContributorIndex) -> ContributorIndex:
        """
        Given a by-name contributor index:

        * Makes sure that all contributors have only one name, indexed by the contributor's primary name
        * Sorts the resulting index using a case-insensitive alphabetic sort

        and then returns the result.

        :param contributors_by_name: a contributor index indexed by human name
        :return: a contributor index
        """
        seen = set()
        nicknames_seen = set()
        contributor_items = []
        contributors = {}
        for name, contributor in contributors_by_name.items():
            if contributor not in seen:
                for nickname in contributor.names:
                    if nickname in nicknames_seen:
                        raise Exception(f"Name improperly shared between {contributor}"
                                        f" and {contributors_by_name[nickname]}")
                    nicknames_seen.add(nickname)
                contributor_items.append((contributor.primary_name, contributor))
                seen.add(contributor)
        for name, contributor in sorted(contributor_items,
                                        # Having selected the longest names, now sort names ignoring case
                                        key=lambda pair: pair[0].lower()):
            contributors[name] = contributor
        return contributors

    @classmethod
    def by_email_from_by_name(cls, contributors_by_name_json):
        result = {}
        seen = set()
        for name_key, entry in contributors_by_name_json.items():
            ignored(name_key)
            seen_key = id(entry)
            if seen_key in seen:
                continue
            seen.add(seen_key)
            for email in entry.get("emails", []) if isinstance(entry, dict) else entry.emails:
                if result.get(email):
                    raise Exception(f"email address {email} is used more than once.")
                result[email] = entry
        return result

    @classmethod
    def set_keys_as_primary_names(cls, contributors_by_name: Optional[ContributorIndex]):
        if contributors_by_name is not None:
            for key, contributor in contributors_by_name.items():
                contributor.set_primary_name(key)


class Contributions(BasicContributions):

    def __init__(self, *, repo: Optional[str] = None,
                 exclude_fork: Optional[str] = None,
                 verbose: Optional[bool] = None):
        super().__init__(repo=repo, verbose=verbose)
        existing_contributor_data_file = self.existing_contributors_json_file()
        if existing_contributor_data_file:
            # This will set .loaded_contributor_data and other values from CONTRIBUTORS.json
            self.load_contributors_from_json_file_cache(existing_contributor_data_file)

        checkpoint1 = self.checkpoint_state()
        self.reconcile_contributors_with_github_log(exclude_fork=exclude_fork)
        checkpoint2 = self.checkpoint_state()

        def list_to_dict_normalizer(*, label, item):
            ignored(label)
            if isinstance(item, list):
                return {elem: elem for elem in item}
            else:
                return item

        if existing_contributor_data_file:
            diff_manager = DiffManager(label="contributors")
            contributors1 = checkpoint1['contributors_by_name']
            contributors2 = checkpoint2['contributors_by_name']
            diffs = diff_manager.diffs(contributors1, contributors2, normalizer=list_to_dict_normalizer)
            self.cache_discrepancies = self.resummarize_discrepancies(diffs)

    @classmethod
    def resummarize_discrepancies(cls, diffs: Dict) -> Dict:
        """
        Reformats the dictionary result from DiffManager.diffs in a way that's more appropriate to our situation.
        In particular:

        * the "added" key is renamed to "to add"
        * the "changed" key is renamed to "to_change"
        * the "removed" key is renamed to "to_remove"

        This tense change is necessary because the labels are cues to the user about actions that need to be done
        in the future, not actions already done.
        """
        added = diffs.get('added')
        changed = diffs.get('changed')
        removed = diffs.get('removed')
        cache_discrepancies = {}
        if added:
            cache_discrepancies['to_add'] = added
        if changed:
            cache_discrepancies['to_change'] = changed
        if removed:
            cache_discrepancies['to_remove'] = removed
        return cache_discrepancies

    def load_contributors_from_json_file_cache(self, filename):
        self.loaded_contributor_data = data = self.get_contributors_json_from_file_cache(filename)
        self.load_from_dict(data)

        if DEBUG_CONTRIBUTIONS:  # pragma: no cover - debugging only
            PRINT("After load_contributors_from_json_file_cache...")
            PRINT(f"{n_of(self.pre_fork_contributors_by_name, 'pre-fork contributor by name')}")
            PRINT(f"{n_of(self.pre_fork_contributors_by_email, 'pre-fork contributor by email')}")
            PRINT(f"{n_of(self.contributors_by_name, 'contributor by name')}")
            PRINT(f"{n_of(self.contributors_by_email, 'contributor by email')}")

    def load_from_dict(self, data: Dict):
        forked_at: Optional[str] = data.get('forked_at')
        self.forked_at: Optional[datetime.datetime] = (None
                                                       if forked_at is None
                                                       else datetime.datetime.fromisoformat(forked_at))

        if 'excluded_fork' in data:
            # We originally implemented this, but it isn't needed and supporting it leads to problems. -kmp 30-Jul-2023
            raise ValueError('"excluded_fork" is no longer supported.')

        pre_fork_contributors_by_name_json = data.get('pre_fork_contributors_by_name') or {}
        pre_fork_contributors_by_name = self.contributor_values_as_objects(pre_fork_contributors_by_name_json)
        self.set_keys_as_primary_names(pre_fork_contributors_by_name)
        self.pre_fork_contributors_by_name = pre_fork_contributors_by_name

        if 'pre_fork_contributors_by_email' in data:
            # We originally implemented this, but supporting it is unnecessarily complex
            # because of redundancies of implementation and the possibility of ambiguities if both
            # markers are present. -kmp 30-Jul-2023
            raise ValueError('"pre_fork_contributors_by_email" is no longer supported.')
        pre_fork_contributors_by_email = self.by_email_from_by_name(pre_fork_contributors_by_name)
        self.pre_fork_contributors_by_email = pre_fork_contributors_by_email

        contributors_by_name_json = data.get('contributors_by_name') or {}
        contributors_by_name = self.contributor_values_as_objects(contributors_by_name_json)
        self.set_keys_as_primary_names(contributors_by_name)
        self.contributors_by_name = contributors_by_name

        if 'contributors_by_email' in data:
            # We originally implemented this, but supporting it is unnecessarily complex
            # because of redundancies of implementation and the possibility of ambiguities if both
            # markers are present. -kmp 30-Jul-2023
            raise ValueError('"contributors_by_email" is no longer supported.')
        contributors_by_email = self.by_email_from_by_name(contributors_by_name)
        self.contributors_by_email = contributors_by_email

    def reconcile_contributors_with_github_log(self, exclude_fork=None):
        """
        Rummages the GitHub log entries for contributors we don't know about.
        That data is merged against our existing structures.
        """
        if DEBUG_CONTRIBUTIONS:  # pragma: no cover - debugging only
            PRINT("Reconciling with git log.")

        if self.loaded_contributor_data:
            pre_fork_contributor_emails = set(self.pre_fork_contributors_by_email.keys())
        elif exclude_fork:
            excluded_contributions = Contributions(repo=exclude_fork)
            self.pre_fork_contributors_by_email = excluded_contributions.contributors_by_email
            self.pre_fork_contributors_by_name = excluded_contributions.contributors_by_name
            pre_fork_contributor_emails = set(self.pre_fork_contributors_by_email.keys())
        else:
            pre_fork_contributor_emails = set()

        post_fork_contributors_seen = defaultdict(lambda: [])  # pragma: no cover - for debugging only

        contributors_by_email: ContributorIndex = self.contributors_by_email or {}
        git_repo = self.find_repo(repo_name=self.repo)

        n = 0

        def notice_author(*, author: git.Actor, date: datetime.datetime):
            if self.forked_at:
                if date < self.forked_at:
                    return
                    # raise Exception("Commits are out of order.")
            elif author.email not in pre_fork_contributor_emails:
                action = "Created" if n == 1 else (f"Forked {exclude_fork} as" if exclude_fork else "Forked")
                PRINT(f"{action} {self.repo} at {date} by {author.email}")
                self.forked_at = date

            if self.forked_at and date >= self.forked_at:
                if DEBUG_CONTRIBUTIONS:  # pragma: no cover - debugging only
                    if author.email in pre_fork_contributor_emails:  # pragma: no cover - this is for debugging
                        # PRINT(f"Post-fork contribution from {author.email} ({date})")
                        post_fork_contributors_seen[author.email].append(date)
                self.notice_reference_time(key=author.email, timestamp=date, timestamps=self.email_timestamps)
                self.notice_reference_time(key=GitAnalysis.author_name(author), timestamp=date,
                                           timestamps=self.name_timestamps)

                contributor_by_email = contributors_by_email.get(author.email)
                if contributor_by_email:  # already exists, so update it
                    contributor_by_email.notice_mention_as(email=author.email, name=GitAnalysis.author_name(author))
                else:  # need to create it new
                    contributor_by_email = Contributor.create(author=author)
                    contributors_by_email[author.email] = contributor_by_email
            else:
                # print("skipped")
                pass

        for commit in reversed(list(git_repo.iter_commits())):
            n += 1
            commit_date = commit.committed_datetime
            notice_author(author=commit.author, date=commit_date)
            for co_author in commit.co_authors:
                notice_author(author=co_author, date=commit_date)
        if DEBUG_CONTRIBUTIONS:  # pragma: no cover - debugging only
            PRINT(f"{n_of(n, 'commit')} processed.")
            for email, dates in post_fork_contributors_seen.items():
                when = str(dates[0].date())
                if len(dates) > 1:
                    when += f" to {dates[-1].date()}"
                PRINT(f"{n_of(dates, 'post-fork commit')} seen for {email} ({when}).")

        contributors_by_name: ContributorIndex = {}

        for contributor_by_email in contributors_by_email.values():
            self.traverse(root=contributor_by_email,
                          cursor=contributor_by_email,
                          contributors_by_email=contributors_by_email,
                          contributors_by_name=contributors_by_name)
            for name in list(contributor_by_email.names):
                contributors_by_name[name] = contributor_by_email
            for email in list(contributor_by_email.emails):
                contributors_by_email[email] = contributor_by_email

        self.contributors_by_name = self.contributor_index_by_primary_name(contributors_by_name)
        self.contributors_by_email = self.by_email_from_by_name(self.contributors_by_name)  # contributors_by_email

        if DEBUG_CONTRIBUTIONS:  # pragma: no cover - debugging only
            PRINT("After reconcile_contributors_with_github_log...")
            PRINT(f"{n_of(self.pre_fork_contributors_by_name, 'pre-fork contributor by name')}")
            PRINT(f"{n_of(self.pre_fork_contributors_by_email, 'pre-fork contributor by email')}")
            PRINT(f"{n_of(self.contributors_by_name, 'contributor by name')}")
            PRINT(f"{n_of(self.contributors_by_email, 'contributor by email')}")

    @classmethod
    def traverse(cls,
                 root: Contributor,
                 cursor: Optional[Contributor],
                 contributors_by_email: ContributorIndex,
                 contributors_by_name: ContributorIndex,
                 seen: Optional[Set[Contributor]] = None):
        if seen is None:
            seen = set()
        if cursor in seen:  # It's slightly possible that a person has a name of None that slipped in. Ignore that.
            return
        seen.add(cursor)
        for name in list(cursor.names):
            root.names.add(name)
        for email in list(cursor.emails):
            root.emails.add(email)
        for name in list(cursor.names):
            contributor = contributors_by_name.get(name)
            if contributor and contributor not in seen:
                cls.traverse(root=root, cursor=contributor, contributors_by_email=contributors_by_email,
                             contributors_by_name=contributors_by_name, seen=seen)
        for email in list(cursor.emails):
            contributor = contributors_by_email.get(email)
            if contributor and contributor not in seen:  # pragma: no cover - shouldn't happen, included 'just in case'
                warnings.warn(f"Unexpected stray email seen: {email}")
                cls.traverse(root=root, cursor=contributor, contributors_by_email=contributors_by_email,
                             contributors_by_name=contributors_by_name, seen=seen)
