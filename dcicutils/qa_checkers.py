import glob
import io
import os
import re
import toml
import warnings

from collections import defaultdict
from typing import Optional, List, Dict, Type, Literal
from .contribution_utils import Contributions
from .lang_utils import conjoined_list, n_of, there_are
from .misc_utils import PRINT, remove_prefix, remove_suffix, getattr_customized, CustomizableProperty


QA_EXCEPTION_PATTERN = re.compile(r"[#].*\b[N][O][Q][A]\b", re.IGNORECASE)


class StaticChecker:

    ROOT_DIR = None

    # I wanted to use pytest.PytestConfigWarning, but that creates a dependency
    # on particular versions of pytest, and we don't export a delivery
    # constraint of a particular pytest version. So RuntimeWarning is a
    # safer setting for now. -kmp 14-Jan-2021

    WARNING_CATEGORY: Type[Warning] = SyntaxWarning

    def __init__(self, root_dir: Optional[str] = None):
        super().__init__()
        self.root_dir: str = os.path.abspath(root_dir or self.ROOT_DIR or os.curdir)


class ChangeLogChecker(StaticChecker):

    """
    Given appropriate customizations, this allows cross-checking of pyproject.toml and a changelog for consistency.

    By default, it will raise an error if the CHANGELOG is specified and is not consistent with the version
    (unless that version is a beta).

    If the class variable RAISE_ERROR_IF_CHANGELOG_MISMATCH is set to False, as is the case in
    subclass VersionChecker, only a warning (of a kind given by cls.WARNING_CATEGORY) will be generated,
    not an error, so use that subclass if you don't want hard errors for version inconsistency.

    You must subclass this class, specifying both the pyproject filename and the changelog filename as
    class variables PYPROJECT and CHANGELOG, respectively.

    def test_version():

        class MyAppChangeLogChecker(ChangeLogChecker):
            PYPROJECT = os.path.join(ROOT_DIR, "pyproject.toml")
            CHANGELOG = os.path.join(ROOT_DIR, "CHANGELOG.rst")

        MyAppChangeLogChecker.check_version()

    """

    PYPROJECT = CustomizableProperty('PYPROJECT', description="The repository-relative name of the pyproject file.")
    CHANGELOG = CustomizableProperty('CHANGELOG', description="The repository-relative name of the change log.")

    @classmethod
    def check_version(cls):
        version = cls._check_version()
        if getattr_customized(cls, "CHANGELOG"):
            cls._check_change_history(version)

    @classmethod
    def _check_version(cls):

        __tracebackhide__ = True

        pyproject_file = getattr_customized(cls, 'PYPROJECT')
        assert os.path.exists(pyproject_file), "Missing pyproject file: %s" % pyproject_file
        pyproject = toml.load(pyproject_file)
        version = pyproject.get('tool', {}).get('poetry', {}).get('version', None)
        assert version, "Missing version in %s." % pyproject_file
        PRINT("Version = %s" % version)
        return version

    RAISE_ERROR_IF_CHANGELOG_MISMATCH = True

    VERSION_LINE_PATTERN = re.compile("^[#* ]*([0-9]+[.][^ \t\n]*)([ \t\n].*)?$")
    VERSION_IS_BETA_PATTERN = re.compile("^.*[0-9]([AaBb][0-9]+|[-][A-Za-z0-9-_.]*)$")

    @classmethod
    def _check_change_history(cls, version=None):

        __tracebackhide__ = True

        if version and cls.VERSION_IS_BETA_PATTERN.match(version):
            # Don't require beta versions to match up in change log.
            # We don't just strip the version and look at that because sometimes we use other numbers on betas.
            # Better to just not do it at all.
            return

        changelog_file = getattr_customized(cls, "CHANGELOG")

        if not changelog_file:
            if version:
                raise AssertionError("Cannot check version without declaring a CHANGELOG file.")
            return

        assert os.path.exists(changelog_file), "Missing changelog file: %s" % changelog_file

        with io.open(changelog_file) as fp:
            versions = []
            for line in fp:
                m = cls.VERSION_LINE_PATTERN.match(line)
                if m:
                    versions.append(m.group(1))

        assert versions, "No version info was parsed from %s" % changelog_file

        # Might be sorted top to bottom or bottom to top, but ultimately the current version should be first or last.
        if versions[0] != version and versions[-1] != version:
            message = "Missing entry for version %s in %s." % (version, changelog_file)
            if cls.RAISE_ERROR_IF_CHANGELOG_MISMATCH:
                raise AssertionError(message)
            else:
                warnings.warn(message, category=cls.WARNING_CATEGORY, stacklevel=3)
            return


class VersionChecker(ChangeLogChecker):

    """
    Given appropriate customizations, this allows cross-checking of pyproject.toml and a changelog for consistency.

    By default, a warning (of a kind given by cls.WARNING_CATEGORY) will be generated, not an error, if the change
    log is not consistent. If you want a hard error, use the superclass ChangeLogChecker.

    You must subclass this class, specifying both the pyproject filename and the changelog filename as
    class variables PYPROJECT and CHANGELOG, respectively.

    def test_version():

        class MyAppVersionChecker(VersionChecker):
            PYPROJECT = os.path.join(ROOT_DIR, "pyproject.toml")
            CHANGELOG = os.path.join(ROOT_DIR, "CHANGELOG.rst")

        MyAppVersionChecker.check_version()

    """

    RAISE_ERROR_IF_CHANGELOG_MISMATCH = False


class StaticSourcesChecker(StaticChecker):

    def __init__(self, sources_subdir: str, root_dir: Optional[str] = None):
        super().__init__(root_dir=root_dir)
        self.sources_dir: str = os.path.abspath(os.path.join(self.root_dir, sources_subdir))

    @classmethod
    def compute_sources_files(cls, *, where, recursive, base_only: Optional[bool] = None) -> List[str]:
        """
        This computes the set of source files referred to.
        By default, it returns all python files in the sources directory and any of its subdirectories,
        except those that are not acceptable to .is_allowed_submodule_file().

        This method can be customized in subclasses if someone disagrees with this selection.
        """
        base_only = not recursive if base_only is None else base_only
        # This is all the files in the backbone of the sources subdir, but not recursively,
        # which we'll use to get a list of actual submodules we might want to autodoc.
        all_files_raw = glob.glob(os.path.join(where, "**/*.py" if recursive else "*.py"), recursive=recursive)
        all_files = [os.path.basename(file) if base_only else file
                     for file in all_files_raw]
        return all_files


class DocsChecker(StaticSourcesChecker):

    SKIP_SUBMODULES: List[str] = []

    DOCS_SUBDIR = "docs/source"

    def __init__(self, *, sources_subdir, docs_index_file,
                 root_dir: Optional[str] = None, docs_subdir: Optional[str] = None,
                 module_name: Optional[str] = None, skip_submodules: List[str] = None, recursive: bool = True,
                 show_detail: bool = True):
        """
        A DocChecker is potentially capable of various kinds of checks of a repository's documentation.

        :param sources_subdir: In most cases, e.g., for a library, code will live in a subdir that names the library,
            such as 'dcicutils' for this (4dn-dcic/utils) repository.
            For CGAP and Fourfront, sources in a more obscure place, 'src/encoded'.
        :param docs_index_file: The name of the documentation file in which module reference documentation would be.
            This is not necessarily the root of the documentation, which may contain other introductory material.
            Typically this will have a name like 'index.rst' but for this library (dcicutils), the name `dcicutils.rst'
            is used. Our repositories vary a lot, so no point in guessing. Just specify it.
        :param root_dir: The name of the folder that is the root of this repository (e.g., where the pyproject.toml
            file or CHANGELOG.rst files would be, but also the place relative to which the various subdirs are given).
            By default, as the system loads, it's assumed you're in the root directory so the default is
            os.path.abspath(os.curdir).
        :param docs_subdir: The place to find documentation. By default we assume 'docs/source'.
        :param module_name: The top-level module name (usually a library or an application name).
            If not specified, a default will be taken from the last component of the sources_subdir.
        :param skip_submodules: The name of modules that are not expected to be documented. These might be modules
            containing deprecated functionality or other implementation substrate not intended to be documented.
        :param recursive: Whether to consider source files beyond the top-level of the sources_subdir. If False,
            only the immediate contents of sources_subdir are considered. Otherwise, all of its contents recursively
            are considered, subject to some filtering that might be done by some operations.
        """

        super().__init__(sources_subdir=sources_subdir, root_dir=root_dir)
        self.module_name = module_name or sources_subdir.split('.')[-1]
        self.docs_dir = os.path.join(self.root_dir, docs_subdir or self.DOCS_SUBDIR)
        self.docs_index_file = os.path.join(self.docs_dir, docs_index_file)
        self.skip_submodules = set(skip_submodules or self.SKIP_SUBMODULES)
        self.sources_files = self.compute_sources_files(where=sources_subdir, recursive=recursive)
        self.show_detail = show_detail

    _UNWANTED_SUBMODULE_FILTER = re.compile("(tests?/|/test_|^test_|/[^a-z]|^[^a-z/])", re.IGNORECASE)

    @classmethod
    def is_allowed_submodule_file(cls, submodule_file_candidate):
        """
        Returns true if the given submodule filename looks like a module that should be docuemnted.
        Modules that are not documented are test files or files in test folders, as well as __init__.py
        or any hidden files.

        This method can be customized in subclasses if someone disagrees with this selection.
        """
        return not cls._UNWANTED_SUBMODULE_FILTER.search(submodule_file_candidate)

    @classmethod
    def compute_sources_files(cls, *, where, recursive, base_only: Optional[bool] = None):
        """
        This computes the set of source files referred to.
        By default, it returns all python files in the sources directory and any of its subdirectories,
        except those that are not acceptable to .is_allowed_submodule_file().

        This method can be customized in subclasses if someone disagrees with this selection.
        """
        return [file
                for file in super().compute_sources_files(where=where, recursive=recursive, base_only=base_only)
                if cls.is_allowed_submodule_file(file)]

    @classmethod
    def as_module_name(cls, file, relative_to_prefix=''):
        """
        Converts a filename to a module name, relative to a given prefix.
        >>> DocsChecker.as_module_name('/foo/bar/baz/alpha.py', relative_to_prefix="/foo/bar")
        baz.alpha
        """
        if ':' in file:
            # Presumably this could occur on Windows. On MacOS & Linux, not a big concern.
            raise ValueError(f"Don't know how to convert {file} to a module name.")
        return remove_suffix(".py", remove_prefix(relative_to_prefix, file).lstrip('/').replace('/', '.'))

    def expected_modules(self) -> set:
        all_modules = {
            self.as_module_name(file, relative_to_prefix=self.sources_dir)
            for file in self.sources_files
            if file.endswith(".py")
        }
        return all_modules - self.skip_submodules

    _SECTION_OR_SUBSECTION_LINE = re.compile(r"^([=]+|[-]+)$")

    _SUBSUBSECTION_LINE = re.compile(r"^[\^]+$")

    _AUTOMODULE_LINE = re.compile(f"^[.][.][ ]+automodule::[ ]+[A-Za-z][A-Za-z0-9_]*[.](.*)$")

    def check_documentation(self):

        __tracebackhide__ = True

        with io.open(self.docs_index_file) as fp:

            line_number = 0
            current_module = None
            automodules_seen_in_current_section = 0
            prev_line = None
            problems = []
            expected_modules = self.expected_modules()
            documented_modules = set()
            for line in fp:
                line_number += 1  # We count the first line as line 1
                line = line.strip()
                if self._SUBSUBSECTION_LINE.match(line):
                    if current_module and automodules_seen_in_current_section == 0:
                        problems.append(f"Line {line_number}:"
                                        f" Missing automodule declaration for section {current_module}.")
                    current_module = prev_line
                    automodules_seen_in_current_section = 0
                elif self._SECTION_OR_SUBSECTION_LINE.match(line):
                    current_module = None
                    automodules_seen_in_current_section = 0
                else:
                    matched = self._AUTOMODULE_LINE.match(line)
                    if matched:
                        automodule_module = matched.group(1)
                        if not current_module:
                            problems.append(f"Line {line_number}: Unexpected automodule declaration"
                                            f" outside of module section.")
                        else:
                            documented_modules.add(automodule_module)
                            if automodules_seen_in_current_section == 1:
                                # If fewer than 1 seen, no issue.
                                # If more than 1 seen, we already warned, so don't duplicate.
                                # So really only the n == 1 case matters to us.
                                problems.append(f"Line {line_number}: More than one automodule"
                                                f" in section {current_module}?")
                            if automodule_module != current_module:
                                problems.append(f"Line {line_number}: Unexpected automodule declaration"
                                                f" for section {current_module}: {automodule_module}.")
                        automodules_seen_in_current_section += 1
                prev_line = line
            undocumented_modules = expected_modules - documented_modules
            if undocumented_modules:
                problems.append(there_are(sorted(undocumented_modules), kind="undocumented module", punctuate=True,
                                          context=f"and {len(documented_modules) or 'none'} documented"))
            if problems:
                for n, problem in enumerate(problems, start=1):
                    PRINT(f"PROBLEM {n}: {problem}")
                message = there_are(problems, kind="problem", tense='past', show=self.show_detail,
                                    context=f"found in the readthedocs declaration file, {self.docs_index_file!r}")
                raise AssertionError(message)


class DebuggingArtifactChecker(StaticSourcesChecker):

    DEBUGGING_PATTERNS = [
        {
            'key': 'print',
            'summary': "call to print",
            'pattern': r"^[^#]*\bprint[(]"
        },
        {
            'key': 'pdb',
            'summary': "active use of pdb.set_trace",
            'pattern': r"^[^#]*pdb[.]set_trace[(][)]"
        },
    ]

    def __init__(self, sources_subdir: str, *,
                 root_dir: Optional[str] = None,
                 debugging_patterns: Optional[List[Dict[Literal['summary', 'key', 'pattern'], str]]] = None,
                 skip_files: Optional[str] = None,
                 filter_patterns: Optional[List[str]] = None,
                 recursive: bool = True,
                 if_used: Literal['error', 'warning'] = 'error'):
        super().__init__(sources_subdir=sources_subdir, root_dir=root_dir)
        self._debugging_patterns: List[Dict[str, str]] = (
            self.DEBUGGING_PATTERNS if debugging_patterns is None else debugging_patterns)
        self.skip_files: Optional[str] = skip_files
        self.recursive: bool = recursive
        self.sources_pattern: str = os.path.join(self.sources_dir, '**/*.py' if recursive else '*.py')
        self.filter_patterns: Optional[List[str]] = filter_patterns
        self.if_used = if_used

    @property
    def debugging_patterns(self) -> Dict[str, str]:
        return {
            entry['summary']: entry['pattern']
            for entry in self._debugging_patterns
            if self.filter_patterns is None or entry['key'] in self.filter_patterns
        }

    def check_for_debugging_patterns(self):
        __tracebackhide__ = True
        try:
            confirm_no_uses(where=self.sources_pattern,
                            patterns=self.debugging_patterns,
                            skip_files=self.skip_files,
                            recursive=self.recursive)
        except AssertionError as e:
            if self.if_used == 'warning':
                warnings.warn(str(e), category=self.WARNING_CATEGORY, stacklevel=2)
            else:
                raise


def find_uses(*, where: str, patterns: Dict[str, str],
              skip_files: Optional[str] = None, recursive: bool = True):
    """
    In the files specified by where (a glob pattern), finds uses of pattern (a regular expression).

    :param where: a glob pattern
    :param patterns: a dictionary mapping problem summaries to regular expressions
    :param skip_files: a regular expression which if found by search in a filename causes the filename to be skipped
    :param recursive: whether to treat the 'where' pattern as recursive.
        So, for where='foo', recursive=False means 'foo/*.py' and recursive=True means 'foo/**/*.py'.
    """

    checks = []
    for summary, pattern in patterns.items():
        checks.append((re.compile(pattern), summary))
    uses = defaultdict(lambda: [])
    files = glob.glob(where, recursive=recursive)
    for file in files:
        if skip_files and re.search(skip_files, file):
            continue
        with io.open(file, 'r') as fp:
            line_number = 0
            for line in fp:
                line_number += 1
                for matcher, summary in checks:
                    if matcher.search(line):
                        problem_ignorable = QA_EXCEPTION_PATTERN.search(line)
                        if not problem_ignorable:
                            uses[file].append({"line_number": line_number, "line": line.rstrip('\n'),
                                               "summary": summary})
    return uses


def confirm_no_uses(*, where: str, patterns: Dict[str, str],
                    skip_files: Optional[str] = None, recursive: bool = True):
    """
    In the files specified by where (a glob pattern), finds uses of pattern (a regular expression).

    :param where: a glob pattern
    :param patterns: dictionary mapping summaries to regular expressions
    :param skip_files: a regular expression which if found by search in a filename causes the filename to be skipped
    :param recursive: whether to treat the 'where' pattern as recursive.
        So, for where='foo', recursive=False means 'foo/*.py' and recursive=True means 'foo/**/*.py'.
    """

    __tracebackhide__ = True

    def summarize(problems):
        categories = defaultdict(lambda: 0)
        for problem in problems:
            categories[problem['summary']] += 1
        return conjoined_list([n_of(count, category) for category, count in categories.items()])

    uses = find_uses(patterns=patterns, where=where, skip_files=skip_files, recursive=recursive)
    if uses:
        detail = ""
        n = 0
        for file, matches in uses.items():
            n += len(matches)
            detail += f"\n In {file}, {summarize(matches)}."
        message = f"{n_of(n, 'problem')} detected:" + detail
        raise AssertionError(message)


class ContributionsChecker:

    @classmethod
    def validate(cls):
        contributions = Contributions()  # no repo specified, so use current directory
        contributions.show_repo_contributors(error_class=AssertionError)
