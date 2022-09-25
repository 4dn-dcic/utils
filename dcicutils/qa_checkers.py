import glob
import io
import os
import re
import toml
import warnings

from collections import defaultdict
from typing import Optional, List
from .lang_utils import conjoined_list, n_of, there_are
from .misc_utils import PRINT, remove_prefix, remove_suffix, getattr_customized, CustomizableProperty


QA_EXCEPTION_PATTERN = re.compile(r"[#].*\b[N][O][Q][A]\b", re.IGNORECASE)


def find_uses(*, where, patterns, recursive=False):
    """
    In the files specified by where (a glob pattern), finds uses of pattern (a regular expression).

    :param where: a glob pattern
    :param patterns: a dictionary mapping problem summaries to regular expressions
    :param recursive: a boolean saying whether to look recursively [UNIMPLEMENTED]
    """

    assert not recursive, "The recursive option to find_uses is not yet implemented."

    checks = []
    for summary, pattern in patterns.items():
        checks.append((re.compile(pattern), summary))
    uses = defaultdict(lambda: [])
    files = glob.glob(where)
    for file in files:
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


def confirm_no_uses(*, where, patterns):
    """
    In the files specified by where (a glob pattern), finds uses of pattern (a regular expression).

    :param where: a glob pattern
    :param patterns: dicionary mapping summaries to regular expressions
    """

    __tracebackhide__ = True

    def summarize(problems):
        categories = defaultdict(lambda: 0)
        for problem in problems:
            categories[problem['summary']] += 1
        return conjoined_list([n_of(count, category) for category, count in categories.items()])

    uses = find_uses(patterns=patterns, where=where)
    if uses:
        detail = ""
        n = 0
        for file, matches in uses.items():
            n += len(matches)
            detail += f"\n In {file}, {summarize(matches)}."
        message = f"{n_of(n, 'problem')} detected:" + detail
        raise AssertionError(message)


class ChangeLogChecker:

    """
    Given appropriate customizations, this allows cross-checking of pyproject.toml and a changelog for consistency.

    By default, it will raise an error if the CHANGELOG is specified and is not consistent with the version
    (unless that version is a beta).

    If the class variable RAISE_ERROR_IF_CHANGELOG_MISMATCH is set to False, as is the case in
    subclass VersionChecker, only a warning (of a kind given by WARNING_CATEGORY) will be generated,
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

    # I wanted to use pytest.PytestConfigWarning, but that creates a dependency
    # on particular versions of pytest, and we don't export a delivery
    # constraint of a particular pytest version. So RuntimeWarning is a
    # safer setting for now. -kmp 14-Jan-2021
    WARNING_CATEGORY = RuntimeWarning

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
    VERSION_IS_BETA_PATTERN = re.compile("^.*[0-9][Bb][0-9]+$")

    @classmethod
    def _check_change_history(cls, version=None):

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
                warnings.warn(message, category=cls.WARNING_CATEGORY, stacklevel=2)
            return


class VersionChecker(ChangeLogChecker):

    """
    Given appropriate customizations, this allows cross-checking of pyproject.toml and a changelog for consistency.

    By default, a warning (of a kind given by WARNING_CATEGORY) will be generated, not an error, if the change
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


class StaticChecker:

    ROOT_DIR = None

    def __init__(self, sources_subdir, root_dir=None):
        self.root_dir = os.path.abspath(root_dir or self.ROOT_DIR or os.curdir)
        self.sources_dir = os.path.abspath(os.path.join(self.root_dir, sources_subdir))
        super().__init__()


class DocsChecker(StaticChecker):

    SKIP_SUBMODULES: List[str] = []

    DOCS_SUBDIR = "docs/source"

    def __init__(self, *, sources_subdir, docs_index_file,
                 root_dir: Optional[str] = None, docs_subdir: Optional[str] = None,
                 module_name: Optional[str] = None, skip_submodules: List[str] = None, recursive: bool = False):
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
        self.sources_files = self.compute_sources_files(recursive=recursive)

    _UNWANTED_SUBMODULE_FILTER = re.compile("(tests?/|/test_|/[^a-z])", re.IGNORECASE)

    @classmethod
    def is_allowed_submodule_file(cls, submodule_file_candidate):
        """
        Returns true if the given submodule filename looks like a module that should be docuemnted.
        Modules that are not documented are test files or files in test folders, as well as __init__.py
        or any hidden files.

        This method can be customized in subclasses if someone disagrees with this selection.
        """
        return not cls._UNWANTED_SUBMODULE_FILTER.search(submodule_file_candidate)

    def compute_sources_files(self, recursive=False):
        """
        This computes the set of source files referred to.
        By default, it returns all python files in the sources directory and any of its subdirectories,
        except those that are not acceptable to .is_allowed_submodule_file().

        This method can be customized in subclasses if someone disagrees with this selection.
        """
        # This is all the files in the backbone of the sources subdir, but not recursively,
        # which we'll use to get a list of actual submodules we might want to autodoc.
        glob_pattern = os.path.join(self.sources_dir, "*.py")
        recursive_glob_pattern = os.path.join(self.sources_dir, "**/*.py")
        all_files = glob.glob(os.path.join(self.sources_dir, glob_pattern))
        more_files = glob.glob(os.path.join(self.sources_dir, recursive_glob_pattern)) if recursive else []
        sources_files = [
            file for file in all_files + more_files
            if self.is_allowed_submodule_file(file)
        ]
        return sources_files

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
                message = there_are(problems, kind="problem", tense='past', show=False,
                                    context=f"found in the readthedocs declaration file, {self.docs_index_file!r}")
                raise AssertionError(message)


class DebuggingArtifactChecker(StaticChecker):

    _PRINT_PATTERN = "^[^#]*print[(]"
    _TRACE_PATTERN = "^[^#]*pdb[.]set_trace[(][)]"

    DEBUGGING_PATTERNS = {
        "call to print": _PRINT_PATTERN,
        "active use of pdb.set_trace": _TRACE_PATTERN
    }

    def __init__(self, sources_subdir, root_dir=None, debugging_patterns=None):
        super().__init__(sources_subdir=sources_subdir, root_dir=root_dir)
        self.debugging_patterns = debugging_patterns or self.DEBUGGING_PATTERNS

    def check_for_debugging_patterns(self):
        confirm_no_uses(where=os.path.join(self.sources_dir, "*.py"), patterns=self.debugging_patterns)