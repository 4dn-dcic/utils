import contextlib
import csv
import datetime
import glob
import io
import json
import os
import re
import subprocess
import sys
import warnings

try:
    import piplicenses
except ImportError:  # pragma: no cover - not worth unit testing this case
    if not ((sys.version_info[0] == 3) and (sys.version_info[1] >= 12)):
        # For some reason for with Python 3.12 this gets triggered at least for submitr (TODO: track down further).
        raise Exception("The dcicutils.license_utils module is intended for use at development time, not runtime."
                        " It does not export a requirement for the pip-licenses library,"
                        " but to use this in your unit tests, you are expected to assure a dev dependency on that"
                        " library as part of the [tool.poetry.dependencies] section of your pyproject.toml file."
                        " If you are trying to manually evaluate the utility of this library, you can"
                        " do 'pip install pip-licenses' and then retry importing this library.")
# or you can comment out the above raise of Exception and instead execute:
#
#    subprocess.check_output('pip install pip-licenses'.split(' '))
#    import piplicenses

from collections import defaultdict
from jsonc_parser.parser import JsoncParser
from typing import Any, Dict, DefaultDict, List, Optional, Type, TypeVar, Union

# For obscure reasons related to how this file is used for early prototyping, these must use absolute references
# to modules, not relative references. Later when things are better installed, we can make refs relative again.
from dcicutils.common import Regexp, AnyJsonData
from dcicutils.exceptions import InvalidParameterError
from dcicutils.lang_utils import there_are, conjoined_list
from dcicutils.misc_utils import (
    PRINT, get_error_message, ignorable, ignored, json_file_contents, local_attrs, environ_bool,
    remove_suffix, to_camel_case
)

T = TypeVar("T")

# logging.basicConfig()
# logger = logging.getLogger(__name__)

_FRAMEWORK = 'framework'
_LANGUAGE = 'language'
_LICENSE = 'license'
_LICENSE_CLASSIFIER = 'license_classifier'
_LICENSES = 'licenses'
_NAME = 'name'
_STATUS = 'status'

_INHERITS_FROM = 'inherits_from'
_ALLOWED = 'allowed'
_EXCEPT = 'except'


def pattern(x):
    return re.compile(x, re.IGNORECASE)


def augment(d: dict, by: dict):
    return dict(d, **by)


class LicenseStatus:
    ALLOWED = "ALLOWED"
    SPECIALLY_ALLOWED = "SPECIALLY_ALLOWED"
    FAILED = "FAILED"
    EXPECTED_MISSING = "EXPECTED_MISSING"
    UNEXPECTED_MISSING = "UNEXPECTED_MISSING"


class LicenseOptions:
    # General verbosity, such as progress information
    VERBOSE = environ_bool("LICENSE_UTILS_VERBOSE", default=True)
    # Specific additional debugging output
    DEBUG = environ_bool("LICENSE_UTILS_DEBUG", default=False)
    CONDA_PREFIX = os.environ.get("CONDA_LICENSE_CHECKER_PREFIX", os.environ.get("CONDA_PREFIX", ""))
    POLICY_DIR = os.environ.get("LICENSE_UTILS_POLICY_DIR")

    @classmethod
    @contextlib.contextmanager
    def selected_options(cls, verbose=VERBOSE, debug=DEBUG, conda_prefix=CONDA_PREFIX, policy_dir=POLICY_DIR):
        """
        Allows a script, for example, to specify overrides for these options dynamically.
        """
        with local_attrs(cls, VERBOSE=verbose, DEBUG=debug, CONDA_PREFIX=conda_prefix, POLICY_DIR=policy_dir):
            yield


class LicenseFramework:

    NAME = None

    @classmethod
    def get_dependencies(cls):
        raise NotImplementedError(f'{cls.__name__}.get_dependencies is not implemented.')


class LicenseAnalysis:

    def __init__(self):
        self.frameworks: List[Type[LicenseFramework]] = []
        self.dependency_details: List[Dict[str, Any]] = []
        self.unacceptable: DefaultDict[str, List[str]] = defaultdict(lambda: [])
        self.unexpected_missing: List[str] = []
        self.no_longer_missing: List[str] = []
        self.miscellaneous: List[str] = []


FrameworkSpec = Union[str, LicenseFramework, Type[LicenseFramework]]


class LicenseFrameworkRegistry:

    LICENSE_FRAMEWORKS: Dict[str, Type[LicenseFramework]] = {}

    @classmethod
    @contextlib.contextmanager
    def temporary_registration_for_testing(cls):
        # Enter dynamic context where any license frameworks that get registered during the context
        # are discarded upon exiting the context.
        with local_attrs(cls, LICENSE_FRAMEWORKS=cls.LICENSE_FRAMEWORKS.copy()):
            yield

    @classmethod
    def register_framework(cls, *, name):
        """
        Declares a python license framework classs.
        Mostly these names will be language names like 'python' or 'javascript',
        but they might be names of other, non-linguistic frameworks (like 'cgap-pipeline', for example).
        """
        def _decorator(framework_class: T) -> T:
            if not issubclass(framework_class, LicenseFramework):
                raise ValueError(f"The class {framework_class.__name__} does not inherit from LicenseFramework.")
            framework_class.NAME = name
            cls.LICENSE_FRAMEWORKS[name] = framework_class
            return framework_class
        return _decorator

    @classmethod
    def find_framework(cls, framework_spec: FrameworkSpec):
        if isinstance(framework_spec, str):
            return cls.LICENSE_FRAMEWORKS.get(framework_spec)
        elif (isinstance(framework_spec, LicenseFramework)
              or isinstance(framework_spec, type) and issubclass(framework_spec, LicenseFramework)):
            return framework_spec
        else:
            raise ValueError(f"{framework_spec!r} must be an instance or subclass of LicenseFramework,"
                             f" or a name under which such a class is registered in the LicenseFrameworkRegistry.")

    @classmethod
    def all_frameworks(cls):
        return sorted(cls.LICENSE_FRAMEWORKS.values(), key=lambda x: x.NAME)

    @classmethod
    def all_framework_names(cls):
        return sorted(cls.LICENSE_FRAMEWORKS.keys())


# This is intended to match ' (= 3)', ' (>= 3)', ' (version 3)', ' (version 3 or greater)'
# It will incidentally and harmlessly also take ' (>version 3)' or '(>= 3 or greater)'.
# It will also correctly handle the unlikely case of ' (= 3 or greater)'

_OR_LATER_PATTERN = '(?:[- ]or[ -](?:greater|later))'
_PARENTHETICAL_VERSION_CONSTRAINT = re.compile(f'( [(]([>]?)(?:[=]|version) ([0-9.]+)({_OR_LATER_PATTERN}?)[)])')
_POSTFIX_OR_LATER_PATTERN = re.compile(f"({_OR_LATER_PATTERN})")
_GPL_VERSION_CHOICE = re.compile('^GPL-v?([0-9.+]) (?:OR|[|]) GPL-v?([0-9.+])$')


def simplify_license_versions(licenses_spec: str, *, for_package_name) -> str:
    m = _GPL_VERSION_CHOICE.match(licenses_spec)
    if m:
        version_a, version_b = m.groups()
        return f"GPL-{version_a}-or-{version_b}"
    # We only care which licenses were mentioned, not what algebra is used on them.
    # (Thankfully there are no NOTs, and that's probably not by accident, since that would be too big a set.)
    # So for us, either (FOO AND BAR) or (FOO OR BAR) is the same because we want to treat it as "FOO,BAR".
    # If all of those licenses match, all is good. That _does_ mean some things like (MIT OR GPL-3.0) will
    # have trouble passing unless both MIT and GPL-3.0 are allowed.
    transform_count = 0
    original_licenses_spec = licenses_spec
    ignorable(original_licenses_spec)  # sometimes useful for debugging
    while True:
        if transform_count > 100:  # It'd be surprising if there were even ten of these to convert.
            warnings.warn(f"Transforming {for_package_name} {licenses_spec!r} seemed to be looping."
                          f" Please report this as a bug.")
            return licenses_spec  # return the unmodified
        transform_count += 1
        m = _PARENTHETICAL_VERSION_CONSTRAINT.search(licenses_spec)
        if not m:
            break
        matched, greater, version_spec, greater2 = m.groups()
        is_greater = bool(greater or greater2)
        licenses_spec = licenses_spec.replace(matched,
                                              f"-{version_spec}"
                                              f"{'+' if is_greater else ''}")
    transform_count = 0
    while True:
        if transform_count > 100:  # It'd be surprising if there were even ten of these to convert.
            warnings.warn(f"Transforming {for_package_name} {licenses_spec!r} seemed to be looping."
                          f" Please report this as a bug.")
            return licenses_spec  # return the unmodified
        transform_count += 1
        m = _POSTFIX_OR_LATER_PATTERN.search(licenses_spec)
        if not m:
            break
        matched = m.group(1)
        licenses_spec = licenses_spec.replace(matched, '+')
    if LicenseOptions.DEBUG and licenses_spec != original_licenses_spec:
        PRINT(f"Rewriting {original_licenses_spec!r} as {licenses_spec!r}.")
    return licenses_spec


def extract_boolean_terms(boolean_expression: str, for_package_name: str) -> List[str]:
    # We only care which licenses were mentioned, not what algebra is used on them.
    # (Thankfully there are no NOTs, and that's probably not by accident, since that would be too big a set.)
    # So for us, either (FOO AND BAR) or (FOO OR BAR) is the same because we want to treat it as "FOO,BAR".
    # If all of those licenses match, all is good. That _does_ mean some things like (MIT OR GPL-3.0) will
    # have trouble passing unless both MIT and GPL-3.0 are allowed.
    revised_boolean_expression = (
        boolean_expression
        .replace('(', '')
        .replace(')', '')
        .replace(' AND ', ',')
        .replace(' and ', ',')
        .replace(' & ', ',')
        .replace(' OR ', ',')
        .replace(' or ', ',')
        .replace('|', ',')
        .replace(';', ',')
        .replace(' + ', ',')
        .replace('file ', f'Custom: {for_package_name} file ')
    )
    terms = [x for x in sorted(map(lambda x: x.strip(), revised_boolean_expression.split(','))) if x]
    if LicenseOptions.DEBUG and revised_boolean_expression != boolean_expression:
        PRINT(f"Rewriting {boolean_expression!r} as {terms!r}.")
    return terms


@LicenseFrameworkRegistry.register_framework(name='javascript')
class JavascriptLicenseFramework(LicenseFramework):

    @classmethod
    def implicated_licenses(cls, *, package_name, licenses_spec: str) -> List[str]:
        ignored(package_name)
        licenses_spec = simplify_license_versions(licenses_spec, for_package_name=package_name)
        licenses = extract_boolean_terms(licenses_spec, for_package_name=package_name)
        return licenses

    VERSION_PATTERN = re.compile('^.+?([@][0-9.][^@]*|)$')

    @classmethod
    def strip_version(cls, raw_name):
        name = raw_name
        m = cls.VERSION_PATTERN.match(raw_name)  # e.g., @foo/bar@3.7
        if m:
            suffix = m.group(1)
            if suffix:
                name = remove_suffix(m.group(1), name)
        return name

    @classmethod
    def get_dependencies(cls):
        output = subprocess.check_output(['npx', 'license-checker', '--summary', '--json'],
                                         # This will output to stderr if there's an error,
                                         # but it will still put {} on stdout, which is good enough for us.
                                         stderr=subprocess.DEVNULL)
        records = json.loads(output)
        if not records:
            # e.g., this happens if there's no javascript in the repo
            raise Exception("No javascript license data was found.")
        result = []
        for raw_name, record in records.items():
            name = cls.strip_version(raw_name)
            raw_licenses_spec = record.get(_LICENSES)
            licenses = cls.implicated_licenses(licenses_spec=raw_licenses_spec, package_name=name)
            entry = {
                _NAME: name,
                _LICENSES: licenses,
                _FRAMEWORK: 'javascript'
            }
            result.append(entry)
        return result


@LicenseFrameworkRegistry.register_framework(name='python')
class PythonLicenseFramework(LicenseFramework):

    @classmethod
    def _piplicenses_args(cls, _options: Optional[List[str]] = None):
        parser = piplicenses.create_parser()
        args = parser.parse_args(_options or [])
        return args

    @classmethod
    def get_dependencies(cls):
        args = cls._piplicenses_args()
        result = []
        entries = piplicenses.get_packages(args)
        for entry in entries:
            license_name = entry.get(_NAME)
            licenses = entry.get(_LICENSE_CLASSIFIER) or []
            entry = {
                _NAME: license_name,
                _LICENSES: licenses,
                _FRAMEWORK: 'python',
            }
            result.append(entry)
        return sorted(result, key=lambda x: x.get(_NAME).lower())


@LicenseFrameworkRegistry.register_framework(name='conda')
class CondaLicenseFramework(LicenseFramework):

    @classmethod
    def get_dependencies(cls):
        prefix = LicenseOptions.CONDA_PREFIX
        result = []
        filespec = os.path.join(prefix, "conda-meta/*.json")
        files = glob.glob(filespec)
        for file in files:
            data = json_file_contents(file)
            package_name = data['name']
            package_license = data.get('license') or "MISSING"
            if package_license:
                simplified_package_license_spec = simplify_license_versions(package_license,
                                                                            for_package_name=package_name)
                package_licenses = extract_boolean_terms(simplified_package_license_spec,
                                                         for_package_name=package_name)
            else:
                package_licenses = []
            entry = {
                _NAME: package_name,
                _LICENSES: package_licenses,
                _FRAMEWORK: 'conda',
            }
            result.append(entry)
        result.sort(key=lambda x: x['name'])
        return result


@LicenseFrameworkRegistry.register_framework(name='r')
class RLicenseFramework(LicenseFramework):

    R_PART_SPEC = re.compile("^Part of R [0-9.]+$")
    R_LANGUAGE_LICENSE_NAME = 'R-language-license'

    @classmethod
    def implicated_licenses(cls, *, package_name, licenses_spec: str) -> List[str]:
        if cls.R_PART_SPEC.match(licenses_spec):
            return [cls.R_LANGUAGE_LICENSE_NAME]
        licenses_spec = simplify_license_versions(licenses_spec, for_package_name=package_name)
        licenses = extract_boolean_terms(licenses_spec, for_package_name=package_name)
        return licenses

    @classmethod
    def get_dependencies(cls):
        # NOTE: Although the R Language itself is released under the GPL, our belief is that it is
        # still possible to write programs in R that are not GPL, even programs that use commercial licenses.
        # So we do ordinary license checking here, same as in other frameworks.
        # For notes on this, see the R FAQ.
        # Ref: https://cran.r-project.org/doc/FAQ/R-FAQ.html#Can-I-use-R-for-commercial-purposes_003f

        _PACKAGE = "Package"
        _LICENSE = "License"

        found_problems = 0

        output_bytes = subprocess.check_output(['r', '--no-echo', '-q', '-e',
                                                f'write.csv(installed.packages()[,c("Package", "License")])'],
                                               # This will output to stderr if there's an error,
                                               # but it will still put {} on stdout, which is good enough for us.
                                               stderr=subprocess.DEVNULL)
        output = output_bytes.decode('utf-8')
        result = []
        first_line = True
        for entry in csv.reader(io.StringIO(output)):  # [ignore, package, license]
            if first_line:
                first_line = False
                if entry == ["", _PACKAGE, _LICENSE]:  # we expect headers
                    continue
            try:
                package_name = entry[1]
                licenses_spec = entry[2]
                licenses = cls.implicated_licenses(package_name=package_name, licenses_spec=licenses_spec)
                entry = {
                    _NAME: package_name,
                    _LICENSES: licenses,
                    _FRAMEWORK: 'r',
                }
                result.append(entry)
            except Exception as e:
                found_problems += 1
                if LicenseOptions.VERBOSE:
                    PRINT(get_error_message(e))
        if found_problems > 0:
            warnings.warn(there_are(found_problems, kind="problem", show=False, punctuate=True, tense='past'))
        return sorted(result, key=lambda x: x.get(_NAME).lower())


class LicenseFileParser:

    SEPARATORS = '-.,'
    SEPARATORS_AND_WHITESPACE = SEPARATORS + ' \t'
    COPYRIGHT_SYMBOL = '\u00a9'

    COPYRIGHT_LINE = re.compile(f"^Copyright"
                                f"(?: *(?:{COPYRIGHT_SYMBOL}|\\(c\\)))?"
                                f" *((?:(?:[{SEPARATORS}] *)?[1-9][0-9][0-9][0-9] *)+)*"
                                f" *(.+)$",
                                re.IGNORECASE)

    COPYRIGHT_OWNER_SANS_SUFFIX = re.compile(f"^(.*[^{SEPARATORS}]) *[{SEPARATORS}] *All Rights Reserved[.]?$",
                                             re.IGNORECASE)

    @classmethod
    def parse_simple_license_file(cls, *, filename):
        """
        Licenses could be complicated, but we assume a file approximately of the form:
            <license-title>
            Copyright [<copyright-marker>] <copyright-year> <copyright-owner> [All Rights Reserved.]
            <license-text>
        where there is a single license named <license-title>, an actual unicode copyright sign or the letter
        'c' in parentheses, a <copyright-year> (or years connected by hyphens and commas),
        a <copyright-owner>, and optionally the words 'all rights reserved',
        and finally the <license-text> of the single license named in the <license-title>.

        Returns: a json dictionary containing the keys title, copyright-title, copyright-owner, copyright-year,
                 and copyright-text.
        """
        with io.open(filename, 'r') as fp:
            license_title = []
            copyright_owners = []
            primary_copyright_owner = None
            copyright_seen = False
            lines = []
            for i, line in enumerate(fp):
                line = line.strip(' \t\n\r')
                m = cls.COPYRIGHT_LINE.match(line) if line[:1].isupper() else None
                if not m:
                    lines.append(line)
                else:
                    copyright_year = m.group(1).strip(cls.SEPARATORS_AND_WHITESPACE)
                    copyright_owner = m.group(2).rstrip(cls.SEPARATORS_AND_WHITESPACE)
                    m = cls.COPYRIGHT_OWNER_SANS_SUFFIX.match(copyright_owner)
                    if m:
                        copyright_owner = m.group(1)
                    if not copyright_seen:
                        primary_copyright_owner = copyright_owner
                    copyright_owners.append(copyright_owner)
                    if not copyright_seen:
                        license_title = '\n'.join(lines).strip('\n')
                        lines = []
                    else:
                        lines.append(line)
                    copyright_seen = True
            if not copyright_seen:
                raise Exception("Missing copyright line.")
            license_text = '\n'.join(lines).strip('\n')
            return {
                'license-title': license_title,
                'copyright-owner': primary_copyright_owner,
                'copyright-owners': copyright_owners,
                'copyright-year': copyright_year,
                'license-text': license_text
            }

    @classmethod
    def validate_simple_license_file(cls, *, filename: str,
                                     check_license_title: Optional[str] = None,  # a license name
                                     check_copyright_year: Union[bool, str] = True,
                                     check_copyright_owner: str = None,  # a copyright owner
                                     analysis: LicenseAnalysis = None):
        def report(message):
            if analysis:
                analysis.miscellaneous.append(message)
            else:
                warnings.warn(message)
        parsed = cls.parse_simple_license_file(filename=filename)
        if check_license_title:
            license_title = parsed['license-title']
            if not re.match(check_license_title, license_title):
                report(f"The license, {license_title!r}, was expected to match {check_license_title!r}.")
        if check_copyright_year:
            if check_copyright_year is True:
                check_copyright_year = str(datetime.datetime.now().year)
            copyright_year = parsed['copyright-year']
            if not copyright_year.endswith(check_copyright_year):
                report(f"The copyright year, {copyright_year!r}, should have {check_copyright_year!r} at the end.")
        if check_copyright_owner:
            copyright_owner = parsed['copyright-owner']
            if not re.match(check_copyright_owner, copyright_owner):
                report(f"The copyright owner, {copyright_owner!r}, was expected to match {check_copyright_owner!r}.")


class LicenseChecker:
    """
    License checkers are defined as .jsonc. The JSONC file format is JSON with Comments.
    (The comments are Javascript syntax, either '//' or '/* ... */'.)

    There are these important class variables to specify:

    LICENSE_TITLE is a string naming the license to be expected in LICENSE.txt

    COPYRIGHT_OWNER is the name of the copyright owner.

    LICENSE_FRAMEWORKS will default to all defined frameworks (presently ['python', 'javascript'],
      but can be limited to just ['python'] for example.  It doesn't make a lot of sense to limit it to
      ['javascript'], though you could, since you are using a Python library to do this, and it probably
      needs to have its dependencies checked.

    ALLOWED is a list of license names as returned by the various license frameworks. Because they rely on different
      underlying tools the exact format of the names that result might vary. For this reason, there is a regular
      expression capability for this particular attribute. In addition to just a string, you can also use
      {"pattern": "<regexp>"} For very long regular expressions, {"pattern": ["<regexp-part-1>", ...]} will
      concatenate all the parts into a single regexp, so they can be gracefully broken over lines in the .jsonc
      source file.  If regexp flags are requierd, use {"pattern" "<regexp>", "flags": ["flag1", ...]}.

    EXPECTED_MISSING_LICENSES is a list of libraries that are expected to have no license information.
      This is so you don't have to get warning fatigue by seeing a warning over and over for things you know about.
      If a new library with no license info shows up that you don't expect, you should investigate it,
      make sure it's OK, and then add it to this list.

    EXCEPTIONS is a table (a dict) keyed on license names with entries that are lists of library names that are
      allowed to use the indicated license even though the license might not be generally allowed. This should be
      used for license types that might require special consideration. For example, some uses may be OK in a dev-only
      situation like testing or documentation that are not OK in some other situation.

    Note that if you don't like these license names, which are admittedly non-standard and do nt seem to use
    SPDX naming conventions, you can customize the get_dependencies method to return a different
    list, one of the form
    [{"name": "libname", "license_classifier": ["license1", "license2", ...], "framework": "python"}]
    by whatever means you like and using whatever names you like.
    """

    # Set this to True in subclasses if you want your organization's policy to be that you see
    # some visible proof of which licenses were checked.
    LICENSE_TITLE = None
    COPYRIGHT_OWNER = None
    LICENSE_FRAMEWORKS = None

    EXPECTED_MISSING_LICENSES = []

    ALLOWED: List[str] = []

    EXCEPTIONS: Dict[str, str] = {}

    POSSIBLE_LICENSE_FILE_BASE_NAMES = ['LICENSE']
    POSSIBLE_LICENSE_EXTENSIONS = ['', '.txt', '.text', '.md', '.rst']

    @classmethod
    def find_license_files(cls) -> List[str]:
        results = []
        for file_name in cls.POSSIBLE_LICENSE_FILE_BASE_NAMES:
            for file_ext in cls.POSSIBLE_LICENSE_EXTENSIONS:
                file = file_name + file_ext
                if os.path.exists(file):
                    results.append(file)
        return results

    MULTIPLE_LICENSE_FILE_ADVICE = ("Multiple license files create a risk of inconsistency."
                                    " Best practice is to have only one.")

    @classmethod
    def analyze_license_file(cls, *, analysis: LicenseAnalysis,
                             copyright_owner: Optional[str] = None,
                             license_title: Optional[str] = None) -> None:

        copyright_owner = copyright_owner or cls.COPYRIGHT_OWNER
        license_title = license_title or cls.LICENSE_TITLE

        if copyright_owner is None:
            analysis.miscellaneous.append(f"Class {cls.__name__} has no declared license owner.")

        license_files = cls.find_license_files()
        if not license_files:
            analysis.miscellaneous.append("Missing license file.")
            return

        if len(license_files) > 1:
            analysis.miscellaneous.append(
                there_are(license_files, kind='license file', show=True, punctuate=True)
                + " " + cls.MULTIPLE_LICENSE_FILE_ADVICE
            )

        for license_file in license_files:
            LicenseFileParser.validate_simple_license_file(filename=license_file,
                                                           check_copyright_owner=copyright_owner or cls.COPYRIGHT_OWNER,
                                                           check_license_title=license_title or cls.LICENSE_TITLE,
                                                           analysis=analysis)

    CHOICE_REGEXPS = {}

    @classmethod
    def _make_regexp_for_choices(cls, choices):
        inner_pattern = '|'.join('^' + (re.escape(choice) if isinstance(choice, str) else choice.pattern) + '$'
                                 for choice in choices) or "^$"
        return re.compile(f"({inner_pattern})", re.IGNORECASE)

    @classmethod
    def _find_regexp_for_choices(cls, choices):
        key = str(choices)
        regexp = cls.CHOICE_REGEXPS.get(key)
        if not regexp:
            cls.CHOICE_REGEXPS[key] = regexp = cls._make_regexp_for_choices(choices)
        return regexp

    @classmethod
    def analyze_license_dependencies_for_framework(cls, *,
                                                   analysis: LicenseAnalysis,
                                                   framework: Type[LicenseFramework],
                                                   acceptable: Optional[List[str]] = None,
                                                   exceptions: Optional[Dict[str, str]] = None,
                                                   ) -> None:
        acceptability_regexp = cls._find_regexp_for_choices((acceptable or []) + (cls.ALLOWED or []))
        exceptions = dict(cls.EXCEPTIONS or {}, **(exceptions or {}))

        try:
            entries = framework.get_dependencies()
        except Exception as e:
            analysis.miscellaneous.append(f"License framework {framework.NAME!r} failed to get licenses:"
                                          f" {get_error_message(e)}")
            return

        # We don't add this information until we've successfully retrieved dependency info
        # (If we failed, we reported the problem as part of the analysis.)
        analysis.frameworks.append(framework)

        for entry in entries:
            name = entry[_NAME]
            license_names = entry[_LICENSES]
            if not license_names:
                if name in cls.EXPECTED_MISSING_LICENSES:
                    status = LicenseStatus.EXPECTED_MISSING
                else:
                    status = LicenseStatus.UNEXPECTED_MISSING
                    analysis.unexpected_missing.append(name)
            else:
                if name in cls.EXPECTED_MISSING_LICENSES:
                    analysis.no_longer_missing.append(name)
                status = LicenseStatus.ALLOWED
                by_special_exception = False
                for license_name in license_names:
                    special_exceptions = exceptions.get(license_name, [])
                    if acceptability_regexp.match(license_name):  # license_name in acceptable:
                        pass
                    elif name in special_exceptions:
                        by_special_exception = True
                    else:
                        status = LicenseStatus.FAILED
                        analysis.unacceptable[license_name].append(name)
                if status == LicenseStatus.ALLOWED and by_special_exception:
                    status = LicenseStatus.SPECIALLY_ALLOWED
            analysis.dependency_details.append({
                _NAME: name,
                _FRAMEWORK: framework.NAME,
                _LICENSES: license_names,
                _STATUS: status
            })
            if LicenseOptions.VERBOSE:  # pragma: no cover - this is just for debugging
                PRINT(f"Checked {framework.NAME} {name}:"
                      f" {'; '.join(license_names) if license_names else '---'} ({status})")

    @classmethod
    def analyze_license_dependencies_by_framework(cls, *,
                                                  analysis: LicenseAnalysis,
                                                  frameworks: Optional[List[FrameworkSpec]] = None,
                                                  acceptable: Optional[List[str]] = None,
                                                  exceptions: Optional[Dict[str, str]] = None,
                                                  ) -> None:

        if frameworks is None:
            frameworks = cls.LICENSE_FRAMEWORKS

        if frameworks is None:
            frameworks = LicenseFrameworkRegistry.all_frameworks()
        else:
            frameworks = [LicenseFrameworkRegistry.find_framework(framework_spec)
                          for framework_spec in frameworks]

        for framework in frameworks:
            cls.analyze_license_dependencies_for_framework(analysis=analysis, framework=framework,
                                                           acceptable=acceptable, exceptions=exceptions)

    @classmethod
    def show_unacceptable_licenses(cls, *, analysis: LicenseAnalysis) -> LicenseAnalysis:
        if analysis.unacceptable:
            # This is part of the essential output, so is not conditional on switches.
            PRINT(there_are(analysis.unacceptable, kind="unacceptable license", show=False, punctuation_mark=':'))
            for license, names in sorted(analysis.unacceptable.items()):
                PRINT(f" {license}: {', '.join(names)}")
        return analysis

    @classmethod
    def validate(cls, frameworks: Optional[List[FrameworkSpec]] = None) -> None:
        """
        This method is intended to be used in a unit test, as in:

            from my_org_tools import MyOrgLicenseChecker
            def test_license_compatibility():
                MyOrgLicenseChecker.validate()

        where my_org_tools has done something like:

            from dcicutils.license_utils import LicenseChecker
            class MyOrgLicenseChecker(LicenseChecker):
                LICENSE_OWNER = "..."
                ALLOWED = [...]
                EXPECTED_MISSING_LICENSES = [...]
                EXCEPTIONS = {...}

        See the example of C4InfrastructureLicenseChecker we use in our own group for our own family of toools,
        which we sometimes informally refer to collectively as 'C4'.
        """
        analysis = LicenseAnalysis()
        cls.analyze_license_dependencies_by_framework(analysis=analysis, frameworks=frameworks)
        cls.analyze_license_file(analysis=analysis)
        cls.show_unacceptable_licenses(analysis=analysis)
        if analysis.unexpected_missing:
            warnings.warn(there_are(analysis.unexpected_missing, kind='unexpectedly missing license', punctuate=True))
        if analysis.no_longer_missing:
            # This is not so major as to need a warning, but it's still something that should show up somewhere.
            PRINT(there_are(analysis.no_longer_missing, kind='no-longer-missing license', punctuate=True))
        for message in analysis.miscellaneous:
            warnings.warn(message)
        if analysis.unacceptable:
            raise LicenseAcceptabilityCheckFailure(unacceptable_licenses=analysis.unacceptable)


class LicenseCheckerRegistry:

    REGISTRY: Dict[str, Type[LicenseChecker]] = {}

    @classmethod
    def register_checker(cls, name: str):
        def _register(license_checker_class: Type[LicenseChecker]):
            cls.REGISTRY[name] = license_checker_class
            return license_checker_class
        return _register

    @classmethod
    def find_checker(cls, checker_name: str) -> Optional[Type[LicenseChecker]]:
        return cls.REGISTRY.get(checker_name, None)

    @classmethod
    def lookup_checker(cls, checker_name: str, autoload: bool = True,
                       policy_dir: Optional[str] = None) -> Type[LicenseChecker]:
        result: Optional[Type[LicenseChecker]] = cls.find_checker(checker_name)
        if result is None:
            if autoload:
                policy_dir = policy_dir or LicenseOptions.POLICY_DIR or POLICY_DIR
                PRINT(f"Looking for custom policy {checker_name} in {policy_dir} ...")
                result = find_or_create_license_class(policy_name=checker_name,
                                                      policy_dir=policy_dir)
                if result:
                    return result
            raise InvalidParameterError(parameter='checker_name', value=checker_name,
                                        options=cls.all_checker_names())
        return result

    @classmethod
    def all_checker_names(cls):
        return list(cls.REGISTRY.keys())


class LicenseCheckFailure(Exception):

    DEFAULT_MESSAGE = "License check failure."

    def __init__(self, message=None):
        super().__init__(message or self.DEFAULT_MESSAGE)


class LicenseOwnershipCheckFailure(LicenseCheckFailure):

    DEFAULT_MESSAGE = "License ownership check failure."


class LicenseAcceptabilityCheckFailure(LicenseCheckFailure):

    DEFAULT_MESSAGE = "License acceptability check failure."

    def __init__(self, message=None, unacceptable_licenses=None):
        self.unacceptable_licenses = unacceptable_licenses
        if not message and unacceptable_licenses:
            message = there_are(unacceptable_licenses, kind='unacceptable license')
        super().__init__(message=message)


def literal_string_or_regexp_from_dict(item):
    """
    Expects either a string (which will be matched using ordinary equality) ore a regular expression,
    expressed as a dictionary of the form {"pattern": <regexp>, "flags": [<flag>, ...]}
    The pattern is required. The flags may be omitted if null.
    A pattern is either a string or a list of strings. If it is a list of strings, it will be concatenated
    into a single string, which can be useful for breaking long strings over lines.
    Flags are string names of re.WHATEVER flags that would be given to Python's re.compile.
    UNICODE and IGNORECASE are on by default.
    """
    if isinstance(item, str):
        return item
    elif not isinstance(item, dict):
        raise ValueError(f'Expected a string or a dictionary describing a regular expression.')
    pattern = item.get('pattern')
    # The pattern is permitted to be a string or list of strings, since in a JSON-style file we can't
    # do the thing we do in python where we just juxtapose several strings, separated by whitespace
    # and/or newlines, in order to have them taken as a single literal string. -kmp 29-Sep-2023
    if isinstance(pattern, str):
        pass
    elif isinstance(pattern, list):
        pattern = ''.join(pattern)
    else:
        raise ValueError(f"Invalid pattern expression: {item!r}")
    flags = item.get('flags') or []
    compilation_flags = re.IGNORECASE  # UNICODE will default, but IGNORECASE we have to set up manually
    for flag in flags:
        if isinstance(flag, str) and flag.isupper():
            if hasattr(re, flag):
                compilation_flags |= getattr(re, flag)
            else:
                raise ValueError(f"No such flag re.{flag}")
        else:
            raise ValueError(f"Flags must be strigs: {flag!r}")
    regexp = re.compile(pattern, compilation_flags)
    return regexp


def read_license_policy_file(file):
    """
    Reads a license policy file, which is a JSONC file (can contain JSON with Javascript-style comments)
    The policy is a dictionary, but the ALLOWED option is a list that can contain special syntax allowing
    a regular expression to be inferred. See documentation of `string_or_regexp_dict` for details.
    """
    data = JsoncParser.parse_file(file)
    allowed = data.get('ALLOWED')
    if isinstance(allowed, list):
        # The "ALLOWED" option is specially permitted to contain regular expressions.
        data['ALLOWED'] = [literal_string_or_regexp_from_dict(allowance) for allowance in allowed]
    return data


_MY_DIR = os.path.dirname(__file__)

POLICY_DIR = os.path.join(_MY_DIR, "license_policies")

POLICY_DATA_CACHE = {}


def built_in_policy_names():
    return [
        os.path.splitext(os.path.basename(license_policy_path))[0]
        for license_policy_path in glob.glob(os.path.join(POLICY_DIR, "*.jsonc"))]


def find_policy_data(policy_name: str, policy_dir: Optional[str] = None,
                     use_cache: bool = True, error_if_missing: bool = True):
    policy_dir = POLICY_DIR if policy_dir is None else policy_dir
    existing_data = POLICY_DATA_CACHE.get(policy_name) if use_cache else None
    if existing_data:
        return existing_data
    else:
        filename = os.path.join(policy_dir, policy_name + ".jsonc")
        if not os.path.exists(filename):
            if error_if_missing:
                raise ValueError(f"No such policy: {policy_name!r}")
            else:
                return None
        data = read_license_policy_file(filename)
        POLICY_DATA_CACHE[policy_name] = data
        return data


def find_or_create_license_class(*, policy_name: str, policy_dir: str,
                                 # This next argument should never be passed explicitly by callers other than
                                 # recursive calls to this function. -kmp 28-Sep-2023
                                 _creation_attmpts_in_progress=None):
    """
    Define a policy class given a policy name (like 'c4-infrastructure').
    """
    _creation_attmpts_in_progress = _creation_attmpts_in_progress or []
    existing_checker = LicenseCheckerRegistry.find_checker(checker_name=policy_name)
    if existing_checker:
        return existing_checker
    elif policy_name in _creation_attmpts_in_progress:
        raise ValueError(f"Circular reference to {policy_name} detected"
                         f" while creating {conjoined_list(_creation_attmpts_in_progress)}.")
    _creation_attmpts_in_progress.append(policy_name)
    license_checker_class_name = to_camel_case(policy_name) + "LicenseChecker"
    policy_data = find_policy_data(policy_name, policy_dir=policy_dir)
    inherits_from = policy_data.get('inherits_from')
    if not isinstance(inherits_from, list):
        raise ValueError(f'Policy {policy_name!r} needs "inherits_from": [...parent names...],'
                         f' which may be empty but must be specified.')
    license_frameworks = policy_data.get('LICENSE_FRAMEWORKS')
    if license_frameworks == "ALL":
        policy_data['LICENSE_FRAMEWORKS'] = LicenseFrameworkRegistry.all_framework_names()
    parent_classes = [find_or_create_license_class(policy_name=parent_name, policy_dir=policy_dir,
                                                   _creation_attmpts_in_progress=_creation_attmpts_in_progress)
                      for parent_name in inherits_from]
    defaulted_policy_data = default_policy_data(policy_name=policy_name, policy_data=policy_data,
                                                parent_classes=parent_classes)
    new_class = type(license_checker_class_name,
                     (*parent_classes, LicenseChecker),
                     {'_policy_data': policy_data, **defaulted_policy_data})
    new_class.__doc__ = policy_data.get("description") or f'License policy {policy_name} needs a "description".'
    # Sigh. PyCharm can't figure this out type fact out, even with a type hint on the above assignment to new_class,
    # such as 'new_class: Type[LicenseChecker] = ...'. That should have worked. Putting in an assert was the only way
    # I could find to convince PyCharm of the truth. I don't expect this assertion to ever fail. It's just an artifact
    # to prevent ugly browser highlighting. I'll try to arrange a bug report for them. -kmp 29-Sep-2023
    assert isinstance(new_class, type) and issubclass(new_class, LicenseChecker)
    license_policy_class: Type[LicenseChecker] = new_class
    decorator = LicenseCheckerRegistry.register_checker(name=policy_name)
    registered_class = decorator(license_policy_class)
    if LicenseOptions.DEBUG:  # pragma: no cover - this doesn't have to work for production
        found_class = LicenseCheckerRegistry.lookup_checker(policy_name)
        PRINT(f"Registered checker class {policy_name!r}"
              f" with license_frameworks {conjoined_list(found_class.LICENSE_FRAMEWORKS)}.")
    _creation_attmpts_in_progress.remove(policy_name)
    return registered_class


def use_policy_literal(*, policy_name, policy_datum, other_policy_data):
    """This is used for datum that requires no merging. The policy_datum is returned. Other arguments are ignored."""
    ignored(policy_name, other_policy_data)
    return policy_datum


def str_or_regexp_sort_key(datum: Union[str, Regexp]):
    """
    Returns a key for a datum that is an element of a list of elements that are strings or compiled regular expressions.
    Regular expressions will sort where their parttern would be in the series of strings.
    """
    # Rationale: We want something like this just to make testing predictable.
    if isinstance(datum, str):
        return datum
    else:
        return datum.pattern


def merge_policy_lists(*, policy_name, policy_datum, other_policy_data, sort_key=None):
    """
    Merges a set of policy lists by appending them and de-duplicating.
    By default, the result list is assumed to be homogenous in type and suitable for sorting.
    If the list is of heterogeneous type, a sort_key is must be supplied to allow a total ordering.
    """
    ignored(policy_name)
    result = policy_datum
    for other_datum in other_policy_data:
        result += other_datum
    # de-duplicate and apply a deterministic ordering to make testing easier.
    return sorted(set(result), key=sort_key)


def merge_policy_strings_or_regexps(*, policy_name, policy_datum, other_policy_data):
    return merge_policy_lists(policy_name=policy_name, policy_datum=policy_datum, other_policy_data=other_policy_data,
                              sort_key=str_or_regexp_sort_key)


def merge_policy_dicts(*, policy_name, policy_datum, other_policy_data):
    ignored(policy_name)
    merged = defaultdict(lambda: [])

    def add_to_merged(d):
        for k, values in d.items():
            for value in values:
                merged[k].append(value)

    add_to_merged(policy_datum)
    for other_datum in other_policy_data:
        add_to_merged(other_datum)

    return {k: sorted(set(v)) for k, v in sorted(merged.items())}


POLICY_ATTRS: callable = {
    'class_key': use_policy_literal,
    'class_name': use_policy_literal,
    'inherits_from': use_policy_literal,
    'description': use_policy_literal,
    'LICENSE_TITLE': use_policy_literal,
    'COPYRIGHT_OWNER': use_policy_literal,
    'LICENSE_FRAMEWORKS': use_policy_literal,
    'ALLOWED': merge_policy_strings_or_regexps,
    'EXPECTED_MISSING_LICENSES': merge_policy_lists,
    'EXCEPTIONS': merge_policy_dicts,
}

POLICY_MERGE_LISTS = {'ALLOWED', 'EXPECTED_MISSING_LICENSES'}
POLICY_MERGE_DICTS = {'EXCEPTIONS'}


def get_attrs_for_classes(attr: str, class_data: List[Type]):
    result = []
    for class_datum in class_data:
        attr_val = getattr(class_datum, attr, None)  # Intentionally treats explicit None the same as missing
        if attr_val is not None:
            result.append(attr_val)
    return result


def default_policy_data(*, policy_name: str, policy_data: AnyJsonData, parent_classes: List[Type]):
    result = {}
    for key_to_default, val_to_be_defaulted in policy_data.items():
        attr_handler: Optional[callable] = POLICY_ATTRS.get(key_to_default)
        if attr_handler is None:
            raise ValueError(f"Bad policy attribute: {key_to_default}")
        result[key_to_default] = attr_handler(policy_name=policy_name, policy_datum=val_to_be_defaulted,
                                              other_policy_data=get_attrs_for_classes(key_to_default, parent_classes))
    return result


def load_license_policies(policy_dir=None):
    for policy_name in built_in_policy_names():
        find_or_create_license_class(policy_name=policy_name, policy_dir=policy_dir)


# This will cause the definitions of classes to in the predefined set to be exported by this library
# in case they need to be imported elsewhere, for example to use in unit-testing. Those are things like
#  * ParkLabCommonLicenseChecker, etc.
#  * C4InfrastructureLicenseChecker, etc.
# See license_policies/*.jsonc for a full list.
load_license_policies()

ParkLabCommonLicenseChecker = LicenseCheckerRegistry.lookup_checker('park-lab-common')
ParkLabCommonServerLicenseChecker = LicenseCheckerRegistry.lookup_checker('park-lab-common-server')
ParkLabPipelineLicenseChecker = LicenseCheckerRegistry.lookup_checker('park-lab-pipeline')
ParkLabGplPipelineLicenseChecker = LicenseCheckerRegistry.lookup_checker('park-lab-gpl-pipeline')
C4InfrastructureLicenseChecker = LicenseCheckerRegistry.lookup_checker('c4-infrastructure')
C4PythonInfrastructureLicenseChecker = LicenseCheckerRegistry.lookup_checker('c4-python-infrastructure')
