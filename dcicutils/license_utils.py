import contextlib
import csv
import datetime
import glob
import io
import json
# import logging
import os
import re
import subprocess
import warnings

try:
    import piplicenses
except ImportError:  # pragma: no cover - not worth unit testing this case
    raise Exception("The dcicutils.license_utils module is intended for use at development time, not runtime."
                    " It does not export a requirement for the pip-licenses library,"
                    " but to use this in your unit tests, you are expected to assure a dev dependency on that library"
                    " as part of the [tool.poetry.dependencies] section of your pyproject.toml file."
                    " If you are trying to manually evaluate the utility of this library, you can"
                    " do 'pip install pip-licenses' and then retry importing this library.")
# or you can comment out the above raise of Exception and instead execute:
#
#    subprocess.check_output('pip install pip-licenses'.split(' '))
#    import piplicenses

from collections import defaultdict
from typing import Any, Dict, DefaultDict, List, Optional, Type, TypeVar, Union

# For obscure reasons related to how this file is used for early prototyping, these must use absolute references
# to modules, not relative references. Later when things are better installed, we can make refs relative again.
from dcicutils.exceptions import InvalidParameterError
from dcicutils.lang_utils import there_are
from dcicutils.misc_utils import (
    PRINT, get_error_message, ignorable, ignored, json_file_contents, local_attrs, environ_bool,
    remove_suffix,
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

    @classmethod
    @contextlib.contextmanager
    def selected_options(cls, verbose=VERBOSE, debug=DEBUG, conda_prefix=CONDA_PREFIX):
        """
        Allows a script, for example, to specify overrides for these options dynamically.
        """
        with local_attrs(cls, VERBOSE=verbose, DEBUG=debug, CONDA_PREFIX=conda_prefix):
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
                # print(f"package_license={package_license}")
                simplified_package_license_spec = simplify_license_versions(package_license,
                                                                            for_package_name=package_name)
                # print(f" =simplified_package_license_spec => {simplified_package_license_spec}")
                package_licenses = extract_boolean_terms(simplified_package_license_spec,
                                                         for_package_name=package_name)
                # print(f"=> {package_licenses}")
            else:
                package_licenses = []
            entry = {
                _NAME: package_name,
                _LICENSES: package_licenses,
                _FRAMEWORK: 'conda',
            }
            result.append(entry)
        result.sort(key=lambda x: x['name'])
        # print(f"conda get_dependencies result={json.dumps(result, indent=2)}")
        # print("conda deps = ", json.dumps(result, indent=2))
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
    There are three important class variables to specify:

    LICENSE_TITLE is a string naming the license to be expected in LICENSE.txt

    COPYRIGHT_OWNER is the name of the copyright owner.

    FRAMEWORKS will default to all defined frameworks (presently ['python', 'javascript'], but can be limited to
     just ['python'] for example.  It doesn't make a lot of sense to limit it to ['javascript'], though you could,
     since you are using a Python library to do this, and it probably needs to have its dependencies checked.

    ALLOWED is a list of license names as returned by the pip-licenses library.

    EXPECTED_MISSING is a list of libraries that are expected to have no license information. This is so you don't
      have to get warning fatigue by seeing a warning over and over for things you know about. If a new library
      with no license info shows up that you don't expect, you should investigate it, make sure it's OK,
      and then add it to this list.

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
    def lookup_checker(cls, name: str) -> Type[LicenseChecker]:
        result: Optional[Type[LicenseChecker]] = cls.REGISTRY.get(name)
        if result is None:
            raise InvalidParameterError(parameter='checker_name', value=name,
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


@LicenseCheckerRegistry.register_checker('park-lab-common')
class ParkLabCommonLicenseChecker(LicenseChecker):
    """
    Minimal checker common to all tech from Park Lab.
    """

    COPYRIGHT_OWNER = "President and Fellows of Harvard College"

    ALLOWED = [

        # <<Despite its name, Zero-Clause BSD is an alteration of the ISC license,
        #   and is not textually derived from licenses in the BSD family.
        #   Zero-Clause BSD was originally approved under the name “Free Public License 1.0.0”>>
        # Ref: https://opensource.org/license/0bsd/
        '0BSD',

        # Linking = Permissive, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'Academic Free License (AFL)',
        'AFL-2.1',

        # Linking = Permissive, Private Use = Yes
        # Apache licenses before version 2.0 are controversial, but we here construe an unmarked naming to imply
        # any version, and hence v2.
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'Apache Software License',
        'Apache-Style',
        pattern("Apache([- ]2([.]0)?)?([- ]Licen[cs]e)?([- ]with[- ]LLVM[- ]exception)?"),
        # 'Apache-2.0',

        # Artistic License 1.0 was confusing to people, so its status as permissive is in general uncertain,
        # however the issue seems to revolve around point 8 (relating to whether or not perl is deliberately
        # exposed). That isn't in play for our uses, so we don't flag it here.
        # Artistic license 2.0 is a permissive license.
        # Ref: https://en.wikipedia.org/wiki/Artistic_License
        'Artistic-1.0-Perl',
        pattern('Artistic[- ]2([.]0)?'),

        # According to Wikipedia, the Boost is considered permissive and BSD-like.
        # Refs:
        #  *
        #  * https://en.wikipedia.org/wiki/Boost_(C%2B%2B_libraries)#License
        pattern('(BSL|Boost(([- ]Software)?[- ]License)?)([- ]1([.]0)?)?'),

        # Linking = Permissive, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        pattern('((modified[- ])?[234][- ]Clause[- ])?BSD([- ][234][- ]Clause)?( Licen[cs]e)?'),
        # 'BSD License',
        # 'BSD-2-Clause',
        # 'BSD-3-Clause',
        # 'BSD 3-Clause',

        # BZIP2 is a permissive license
        # Ref: https://github.com/asimonov-im/bzip2/blob/master/LICENSE
        pattern('bzip2(-1[.0-9]*)'),

        # Linking = Public Domain, Private Use = Public Domain
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'CC0',
        'CC0-1.0',

        # Linking = Permissive, Private Use = Permissive
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'CC-BY',
        'CC-BY-3.0',
        'CC-BY-4.0',

        # The curl license is a permissive license.
        # Ref: https://curl.se/docs/copyright.html
        'curl',

        # Linking = Permissive, Private Use = ?
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'CDDL',

        # The original Eclipse Distribution License 1.0 is essentially a BSD-3-Clause license.
        # Ref: https://www.eclipse.org/org/documents/edl-v10.php
        'Eclipse Distribution License',

        # Linking = Permissive, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'Eclipse Public License',
        'EPL-2.0',

        # The FSF Unlimited License (FSFUL) seems to be a completely permissive license.
        # Refs:
        #  * https://spdx.org/licenses/FSFUL.html
        #  * https://fedoraproject.org/wiki/Licensing/FSF_Unlimited_License
        'FSF Unlimited License',
        'FSFUL',

        # The FreeType license is a permissive license.
        # Ref: LicenseRef-FreeType
        pattern('(Licen[cs]eRef-)?(FTL|FreeType( Licen[cs]e)?)'),

        # Linking = Yes, Cat = Permissive Software Licenses
        # Ref: https://en.wikipedia.org/wiki/Historical_Permission_Notice_and_Disclaimer
        'Historical Permission Notice and Disclaimer (HPND)',
        'HPND',
        pattern('(Licen[cs]eRef-)?PIL'),
        # The Pillow or Python Image Library is an HPND license, which is a simple permissive license:
        # Refs:
        #   * https://github.com/python-pillow/Pillow/blob/main/LICENSE
        #   * https://www.fsf.org/blogs/licensing/historical-permission-notice-and-disclaimer-added-to-license-list

        # The IJG license, used by Independent JPEG Group (IJG) is a custom permissive license.
        # Refs:
        #   * https://en.wikipedia.org/wiki/Libjpeg
        #   * https://github.com/libjpeg-turbo/libjpeg-turbo/blob/main/LICENSE.md
        'IJG',

        # Linking = Permissive, Private Use = Permissive
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'ISC License (ISCL)',
        'ISC',

        # Linking = Permissive, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'MIT License',
        'MIT',

        # Linking = Permissive, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'Mozilla Public License 2.0 (MPL 2.0)',
        'MPL-1.1',
        'MPL-2.0',

        # The SIL Open Font License appears to be a copyleft-style license that applies narrowly
        # to icons and not to the entire codebase. It is advertised as OK for use even in commercial
        # applications.
        # Ref: https://fontawesome.com/license/free
        'OFL-1.1',

        # Ref: https://en.wikipedia.org/wiki/Public_domain
        pattern('(Licen[cs]eRef-)?Public[- ]Domain([- ]dedic[t]?ation)?'),  # "dedictation" is a typo in docutils

        # Linking = Permissive, Private Use = Permissive
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        pattern('(Licen[cs]eRef-)?PSF-2([.][.0-9]*)'),
        'Python Software Foundation License',
        'Python-2.0',

        # License = BSD-like
        # Ref: https://en.wikipedia.org/wiki/Pylons_project
        'Repoze Public License',

        # The TCL or Tcl/Tk licenses are permissive licenses.
        # Ref: https://www.tcl.tk/software/tcltk/license.html
        # The one used by the tktable library has a 'bourbon' clause that doesn't add compliance requirements
        # Ref: https://github.com/wjoye/tktable/blob/master/license.txt
        pattern('Tcl([/]tk)?'),

        # The Ubuntu Font Licence is mostly permissive. It contains some restrictions if you are going to modify the
        # fonts that require you to change the name to avoid confusion. But for our purposes, we're assuming that's
        # not done, and so we're not flagging it.
        pattern('Ubuntu Font Licen[cs]e Version( 1([.]0)?)?'),

        # Linking = Permissive/Public domain, Private Use = Permissive/Public domain
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'The Unlicense (Unlicense)',
        'Unlicense',

        # Various licenses seem to call themselves or be summed up as unlimited.
        # So far we know of none that are not highly permissive.
        #   * boot and KernSmooth are reported by R as being 'Unlimited'
        #     Refs:
        #       * https://cran.r-project.org/web/packages/KernSmooth/index.html
        #         (https://github.com/cran/KernSmooth/blob/master/LICENCE.note)
        #       * https://cran.r-project.org/package=boot
        #         (https://github.com/cran/boot/blob/master/DESCRIPTION)
        'Unlimited',

        # Linking = Permissive, Private Use = ?
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'W3C License',
        'W3C-20150513',

        # Linking = Permissive/Public Domain, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'WTFPL',

        # Copyleft = No
        # Ref: https://en.wikipedia.org/wiki/Zlib_License
        # Linking = Permissive, Private Use = ? (for zlib/libpng license)
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'Zlib',

        # Copyleft = No, FSF/OSI-approved: Yes
        # Ref: https://en.wikipedia.org/wiki/Zope_Public_License
        'Zope Public License',
    ]

    EXCEPTIONS = {

        # The Bioconductor zlibbioc license is a permissive license.
        # Ref: https://github.com/Bioconductor/zlibbioc/blob/devel/LICENSE
        'Custom: bioconductor-zlibbioc file LICENSE': [
            'bioconductor-zlibbioc'
        ],

        # The Bioconductor rsamtools license is an MIT license
        # Ref: https://bioconductor.org/packages/release/bioc/licenses/Rsamtools/LICENSE
        'Custom: bioconductor-rsamtools file LICENSE': [
            'bioconductor-rsamtools'
        ],

        # DFSG = Debian Free Software Guidelines
        # Ref: https://en.wikipedia.org/wiki/Debian_Free_Software_Guidelines
        # Used as an apparent modifier to other licenses, to say they are approved per Debian.
        # For example in this case, pytest-timeout has license: DFSG approved, MIT License,
        # but is really just an MIT License that someone has checked is DFSG approved.
        'DFSG approved': [
            'pytest-timeout',  # MIT Licensed
        ],

        'FOSS': [
            # The r-stringi library is a conda library that implements a stringi (pronounced "stringy") library for R.
            # The COnda source feed is: https://github.com/conda-forge/r-stringi-feedstock
            # This page explains that the home source is https://stringi.gagolewski.com/ but that's a doc page.
            # The doc page says:
            # > stringi’s source code is hosted on GitHub.
            # > It is distributed under the open source BSD-3-clause license.
            # The source code has a license that begins with a BSD-3-clause license and includes numerous others,
            # but they all appear to be permissive.
            #   Ref: https://github.com/gagolews/stringi/blob/master/LICENSE
            'stringi', 'r-stringi',
        ],

        # Linking = With Restrictions, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'GNU Lesser General Public License v2 or later (LGPLv2+)': [
            'chardet'  # used at runtime during server operation (ingestion), but not modified or distributed
        ],

        # Linking = With Restrictions, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'GNU Lesser General Public License v3 or later (LGPLv3+)': [
            # used only privately in testing, not used in server code, not modified, not distributed
            'pytest-redis',
            # required by pytest-redis (used only where it's used)
            'mirakuru',
        ],

        'GNU General Public License (GPL)': [
            'docutils',  # Used only privately as a separate documentation-generation task for ReadTheDocs
        ],

        'MIT/X11 Derivative': [
            # The license used by libxkbcommon is complicated and involves numerous included licenses,
            # but all are permissive.
            # Ref: https://github.com/xkbcommon/libxkbcommon/blob/master/LICENSE
            'libxkbcommon',
        ],

        'None': [
            # It's not obvious why Conda shows this license as 'None'.
            # In fact, though, BSD 3-Clause "New" or "Revised" License
            # Ref: https://github.com/AnacondaRecipes/_libgcc_mutex-feedstock/blob/master/LICENSE.txt
            '_libgcc_mutex',
        ],

        'PostgreSQL': [
            # The libpq library is actually licensed with a permissive BSD 3-Clause "New" or "Revised" License
            # Ref: https://github.com/lpsmith/postgresql-libpq/blob/master/LICENSE
            'libpq',
        ],

        'UCSD': [
            # It isn't obvious why these show up with a UCSD license in Conda.
            # The actual sources say it should be a 2-clause BSD license:
            # Refs:
            #   * https://github.com/AlexandrovLab/SigProfilerMatrixGenerator/blob/master/LICENSE
            #   * https://github.com/AlexandrovLab/SigProfilerPlotting/blob/master/LICENSE
            'sigprofilermatrixgenerator',
            'sigprofilerplotting',
        ],

        'X11': [
            # The ncurses library has a VERY complicated history, BUT seems consistently permissive
            # and the most recent version seems to be essentially the MIT license.
            # Refs:
            #   * https://en.wikipedia.org/wiki/Ncurses#License
            #   * https://invisible-island.net/ncurses/ncurses-license.html
            'ncurses'
        ],

        'zlib-acknowledgement': [
            # It isn't clear whey libpng shows up with this license name, but the license for libpng
            # is a permissive license.
            # Ref: https://github.com/glennrp/libpng/blob/libpng16/LICENSE
            'libpng',
        ],

    }

    EXPECTED_MISSING_LICENSES = [

        # This is a name we use for our C4 portals. And it isn't published.
        # We inherited the name from the Stanford ENCODE group, which had an MIT-licensed repo we forked
        'encoded',  # cgap-portal, fourfront, and smaht-portal all call themselves this

        # We believe that since these next here are part of the Pylons project, they're covered under
        # the same license as the other Pylons projects. We're seeking clarification.
        'pyramid-translogger',
        'subprocess-middleware',

        # This appears to be a BSD 2-Clause "Simplified" License, according to GitHub.
        # PyPi also says it's a BSD license.
        # Ref: https://github.com/paulc/dnslib/blob/master/LICENSE
        'dnslib',

        # This says it wants an ISC License, which we already have approval for but just isn't showing up.
        # Ref: https://github.com/rthalley/dnspython/blob/master/LICENSE
        'dnspython',

        # This appears to be a mostly-MIT-style license.
        # There are references to parts being in the public domain, though it's not obvious if that's meaningful.
        # It's probably sufficient for our purposes to treat this as a permissive license.
        # Ref: https://github.com/tlsfuzzer/python-ecdsa/blob/master/LICENSE
        'ecdsa',

        # This has an MIT license in its source repository
        # Ref: https://github.com/xlwings/jsondiff/blob/master/LICENSE
        'jsondiff',

        # This has an MIT license in its source repository
        # Ref: https://github.com/pkerpedjiev/negspy/blob/master/LICENSE
        'negspy',

        # This license statement is complicated, but seems adequately permissive.
        # Ref: https://foss.heptapod.net/python-libs/passlib/-/blob/branch/stable/LICENSE
        'passlib',

        # This seems to be a BSD-3-Clause license.
        # Ref: https://github.com/protocolbuffers/protobuf/blob/main/LICENSE
        # pypi agrees in the Meta section of protobuf's page, where it says "3-Clause BSD License"
        # Ref: https://pypi.org/project/protobuf/
        'protobuf',

        # The WTFPL license is permissive.
        # Ref: https://github.com/mk-fg/pretty-yaml/blob/master/COPYING
        'pyaml',

        # This uses a BSD license
        # Ref: https://github.com/eliben/pycparser/blob/master/LICENSE
        'pycparser',

        # The source repo for pyDes says this is under an MIT license
        # Ref: https://github.com/twhiteman/pyDes/blob/master/LICENSE.txt
        # pypi, probably wrongly, thinks this is in the public domain (as of 2023-07-21)
        # Ref: https://pypi.org/project/pyDes/
        'pyDes',

        # This uses an MIT license
        # Ref: https://github.com/pysam-developers/pysam/blob/master/COPYING
        'pysam',

        # The version of python-lambda that we forked calls itself this (and publishes at pypi under this name)
        "python-lambda-4dn",

        # This is MIT-licensed:
        # Ref: https://github.com/themiurgo/ratelim/blob/master/LICENSE
        # pypi agrees
        # Ref: https://pypi.org/project/ratelim/
        'ratelim',

        # This is a BSD-3-Clause-Modification license
        # Ref: https://github.com/repoze/repoze.debug/blob/master/LICENSE.txt
        'repoze.debug',

        # This is an Apache-2.0 license
        # Ref: https://github.com/getsentry/responses/blob/master/LICENSE
        'responses',

        # This seems to get flagged sometimes, but is not the pypi snovault library, it's what our dcicsnovault
        # calls itself internally. In any case, it's under MIT license and OK.
        # Ref: https://github.com/4dn-dcic/snovault/blob/master/LICENSE.txt
        'snovault',

        # PyPi identifies the supervisor library license as "BSD-derived (http://www.repoze.org/LICENSE.txt)"
        # Ref: https://pypi.org/project/supervisor/
        # In fact, though, the license is a bit more complicated, though apparently still permissive.
        # Ref: https://github.com/Supervisor/supervisor/blob/main/LICENSES.txt
        'supervisor',

        # This seems to be a BSD-3-Clause-Modification license.
        # Ref: https://github.com/Pylons/translationstring/blob/master/LICENSE.txt
        'translationstring',

        # This seems to be a BSD-3-Clause-Modification license.
        # Ref: https://github.com/Pylons/venusian/blob/master/LICENSE.txt
        'venusian',

        # PyPi identifies zope.deprecation as using the "Zope Public License (ZPL 2.1)" license.
        # Ref: https://github.com/zopefoundation/Zope/blob/master/LICENSE.txt
        'zope.deprecation',

        # Below are licenses last known to have licenses missing in pip-licenses and need to be investigated further.
        # Note well that just because pip-licenses doesn't know the license doesn't mean the software has
        # no license. It may just mean the library is poorly registered in pypi. Some licenses have to be
        # found by looking at the library's documentation or source files.

        # (all of these have been classified at this point)

    ]


@LicenseCheckerRegistry.register_checker('park-lab-pipeline')
class ParkLabPipelineLicenseChecker(ParkLabCommonLicenseChecker):
    """
    Minimal checker common to pipelines from Park Lab.
    """

    LICENSE_FRAMEWORKS = ['python', 'conda', 'r']


@LicenseCheckerRegistry.register_checker('park-lab-gpl-pipeline')
class ParkLabGplPipelineLicenseChecker(ParkLabCommonLicenseChecker):
    """
    Minimal checker common to GPL pipelines from Park Lab.
    """

    ALLOWED = ParkLabPipelineLicenseChecker.ALLOWED + [

        # Linking = With Restrictions, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        # The "exceptions", if present, indicate waivers to source delivery requirements.
        # Ref: https://spdx.org/licenses/LGPL-3.0-linking-exception.html
        pattern('GNU Lesser General Public License v2( or later)?( [(]LGPL[v]?[23][+]?[)])?'),
        # 'GNU Lesser General Public License v2 or later (LGPLv2+)',
        # 'GNU Lesser General Public License v3 or later (LGPLv3+)',
        # 'LGPLv2', 'LGPL-v2', 'LGPL-v2.0', 'LGPL-2', 'LGPL-2.0',
        # 'LGPLv2+', 'LGPL-v2+', 'LGPL-v2.0+', 'LGPL-2+', 'LGPL-2.0+',
        # 'LGPLv3', 'LGPL-v3', 'LGPL-v3.0', 'LGPL-3', 'LGPL-3.0',
        # 'LGPLv3+', 'LGPL-v3+', 'LGPL-v3.0+', 'LGPL-3+', 'LGPL-3.0+',
        pattern('LGPL[v-]?[.0-9]*([+]|-only)?([- ]with[- ]exceptions)?'),

        # Uncertain whether this is LGPL 2 or 3, but in any case we think weak copyleft should be OK
        # for pipeline or server use as long as we're not distributing sources.
        'LGPL',
        'GNU Library or Lesser General Public License (LGPL)',

        # GPL
        #  * library exception operates like LGPL
        #  * classpath exception is a linking exception related to Oracle
        # Refs:
        #   * https://www.gnu.org/licenses/old-licenses/gpl-1.0.en.html
        #   * https://spdx.org/licenses/GPL-2.0-with-GCC-exception.html
        #   * https://spdx.org/licenses/GPL-3.0-with-GCC-exception.html
        pattern('(GNU General Public License|GPL)[ ]?[v-]?[123]([.]0)?([+]|[- ]only)?'
                '([- ]with[- ]GCC(([- ]runtime)?[- ]library)?[- ]exception([- ][.0-9]*)?)?'
                '([- ]with[- ]Classpath[- ]exception([- ][.0-9]+)?)?'),

        # Linking = "GPLv3 compatible only", Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'GPL-2-or-3',  # we sometimes generate this token
        # 'GPLv2+', 'GPL-v2+', 'GPL-v2.0+', 'GPL-2+', 'GPL-2.0+',
        # 'GPLv3', 'GPL-v3', 'GPL-v3.0', 'GPL-3', 'GPL-3.0',
        # 'GPLv3+', 'GPL-v3+', 'GPL-v3.0+', 'GPL-3+', 'GPL-3.0+',
        # 'GPLv3-only', 'GPL-3-only', 'GPL-v3-only', 'GPL-3.0-only', 'GPL-v3.0-only',

        # Uncertain whether this is GPL 2 or 3, but we'll assume that means we can use either.
        # And version 3 is our preferred interpretation.
        'GNU General Public License',
        'GPL',

        RLicenseFramework.R_LANGUAGE_LICENSE_NAME

    ]


@LicenseCheckerRegistry.register_checker('park-lab-common-server')
class ParkLabCommonServerLicenseChecker(ParkLabCommonLicenseChecker):
    """
    Checker for servers from Park Lab.

    If you're at some other organization, we recommend you make a class that has values
    suitable to your own organizational needs.
    """

    LICENSE_FRAMEWORKS = ['python', 'javascript']

    EXCEPTIONS = augment(
        ParkLabCommonLicenseChecker.EXCEPTIONS,
        by={
            'BSD*': [
                # Although modified to insert the author name into the license text itself,
                # the license for these libraries are essentially BSD-3-Clause.
                'formatio',
                'samsam',

                # There are some slightly different versions of what appear to be BSD licenses here,
                # but clearly the license is permissive.
                # Ref: https://www.npmjs.com/package/mutation-observer?activeTab=readme
                'mutation-observer',
            ],

            'Custom: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global': [
                # The use of this URL appears to be a syntax error in the definition of entries-ponyfill
                # In fact this seems to be covered by a CC0-1.0 license.
                # Ref: https://unpkg.com/browse/object.entries-ponyfill@1.0.1/LICENSE
                'object.entries-ponyfill',
            ],

            'Custom: https://github.com/saikocat/colorbrewer.': [
                # The use of this URL appears to be a syntax error in the definition of cartocolor
                # In fact, this seems to be covered by a CC-BY-3.0 license.
                # Ref: https://www.npmjs.com/package/cartocolor?activeTab=readme
                'cartocolor',
            ],

            'Custom: https://travis-ci.org/component/emitter.png': [
                # The use of this png appears to be a syntax error in the definition of emitter-component.
                # In fact, emitter-component uses an MIT License
                # Ref: https://www.npmjs.com/package/emitter-component
                # Ref: https://github.com/component/emitter/blob/master/LICENSE
                'emitter-component',
            ],

            # The 'turfs-jsts' repository (https://github.com/DenisCarriere/turf-jsts/blob/master/README.md)
            # seems to lack a license, but appears to be forked from the jsts library that uses
            # the Eclipse Public License 1.0 and Eclipse Distribution License 1.0, so probably a permissive
            # license is intended.
            'Custom: https://travis-ci.org/DenisCarriere/turf-jsts.svg': [
                'turf-jsts'
            ],

            'GNU General Public License (GPL)': [
                'docutils',  # Used only privately as a separate documentation-generation task for ReadTheDocs
            ],

            # Linking = With Restrictions, Private Use = Yes
            # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
            # 'GNU Lesser General Public License v3 or later (LGPLv3+)',

            # Linking = With Restrictions, Private Use = Yes
            # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
            'GNU Library or Lesser General Public License (LGPL)': [
                'psycopg2',  # Used at runtime during server operation, but not modified or distributed
                'psycopg2-binary',  # Used at runtime during server operation, but not modified or distributed
                'chardet',  # Potentially used downstream in loadxl to detect charset for text files
                'pyzmq',  # Used in post-deploy-perf-tests, not distributed, and not modified or distributed
            ],

            'GPL-2.0': [
                # The license file for the node-forge javascript library says:
                #
                #   "You may use the Forge project under the terms of either the BSD License or the
                #   GNU General Public License (GPL) Version 2."
                #
                # (We choose to use it under the BSD license.)
                # Ref: https://www.npmjs.com/package/node-forge?activeTab=code
                'node-forge',
            ],

            'MIT*': [

                # This library uses a mix of licenses, but they (MIT, CC0) generally seem permissive.
                # (It also mentions that some tools for building/testing use other libraries.)
                # Ref: https://github.com/requirejs/domReady/blob/master/LICENSE
                'domready',

                # This library is under 'COMMON DEVELOPMENT AND DISTRIBUTION LICENSE (CDDL) Version 1.1'
                # Ref: https://github.com/javaee/jsonp/blob/master/LICENSE.txt
                # About CDDL ...
                # Linking = Permissive, Private Use = ?
                # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
                'jsonp',

                # This library says pretty clearly it intends MIT license.
                # Ref: https://www.npmjs.com/package/component-indexof
                # Linking = Permissive, Private Use = Yes
                # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
                'component-indexof',

                # These look like a pretty straight MIT license.
                # Linking = Permissive, Private Use = Yes
                # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
                'mixin',        # LICENSE file at https://www.npmjs.com/package/mixin?activeTab=code
                'stack-trace',  # https://github.com/stacktracejs/stacktrace.js/blob/master/LICENSE
                'typed-function',  # LICENSE at https://www.npmjs.com/package/typed-function?activeTab=code

            ],

            'UNLICENSED': [
                # The udn-browser library is our own and has been observed to sometimes show up in some contexts
                # as UNLICENSED, when really it's MIT.
                # Ref: https://github.com/dbmi-bgm/udn-browser/blob/main/LICENSE
                'udn-browser',
            ],
        })


@LicenseCheckerRegistry.register_checker('c4-infrastructure')
class C4InfrastructureLicenseChecker(ParkLabCommonServerLicenseChecker):
    """
    Checker for C4 infrastructure (Fourfront, CGAP, SMaHT) from Park Lab.
    """

    LICENSE_TITLE = "(The )?MIT License"


@LicenseCheckerRegistry.register_checker('c4-python-infrastructure')
class C4PythonInfrastructureLicenseChecker(C4InfrastructureLicenseChecker):
    """
    Checker for C4 python library infrastructure (Fourfront, CGAP, SMaHT) from Park Lab.
    """
    LICENSE_FRAMEWORKS = ['python']


@LicenseCheckerRegistry.register_checker('scan2-pipeline')
class Scan2PipelineLicenseChecker(ParkLabGplPipelineLicenseChecker):
    """
    Checker for SCAN2 library from Park Lab.
    """

    EXCEPTIONS = augment(
        ParkLabGplPipelineLicenseChecker.EXCEPTIONS,
        by={
            'Custom: Matrix file LICENCE': [
                # The custom information in https://cran.r-project.org/web/packages/Matrix/LICENCE
                # says there are potential extra restrictions beyond a simple GPL license
                # if SparseSuite is used, but it is not requested explicitly by Scan2, and we're
                # trusting that any other libraries used by Scan2 would have investigated this.
                # So, effectively, we think the Matrix library for this situation operates the
                # same as if it were just GPL-3 licensed, and we are fine with that.
                'Matrix'
            ],

            "MISSING": [
                # mysql-common and mysql-libs are GPL, but since they are delivered by conda
                # and not distributed as part of the Scan2 distribution, they should be OK.
                # Ref: https://redresscompliance.com/mysql-license-a-complete-guide-to-licensing/#:~:text=commercial%20use  # noQA
                'mysql-common',
                'mysql-libs',

                # This is our own library
                'r-scan2', 'scan2',
            ]
        }
    )

    EXPECTED_MISSING_LICENSES = ParkLabGplPipelineLicenseChecker.EXPECTED_MISSING_LICENSES + [

    ]
