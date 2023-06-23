import logging

try:
    import piplicenses
except ImportError:
    raise Exception("The dcicutils.license_utils module is intended for use at development time, not runtime."
                    " It does not export a requirement for the pip-licenses library,"
                    " but to use this in your unit tests, you are expected to assure a dev dependency on that library"
                    " as part of the [tool.poetry.dependencies] section of your pyproject.toml file."
                    " If you are trying to manually evaluate the utility of this library, you can"
                    " do 'pip install pip-licenses' and then retry importing this library.")
# or you can comment out the above raise of Exception and instead execute:
#
#    import subprocess
#    subprocess.check_output('pip install pip-licenses'.split(' '))
#    import piplicenses

from collections import defaultdict
from typing import Dict, List, Optional

# For obscure reasons related to how this file is used for early prototyping, these must use absolute references
# to modules, not relative references. Later when things are better installed, we can make refs relative again.
from dcicutils.lang_utils import there_are
from dcicutils.misc_utils import PRINT


logging.basicConfig()
logger = logging.getLogger(__name__)

_NAME = 'name'
_LICENSE_CLASSIFIER = 'license_classifier'


class LicenseStatus:
    ALLOWED = "ALLOWED"
    SPECIALLY_ALLOWED = "SPECIALLY_ALLOWED"
    FAILED = "FAILED"
    EXPECTED_MISSING = "EXPECTED_MISSING"
    UNEXPECTED_MISSING = "UNEXPECTED_MISSING"


class LicenseAnalysis:

    def __init__(self, details=None, unacceptable=None, unexpected_missing=None, no_longer_missing=None):
        self.details = details or []
        self.unacceptable: Dict[str, List[str]] = unacceptable or {}
        self.unexpected_missing = unexpected_missing or []
        self.no_longer_missing = no_longer_missing or []


class LicenseChecker:
    """
    There are three important class variables to specify:

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
    SPDX naming conventions, you can customize the get_licenses method to return a different list of the form
    [{"name": "libname", "license_classifier": ["license1", "license2", ...], "language": "python"}]
    by whatever means you like and using whatever names you like.
    """

    # Set this to True in subclasses if you want your organization's policy to be that you see
    # some visible proof of which licenses were checked.
    VERBOSE = True

    EXPECTED_MISSING_LICENSES = []

    ALLOWED: List[str] = []

    EXCEPTIONS: Dict[str, str] = {}

    @classmethod
    def _piplicenses_args(cls, _options: Optional[List[str]] = None):
        parser = piplicenses.create_parser()
        args = parser.parse_args(_options or [])
        return args

    @classmethod
    def get_licenses(cls, keys=None, _options: Optional[List[str]] = None):
        keys = keys or [_NAME, _LICENSE_CLASSIFIER]
        args = cls._piplicenses_args(_options)
        result = []
        for entry in piplicenses.get_packages(args):
            entry = {key: entry.get(key) for key in keys}
            # All licenses found by piplicenses are Python,
            # but we might want to later support lookup of javascript licenses. -kmp 23-Jun-2023
            entry['language'] = 'python'  # pip licenses are all python, but maybe extend to javascript later, too
            result.append(entry)
        return sorted(result, key=lambda x: x.get(_NAME).lower())

    @classmethod
    def get_license_analysis(cls, acceptable: Optional[List[str]] = None,
                             exceptions: Optional[Dict[str, str]] = None,
                             ) -> LicenseAnalysis:
        acceptable = (acceptable or []) + (cls.ALLOWED or [])
        exceptions = dict(cls.EXCEPTIONS or {}, **(exceptions or {}))
        unacceptable = defaultdict(lambda: [])
        details = []
        expected_missing_licenses = cls.EXPECTED_MISSING_LICENSES
        unexpected_missing_licenses = []
        no_longer_missing_licenses = []
        for entry in cls.get_licenses(keys=[_NAME, _LICENSE_CLASSIFIER]):
            name = entry[_NAME]
            classifiers = entry[_LICENSE_CLASSIFIER]
            if not classifiers:
                if name in expected_missing_licenses:
                    status = LicenseStatus.EXPECTED_MISSING
                else:
                    status = LicenseStatus.UNEXPECTED_MISSING
                    unexpected_missing_licenses.append(name)
            else:
                if name in expected_missing_licenses:
                    no_longer_missing_licenses.append(name)
                status = LicenseStatus.ALLOWED
                by_special_exception = False
                for classifier in classifiers:
                    special_exceptions = exceptions.get(classifier, [])
                    if classifier in acceptable:
                        pass
                    elif name in special_exceptions:
                        by_special_exception = True
                    else:
                        status = LicenseStatus.FAILED
                        unacceptable[classifier].append(name)
                if status == LicenseStatus.ALLOWED and by_special_exception:
                    status = LicenseStatus.SPECIALLY_ALLOWED
            details = {'name': name, 'classifiers': classifiers, 'status': status}
            if cls.VERBOSE:
                PRINT(f"Checked {name}: {'; '.join(classifiers) if classifiers else '---'} ({status})")
        unacceptable: Dict[str, List[str]] = dict(unacceptable)
        analysis = LicenseAnalysis(details=details, unacceptable=unacceptable,
                                   unexpected_missing=unexpected_missing_licenses,
                                   no_longer_missing=no_longer_missing_licenses)
        return analysis

    @classmethod
    def show_unacceptable_licenses(cls, acceptable: Optional[List[str]] = None,
                                   exceptions: Optional[Dict[str, str]] = None,
                                   ) -> LicenseAnalysis:
        analysis = cls.get_license_analysis(acceptable=acceptable, exceptions=exceptions)
        if analysis.unacceptable:
            PRINT(there_are(analysis.unacceptable, kind="unacceptable license", show=False, punctuation_mark=':'))
            for classifier, names in analysis.unacceptable.items():
                PRINT(f" {classifier}: {', '.join(names)}")
        return analysis

    @classmethod
    def validate(cls) -> None:
        """
        This method is intended to be used in a unit test, as in:

            from my_org_tools import MyOrgLicenseChecker
            def test_license_compatibility():
                MyOrgLicenseChecker.validate()

        where my_org_tools has done something like:

            from dcicutils.license_utils import LicenseChecker
            class MyOrgLicenseChecker(LicenseChecker):
                ALLOWED = [...]
                EXPECTED_MISSING_LICENSES = [...]
                EXCEPTIONS = {...}

        See the example of C4InfrastructureLicenseChecker we use in our own group for our own family of toools,
        which we sometimes informally refer to collectively as 'C4'.
        """
        analysis = cls.show_unacceptable_licenses()
        if analysis.unexpected_missing:
            logger.warning(there_are(analysis.unexpected_missing, kind='unexpectedly missing license', punctuate=True))
        if analysis.no_longer_missing:
            logger.warning(there_are(analysis.no_longer_missing, kind='no-longer-missing license', punctuate=True))
        if analysis.unacceptable:
            raise UnacceptableLicenseFailure(unacceptable_licenses=analysis.unacceptable)


class UnacceptableLicenseFailure(Exception):

    def __init__(self, message=None, unacceptable_licenses=None):
        self.unacceptable_licenses = unacceptable_licenses
        if not message:
            if unacceptable_licenses:
                message = there_are(unacceptable_licenses, kind='unacceptable license')
            else:
                message = "One or more licenses are unacceptable."
        super().__init__(message)


class C4InfrastructureLicenseChecker(LicenseChecker):
    """
    This set of values is useful to us in Park Lab where these tools were developed.
    If you're at some other organization, we recommend you make a class that has values
    suitable to your own organizational needs.
    """

    ALLOWED = [

        # Linking = Permissive, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'Academic Free License (AFL)',

        # Linking = Permissive, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'Apache Software License',

        # Linking = Permissive, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'BSD License',

        # Linking = Yes, Cat = Permissive Software Licenses
        # Ref: https://en.wikipedia.org/wiki/Historical_Permission_Notice_and_Disclaimer
        'Historical Permission Notice and Disclaimer (HPND)',

        # Linking = Permissive, Private Use = Permissive
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'ISC License (ISCL)',                                  # [1]

        # Linking = Permissive, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'MIT License',                                         # [1]

        # Linking = Permissive, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'Mozilla Public License 2.0 (MPL 2.0)',

        # Ref: https://en.wikipedia.org/wiki/Public_domain
        'Public Domain',

        # Linking = Permissive, Private Use = Permissive
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'Python Software Foundation License',

        # License = BSD-like
        # Ref: https://en.wikipedia.org/wiki/Pylons_project
        'Repoze Public License',

        # Linking = Permissive/Public domain, Private Use = Permissive/Public domain
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'The Unlicense (Unlicense)',

        # Linking = Permissive, Private Use = ?
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'W3C License',

        # Copyleft = No, FSF/OSI-approved: Yes
        # Ref: https://en.wikipedia.org/wiki/Zope_Public_License
        'Zope Public License',
    ]

    EXPECTED_MISSING_LICENSES = [

        # This is a name we use for our C4 portals. And it isn't published.
        # We inherited the name from the Stanford ENCODE group, which had an MIT-licensed repo we forked
        'encoded',

        # We believe that since these next here are part of the Pylons project, they're covered under
        # the same license as the other Pylons projects. We're seeking clarification.
        'pyramid-translogger',
        'subprocess-middleware',

        # This appears to be a mostly-MIT-style license.
        # There are references to parts being in the public domain, though it's not obvious if that's meaningful.
        # It's probably sufficient for our purposes to treat this as a permissive license.
        # Ref: https://github.com/tlsfuzzer/python-ecdsa/blob/master/LICENSE
        'ecdsa',

        # This has an MIT license in its source repository
        # Ref: https://github.com/xlwings/jsondiff/blob/master/LICENSE
        'jsondiff',

        # This license statement is complicated, but seems adequately permissive.
        # Ref: https://foss.heptapod.net/python-libs/passlib/-/blob/branch/stable/LICENSE
        'passlib',

        # The WTFPL license is permissive.
        # Ref: https://github.com/mk-fg/pretty-yaml/blob/master/COPYING
        'pyaml',

        # This is a BSD-3-Clause-Modification license
        # Ref: https://github.com/repoze/repoze.debug/blob/master/LICENSE.txt
        'repoze.debug',

        # This is an Apache-2.0 license
        # Ref: https://github.com/getsentry/responses/blob/master/LICENSE
        'responses',

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

    EXCEPTIONS = {

        # Linking = With Restrictions, Private Use = Yes
        # Ref: https://en.wikipedia.org/wiki/Comparison_of_free_and_open-source_software_licenses
        'GNU Lesser General Public License v3 or later (LGPLv3+)': [
            'pytest-redis',  # used only privately in testing, not used in server code, not modified, not distributed
            'mirakuru',      # required by pytest-redis (used only where it's used)
        ],

        # DFSG = Debian Free Software Guidelines
        # Ref: https://en.wikipedia.org/wiki/Debian_Free_Software_Guidelines
        # Used as an apparent modifier to other licenses, to say they are approved per Debian.
        # For example in this case, pytest-timeout has license: DFSG approved, MIT License,
        # but is really just an MIT License that someone has checked is DFSG approved.
        'DFSG approved': [
            'pytest-timeout',  # MIT Licensed
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
            'psycopg2-binary',  # Used at runtime during server operation, but not modified or distributed
            'chardet',  # Potentially used downstream in loadxl to detect charset for text files
        ],

    }
