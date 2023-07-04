import copy
import datetime
import io
import json
import os
import pytest
import subprocess as subprocess_module

from collections import defaultdict
from dcicutils.license_utils import (
    LicenseFrameworkRegistry, LicenseFramework, PythonLicenseFramework, JavascriptLicenseFramework,
    LicenseAnalysis, LicenseChecker, LicenseStatus, LicenseFileParser,
    LicenseCheckFailure, LicenseOwnershipCheckFailure, LicenseAcceptabilityCheckFailure,
    logger as license_logger,
)
from dcicutils.misc_utils import ignored, file_contents
from dcicutils.qa_utils import printed_output, MockFileSystem
from unittest import mock


def test_license_check_failure():  # error class

    assert issubclass(LicenseCheckFailure, Exception)
    assert isinstance(LicenseCheckFailure.DEFAULT_MESSAGE, str)

    x = LicenseCheckFailure()

    assert str(x) == LicenseCheckFailure.DEFAULT_MESSAGE

    alternate_error_message = "Hey! " + LicenseCheckFailure.DEFAULT_MESSAGE  # different from default

    x = LicenseCheckFailure(message=alternate_error_message)

    assert str(x) == alternate_error_message
    assert str(x) != LicenseCheckFailure.DEFAULT_MESSAGE


def test_license_ownership_check_failure():  # error class

    assert issubclass(LicenseOwnershipCheckFailure, LicenseCheckFailure)
    assert LicenseOwnershipCheckFailure.DEFAULT_MESSAGE != LicenseCheckFailure.DEFAULT_MESSAGE

    x = LicenseOwnershipCheckFailure()

    assert str(x) == LicenseOwnershipCheckFailure.DEFAULT_MESSAGE

    alternate_error_message = "Hey! " + LicenseOwnershipCheckFailure.DEFAULT_MESSAGE  # different from default

    x = LicenseOwnershipCheckFailure(message=alternate_error_message)

    assert str(x) == alternate_error_message
    assert str(x) != LicenseOwnershipCheckFailure.DEFAULT_MESSAGE


def test_license_acceptability_check_failure():  # error class

    assert issubclass(LicenseAcceptabilityCheckFailure, LicenseCheckFailure)
    assert LicenseAcceptabilityCheckFailure.DEFAULT_MESSAGE != LicenseCheckFailure.DEFAULT_MESSAGE

    x = LicenseAcceptabilityCheckFailure()

    assert str(x) == LicenseAcceptabilityCheckFailure.DEFAULT_MESSAGE

    x = LicenseAcceptabilityCheckFailure(unacceptable_licenses=[])

    assert str(x) == LicenseAcceptabilityCheckFailure.DEFAULT_MESSAGE

    alternate_error_message = "Hey! " + LicenseAcceptabilityCheckFailure.DEFAULT_MESSAGE  # different from default

    x = LicenseAcceptabilityCheckFailure(message=alternate_error_message)

    assert str(x) == alternate_error_message
    assert str(x) != LicenseAcceptabilityCheckFailure.DEFAULT_MESSAGE

    x = LicenseAcceptabilityCheckFailure(unacceptable_licenses=['license1'])

    assert str(x) == "There is 1 unacceptable license: license1"

    x = LicenseAcceptabilityCheckFailure(unacceptable_licenses=['license1'])

    assert str(x) == "There is 1 unacceptable license: license1"

    x = LicenseAcceptabilityCheckFailure(unacceptable_licenses=['license1', 'license2'])

    assert str(x) == "There are 2 unacceptable licenses: license1, license2"

    x = LicenseAcceptabilityCheckFailure(
        unacceptable_licenses={
            'license1': ['library1'],
            'license2': ['library2', 'library3']
        }
    )

    assert str(x) == "There are 2 unacceptable licenses: license1, license2"


def test_license_status():  # index of status keywords

    for attr in dir(LicenseStatus):
        if attr.isupper() and not attr.startswith('_'):
            assert getattr(LicenseStatus, attr) == attr


def test_license_analysis():  # represents result of an analysis

    analysis = LicenseAnalysis()

    assert analysis.frameworks == []

    assert analysis.dependency_details == []

    assert analysis.unacceptable == {}
    assert isinstance(analysis.unacceptable, defaultdict)
    analysis.unacceptable['some_license'].append('some_library')
    assert analysis.unacceptable == {'some_license': ['some_library']}

    assert analysis.unexpected_missing == []

    assert analysis.no_longer_missing == []

    assert analysis.miscellaneous == []


def test_license_framework():  # class

    assert LicenseFramework.NAME is None

    with pytest.raises(NotImplementedError):
        LicenseFramework.get_dependencies()


def test_license_framework_registry_register():  # decorator

    with pytest.raises(ValueError):
        @LicenseFrameworkRegistry.register(name='bogus_dummy')
        class BogusDummyLicenseFramework:
            pass
        ignored(BogusDummyLicenseFramework)

    try:
        @LicenseFrameworkRegistry.register(name='dummy')
        class DummyLicenseFramework(LicenseFramework):
            pass

        dummy_framework = LicenseFrameworkRegistry.LICENSE_FRAMEWORKS.get('dummy')
        assert issubclass(dummy_framework, LicenseFramework)
        assert dummy_framework is DummyLicenseFramework

    finally:

        # Clean up the mess we made...
        for name in ['bogus_dummy', 'dummy']:
            LicenseFrameworkRegistry.LICENSE_FRAMEWORKS.pop(name, None)


def test_license_framework_registry_all_frameworks():

    frameworks = LicenseFrameworkRegistry.all_frameworks()

    assert isinstance(frameworks, list)

    assert all(isinstance(framework, type) and issubclass(framework, LicenseFramework) for framework in frameworks)

    assert sorted(frameworks, key=lambda x: x.NAME) == [
        JavascriptLicenseFramework,
        PythonLicenseFramework,
    ]


def test_license_framework_registry_find_framework():

    try:
        @LicenseFrameworkRegistry.register(name='dummy1')
        class DummyLicenseFramework1(LicenseFramework):
            pass

        assert LicenseFrameworkRegistry.find_framework(DummyLicenseFramework1) == DummyLicenseFramework1

        # These only have class methods by default, so there's little point in instantiating them,
        # but it's harmless, and we should accept it. -kmp 3-Jul-2023
        dummy1_instance = DummyLicenseFramework1()
        assert LicenseFrameworkRegistry.find_framework(dummy1_instance) == dummy1_instance
        assert isinstance(dummy1_instance, DummyLicenseFramework1)

        assert LicenseFrameworkRegistry.find_framework('dummy1') == DummyLicenseFramework1

        with pytest.raises(ValueError):
            LicenseFrameworkRegistry.find_framework(1)  # noQA - arg is intentionally of wrong type for testing

    finally:

        # Clean up the mess we made...
        for name in list(LicenseFrameworkRegistry.LICENSE_FRAMEWORKS.keys()):
            if name.startswith('dummy'):
                LicenseFrameworkRegistry.LICENSE_FRAMEWORKS.pop(name, None)


def test_javascript_license_framework_implicated_licenses():

    def check_implications(spec, implications):
        assert JavascriptLicenseFramework.implicated_licenses(licenses_spec=spec) == implications

    check_implications(spec='(MIT AND BSD-3-Clause)', implications=['BSD-3-Clause', 'MIT'])
    check_implications(spec='(CC-BY-4.0 AND OFL-1.1 AND MIT)', implications=['CC-BY-4.0', 'MIT', 'OFL-1.1'])

    check_implications(spec='(MIT OR Apache-2.0)', implications=['Apache-2.0', 'MIT'])

    check_implications(spec='(FOO OR (BAR AND BAZ))', implications=['BAR', 'BAZ', 'FOO'])


def test_javascript_license_framework_get_licenses():

    print()  # start on a fresh line
    packages = {}
    for i, license in enumerate(['Apache-2.0', 'MIT', '(MIT OR Apache-2.0)', ''], start=1):
        package = f'package{i}'
        packages[f"package{i}"] = {
            "licenses": license,
            "repository": f"https://github.com/dummy/{package}",
            "publisher": f"J Dummy{i}",
            "email": f"jdummy{i}@dummyhost.example.com",
            "path": f"/some/path/to/package{i}",
            "licenseFile": f"/some/path/to/package{i}/license"
        }
    subprocess_output = json.dumps(packages)
    with mock.patch.object(subprocess_module, "check_output") as mock_check_output:
        mock_check_output.return_value = subprocess_output
        with printed_output() as printed:
            assert JavascriptLicenseFramework.get_dependencies() == [
                {'licenses': ['Apache-2.0'], 'name': 'package1'},
                {'licenses': ['MIT'], 'name': 'package2'},
                {'licenses': ['Apache-2.0', 'MIT'], 'name': 'package3'},
                {'licenses': [], 'name': 'package4'},
            ]
            assert printed.lines == [
                "Rewriting '(MIT OR Apache-2.0)' as ['Apache-2.0', 'MIT']"
            ]

        # A special case for missing data...
        mock_check_output.return_value = "{}\n\n"
        with pytest.raises(Exception) as esc:
            # When no package data is available, {} gets returned, and we need to complain this is odd.
            JavascriptLicenseFramework.get_dependencies()
        assert str(esc.value) == "No javascript license data was found."


def test_python_license_framework_piplicenses_args():

    default_args = PythonLicenseFramework._piplicenses_args()

    assert default_args.order
    with pytest.raises(Exception):
        print(default_args.no_such_arg)


def test_python_license_framework_get_dependencies():

    licenses = PythonLicenseFramework.get_dependencies()

    assert licenses  # make ure it found some

    for entry in licenses:
        assert 'name' in entry
        assert 'licenses' in entry


def test_analyze_license_dependencies_for_framework_python():

    analysis = LicenseAnalysis()

    assert not analysis.dependency_details

    LicenseChecker.analyze_license_dependencies_for_framework(analysis=analysis, framework=PythonLicenseFramework)

    assert analysis.dependency_details


def test_license_checker_show_unacceptable_licenses():

    print()  # start on a fresh line

    with mock.patch.object(PythonLicenseFramework, "get_dependencies") as mock_get_dependencies:

        mock_get_dependencies.return_value = [
            {'name': 'something', 'licenses': ['Foo License']}
        ]

        with printed_output() as printed:

            analysis = LicenseAnalysis()

            LicenseChecker.analyze_license_dependencies_for_framework(analysis=analysis,
                                                                      framework=PythonLicenseFramework)

            LicenseChecker.show_unacceptable_licenses(analysis=analysis)

            assert printed.lines == [
                'Checked python something: Foo License (FAILED)',
                'There is 1 unacceptable license:',
                ' Foo License: something',
            ]


def check_license_checker_full_scenario_failing_generic(*,
                                                        perturb_setup=None,
                                                        override_checker=None,
                                                        copyright_year=None,
                                                        copyright_owner=None):

    def do_it():

        check_license_checker_full_scenario_failing(
            perturb_setup=perturb_setup,
            checker=override_checker or checker,
            license_title_for_testing="Some License",
            license_text_for_testing=("Our license text\n"
                                      "would go here.\n"),
            copyright_owner_for_testing=copyright_owner,
            copyright_year_for_testing=copyright_year)

    def checker(printed, license_warnings):

        assert printed.lines == ['Checked python library1: Foo License (ALLOWED)',
                                 'Checked python library2: Bar License (FAILED)',
                                 'Checked python library3: Foo License (ALLOWED)',
                                 'Checked python library4: Baz License (FAILED)',
                                 'Checked python library5: --- (EXPECTED_MISSING)',
                                 'Checked python library6: --- (UNEXPECTED_MISSING)',
                                 'Checked python library7: Baz License (FAILED)',
                                 'Checked python library8: Foo License; Bar License (FAILED)',
                                 'Checked python library9: Baz License (FAILED)',
                                 'Checked python libraryA: Big-Org-Approved; Bar License (FAILED)',
                                 'Checked python libraryB: Big-Org-Approved; Bar License (FAILED)',
                                 'Checked python libraryC: Misc-Copyleft; Foo License (SPECIALLY_ALLOWED)',
                                 'Checked python libraryD: Misc-Copyleft (FAILED)',
                                 'There are 4 unacceptable licenses:',
                                 ' Bar License: library2, library8, libraryA, libraryB',
                                 ' Baz License: library4, library7, library9',
                                 ' Big-Org-Approved: libraryB',
                                 ' Misc-Copyleft: libraryD']

        javascript_failure = None
        for warning in license_warnings:
            if warning.startswith("License framework 'javascript' failed to get licenses:"):
                javascript_failure = warning
                break

        assert javascript_failure, "No javascript failure was detected."

        edited_license_warnings = copy.copy(license_warnings)

        edited_license_warnings.remove(javascript_failure)

        assert edited_license_warnings == [
            "There is 1 unexpectedly missing license: library6.",
            "There is 1 no-longer-missing license: library1.",
        ]

    do_it()


def check_license_checker_full_scenario_failing(*, perturb_setup, checker,
                                                license_title_for_testing,
                                                license_text_for_testing,
                                                copyright_owner_for_testing,
                                                copyright_year_for_testing):

    print()  # start on a fresh line

    copyright_owner_for_testing = copyright_owner_for_testing or DEFAULT_COPYRIGHT_OWNER_FOR_TESTING

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove():

        with mock.patch.object(PythonLicenseFramework, "get_dependencies") as mock_get_dependencies:

            with io.open("LICENSE.txt", 'w') as fp:
                print(license_title_for_testing, file=fp)
                copyright_year = copyright_year_for_testing or f'2015, 2018-{datetime.datetime.now().year}'
                print(f"Copyright (c) {copyright_year} {copyright_owner_for_testing}. All Rights Reserved.",
                      file=fp)
                print(license_text_for_testing, file=fp)

            with mock.patch.object(license_logger, 'warning') as mock_license_logger:

                license_warnings = []

                def mocked_license_logger(message):
                    license_warnings.append(message)

                mock_license_logger.side_effect = mocked_license_logger

                class MyLicenseChecker(LicenseChecker):

                    COPYRIGHT_OWNER = DEFAULT_COPYRIGHT_OWNER_FOR_TESTING
                    LICENSE_TITLE = license_title_for_testing
                    LICENSE_FRAMEWORKS = ['python', 'javascript']

                    EXPECTED_MISSING_LICENSES = ['library1', 'library5']

                    ALLOWED = ['Foo License']

                    EXCEPTIONS = {
                        'Big-Org-Approved': ['libraryA'],
                        'Misc-Copyleft': ['libraryC'],
                    }

                mock_get_dependencies.return_value = [
                    {'name': 'library1', 'licenses': ['Foo License'], 'language': 'python'},
                    {'name': 'library2', 'licenses': ['Bar License'], 'language': 'python'},
                    {'name': 'library3', 'licenses': ['Foo License'], 'language': 'python'},
                    {'name': 'library4', 'licenses': ['Baz License'], 'language': 'python'},
                    {'name': 'library5', 'licenses': [], 'language': 'python'},
                    {'name': 'library6', 'licenses': [], 'language': 'python'},
                    {'name': 'library7', 'licenses': ['Baz License'], 'language': 'python'},
                    {'name': 'library8', 'licenses': ['Foo License', 'Bar License'], 'language': 'python'},
                    {'name': 'library9', 'licenses': ['Baz License'], 'language': 'python'},
                    {'name': 'libraryA', 'licenses': ['Big-Org-Approved', 'Bar License'], 'language': 'python'},
                    {'name': 'libraryB', 'licenses': ['Big-Org-Approved', 'Bar License'], 'language': 'python'},
                    {'name': 'libraryC', 'licenses': ['Misc-Copyleft', 'Foo License'], 'language': 'python'},
                    {'name': 'libraryD', 'licenses': ['Misc-Copyleft'], 'language': 'python'},
                ]

                with printed_output() as printed:

                    if perturb_setup:
                        perturb_setup()

                    with pytest.raises(LicenseAcceptabilityCheckFailure):
                        MyLicenseChecker.validate()

                    checker(printed, license_warnings)


def test_license_checker_full_scenario_failing():

    check_license_checker_full_scenario_failing_generic()


def test_license_checker_missing_license_file():

    def perturb_setup():
        os.remove("LICENSE.txt")

    def checker(printed, license_warnings):
        ignored(printed)
        for warning in license_warnings:
            if 'Missing license file.' in warning:
                return
        raise AssertionError(f"Warning about missing file not found in warnings:"
                             f" {json.dumps(license_warnings, indent=2)}")

    check_license_checker_full_scenario_failing_generic(
        perturb_setup=perturb_setup,
        override_checker=checker)


def test_license_checker_bad_license_title():

    def perturb_setup():

        old_file_contents = file_contents("LICENSE.txt")

        with io.open("LICENSE.txt", 'w') as fp:
            fp.write('\n'.join(['BAD TITLE'] + old_file_contents.split('\n')[1:]))

    def checker(printed, license_warnings):
        ignored(printed)  # tested elsewhere
        assert "The license, 'BAD TITLE', was expected to be 'Some License'." in license_warnings

    check_license_checker_full_scenario_failing_generic(
        perturb_setup=perturb_setup,
        override_checker=checker)


def test_license_checker_bad_license_year():

    current_year = datetime.datetime.now().year
    copyright_year = current_year - 2

    def checker(printed, license_warnings):
        ignored(printed)  # tested elsewhere
        for warning in license_warnings:
            if warning.startswith("The copyright year"):
                # The call to str(...) is important because it makes it get proper quotation marks.
                assert warning.endswith(f"should have {str(current_year)!r} at the end.")
                return
        raise AssertionError(f"Warning about copyright year is missing."
                             f" {json.dumps(license_warnings, indent=2)}")

    check_license_checker_full_scenario_failing_generic(
        override_checker=checker,
        copyright_year=f'2015-{copyright_year}')


DEFAULT_COPYRIGHT_OWNER_FOR_TESTING = 'J Doe'


def test_license_checker_bad_license_owner():

    def checker(printed, license_warnings):
        ignored(printed)  # tested elsewhere
        assert (f"The copyright owner, 'Someone different', was expected to be {DEFAULT_COPYRIGHT_OWNER_FOR_TESTING!r}."
                in license_warnings)

    check_license_checker_full_scenario_failing_generic(
        override_checker=checker,
        copyright_owner=f'Someone different')


def test_license_checker_full_scenario_succeeding():

    print()  # start on a fresh line

    mfs = MockFileSystem()

    license_title_for_testing = "Some License"
    license_text_for_testing = ("Our license text\n"
                                "would go here.\n")
    copyright_owner_for_testing = DEFAULT_COPYRIGHT_OWNER_FOR_TESTING
    current_year = str(datetime.datetime.now().year)

    with mfs.mock_exists_open_remove():

        with io.open("LICENSE.txt", 'w') as fp:
            print(license_title_for_testing, file=fp)
            print(f"Copyright (c) 2015, 2018-{current_year} {copyright_owner_for_testing}. All Rights Reserved.",
                  file=fp)
            print(license_text_for_testing, file=fp)

        with mock.patch.object(PythonLicenseFramework, "get_dependencies") as mock_get_dependencies:

            with mock.patch.object(license_logger, 'warning') as mock_license_logger:

                license_warnings = []

                def mocked_license_logger(message):
                    license_warnings.append(message)

                mock_license_logger.side_effect = mocked_license_logger

                class MyLicenseChecker(LicenseChecker):

                    COPYRIGHT_OWNER = copyright_owner_for_testing
                    LICENSE_TITLE = license_title_for_testing
                    LICENSE_FRAMEWORKS = ['python']

                    EXPECTED_MISSING_LICENSES = ['library5', 'library6']

                    ALLOWED = ['Foo License', 'Bar License', 'Baz License']

                    EXCEPTIONS = {
                        'Big-Org-Approved': ['libraryA', 'libraryB'],
                        'Misc-Copyleft': ['libraryC', 'libraryD'],
                    }

                mock_get_dependencies.return_value = [
                    {'name': 'library1', 'licenses': ['Foo License'], 'language': 'python'},
                    {'name': 'library2', 'licenses': ['Bar License'], 'language': 'python'},
                    {'name': 'library3', 'licenses': ['Foo License'], 'language': 'python'},
                    {'name': 'library4', 'licenses': ['Baz License'], 'language': 'python'},
                    {'name': 'library5', 'licenses': [], 'language': 'python'},
                    {'name': 'library6', 'licenses': [], 'language': 'python'},
                    {'name': 'library7', 'licenses': ['Baz License'], 'language': 'python'},
                    {'name': 'library8', 'licenses': ['Foo License', 'Bar License'], 'language': 'python'},
                    {'name': 'library9', 'licenses': ['Baz License'], 'language': 'python'},
                    {'name': 'libraryA', 'licenses': ['Big-Org-Approved', 'Bar License'], 'language': 'python'},
                    {'name': 'libraryB', 'licenses': ['Big-Org-Approved', 'Bar License'], 'language': 'python'},
                    {'name': 'libraryC', 'licenses': ['Misc-Copyleft', 'Foo License'], 'language': 'python'},
                    {'name': 'libraryD', 'licenses': ['Misc-Copyleft'], 'language': 'python'},
                ]

                with printed_output() as printed:

                    MyLicenseChecker.validate()  # not expected to raise an error

                    assert printed.lines == [
                        'Checked python library1: Foo License (ALLOWED)',
                        'Checked python library2: Bar License (ALLOWED)',
                        'Checked python library3: Foo License (ALLOWED)',
                        'Checked python library4: Baz License (ALLOWED)',
                        'Checked python library5: --- (EXPECTED_MISSING)',
                        'Checked python library6: --- (EXPECTED_MISSING)',
                        'Checked python library7: Baz License (ALLOWED)',
                        'Checked python library8: Foo License; Bar License (ALLOWED)',
                        'Checked python library9: Baz License (ALLOWED)',
                        'Checked python libraryA: Big-Org-Approved; Bar License (SPECIALLY_ALLOWED)',
                        'Checked python libraryB: Big-Org-Approved; Bar License (SPECIALLY_ALLOWED)',
                        'Checked python libraryC: Misc-Copyleft; Foo License (SPECIALLY_ALLOWED)',
                        'Checked python libraryD: Misc-Copyleft (SPECIALLY_ALLOWED)',
                    ]

                    assert license_warnings == []


def test_license_file_parser_errors():

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove():

        # Our simpler parser expects just one copyright line.
        with io.open('LICENSE.txt', 'w') as fp:
            fp.write("Some License\n"
                     "Copyright 2003 Somebody\n"
                     "Some license text.\n"
                     "Copyright 2005 Somebody else\n"
                     "More license text.\n")
        with pytest.raises(Exception) as exc:
            LicenseFileParser.parse_simple_license_file(filename="LICENSE.txt")
        assert str(exc.value) == "Too many copyright lines."

        # The simpler parser needs at least one copyright line.
        with io.open('LICENSE.txt', 'w') as fp:
            fp.write("Some License\n"
                     "Some license text.\n"
                     "More license text.\n")
        with pytest.raises(Exception) as exc:
            LicenseFileParser.parse_simple_license_file(filename="LICENSE.txt")
        assert str(exc.value) == "Missing copyright line."


def test_license_checker_analyze_license_dependencies_by_framework():

    # Most other cases of this get tested by other tests, but it's important in particular to test what
    # happens if the checker's .LICENSE_FRAMEWORKS is None and the frameworks= kwarg is passed as None.

    with mock.patch.object(LicenseChecker, "analyze_license_dependencies_for_framework") as mock_analyze:

        analysis = LicenseAnalysis()
        LicenseChecker.analyze_license_dependencies_by_framework(analysis=analysis, frameworks=None)
        assert mock_analyze.mock_calls == [
            mock.call(analysis=analysis, acceptable=None, exceptions=None, framework=JavascriptLicenseFramework),
            mock.call(analysis=analysis, acceptable=None, exceptions=None, framework=PythonLicenseFramework),
        ]


def test_license_checker_analyze_license_file():

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove():

        with mock.patch.object(LicenseFileParser, "validate_simple_license_file"):

            # Check what happens if no license file
            analysis = LicenseAnalysis()
            LicenseChecker.analyze_license_file(analysis=analysis)
            assert "Class LicenseChecker has no declared license owner." in analysis.miscellaneous

            # Check what happens if there's more than one license file
            with io.open("LICENSE.txt", 'w') as fp:
                print("Foo License", file=fp)
            with io.open("LICENSE.rst", 'w') as fp:
                print("Foo License", file=fp)
            analysis = LicenseAnalysis()
            LicenseChecker.analyze_license_file(analysis=analysis)
            assert any(LicenseChecker.MULTIPLE_LICENSE_FILE_ADVICE in warning for warning in analysis.miscellaneous)


def test_validate_simple_license_file():

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove():

        with mock.patch.object(license_logger, 'warning') as mock_license_logger:

            license_warnings = []

            def mocked_license_logger(message):
                license_warnings.append(message)

            mock_license_logger.side_effect = mocked_license_logger

            with io.open('LICENSE.txt', 'w') as fp:
                fp.write("Foo License\n"
                         "Copyright 2020 Acme Inc.\n"
                         "Some license text.\n")

            # Test that with no analysis argument, problems get sent out as warnings
            LicenseFileParser.validate_simple_license_file(filename='LICENSE.txt')
            assert license_warnings == ["The copyright year, '2020', should have '2023' at the end."]

            # Test that with an analysis argument, problems get summarized to that object
            analysis = LicenseAnalysis()
            license_warnings = []
            LicenseFileParser.validate_simple_license_file(filename='LICENSE.txt', analysis=analysis)
            assert analysis.miscellaneous == ["The copyright year, '2020', should have '2023' at the end."]
            assert license_warnings == []
