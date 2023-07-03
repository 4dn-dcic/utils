import copy
import datetime
import io
import pytest

from collections import defaultdict
from dcicutils.license_utils import (
    LicenseFrameworkRegistry, LicenseFramework, PythonLicenseFramework,
    LicenseAnalysis, LicenseChecker, LicenseStatus,
    LicenseCheckFailure, LicenseOwnershipCheckFailure, LicenseAcceptabilityCheckFailure,
    logger as license_logger,
)
from dcicutils.misc_utils import ignored
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


def test_license_checker_full_scenario_failing():

    print()  # start on a fresh line

    mfs = MockFileSystem()

    mfs = MockFileSystem()

    license_title_for_testing = "Some License"
    license_text_for_testing = ("Our license text\n"
                                "would go here.\n")
    copyright_owner_for_testing = "J Doe"
    current_year = str(datetime.datetime.now().year)

    with mfs.mock_exists_open_remove():

        with mock.patch.object(PythonLicenseFramework, "get_dependencies") as mock_get_dependencies:

            with io.open("LICENSE.txt", 'w') as fp:
                print(license_title_for_testing, file=fp)
                print(f"Copyright (c) 2015, 2018-{current_year} {copyright_owner_for_testing}. All Rights Reserved.",
                      file=fp)
                print(license_text_for_testing, file=fp)

            with mock.patch.object(license_logger, 'warning') as mock_license_logger:

                license_warnings = []

                def mocked_license_logger(message):
                    license_warnings.append(message)

                mock_license_logger.side_effect = mocked_license_logger

                class MyLicenseChecker(LicenseChecker):

                    COPYRIGHT_OWNER = copyright_owner_for_testing
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

                    with pytest.raises(LicenseAcceptabilityCheckFailure):
                        MyLicenseChecker.validate()

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


def test_license_checker_full_scenario_succeeding():

    print()  # start on a fresh line

    mfs = MockFileSystem()

    license_title_for_testing = "Some License"
    license_text_for_testing = ("Our license text\n"
                                "would go here.\n")
    copyright_owner_for_testing = "J Doe"
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
