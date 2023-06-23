import pytest

from dcicutils.license_utils import (
    LicenseAnalysis, LicenseChecker, LicenseStatus, UnacceptableLicenseFailure,
    logger as license_logger,
)
from dcicutils.qa_utils import printed_output
from unittest import mock


def test_license_status():

    for attr in dir(LicenseStatus):
        if attr.isupper() and not attr.startswith('_'):
            assert getattr(LicenseStatus, attr) == attr


def test_license_analysis():

    x = LicenseAnalysis()

    assert x.details == []
    assert x.unacceptable == {}
    assert x.unexpected_missing == []
    assert x.no_longer_missing == []

    some_analysis_item = {'name': 'mylib', 'classifiers': ['Foo License'], 'status': LicenseStatus.FAILED}
    some_unacceptable_libs = {'Foo License': ['mylib']}
    some_unexpected_missing = ['obscure_lib']
    some_recently_documented_libs = ['newlib']

    x = LicenseAnalysis(details=[some_analysis_item],
                        unacceptable=some_unacceptable_libs,
                        unexpected_missing=some_unexpected_missing,
                        no_longer_missing=some_recently_documented_libs)

    assert x.details == [some_analysis_item]
    assert x.unacceptable == some_unacceptable_libs
    assert x.unexpected_missing == some_unexpected_missing
    assert x.no_longer_missing == some_recently_documented_libs


def test_license_checker_piplicenses_args():

    default_args = LicenseChecker._piplicenses_args()

    assert default_args.order
    with pytest.raises(Exception):
        print(default_args.no_such_arg)


def test_license_checker_get_licenses():

    licenses = LicenseChecker.get_licenses()

    expected_languages = ['python']

    assert licenses  # make ure it found some

    for entry in licenses:
        assert 'name' in entry
        assert 'license_classifier' in entry
        assert entry.get('language') in expected_languages


def test_license_checker_get_license_analysis():

    analysis = LicenseChecker.get_license_analysis()

    # LicenseChecker defines nothing acceptable, so the analysis will describe failure,
    # but we're not using that failure to fail this test. We just want to know we got
    # back an analysis at all.
    #
    # TODO: To get full coverage we'll need to mock get_licenses to return more interesting stuff.

    assert isinstance(analysis, LicenseAnalysis)


def test_license_checker_show_unacceptable_licenses():

    print()  # start on a fresh line

    with mock.patch.object(LicenseChecker, "get_licenses") as mock_get_licenses:

        mock_get_licenses.return_value = [
            {'name': 'something', 'license_classifier': ['Foo License'], 'language': 'python'}
        ]

        with printed_output() as printed:

            LicenseChecker.show_unacceptable_licenses()

            assert printed.lines == [
                'Checked something: Foo License (FAILED)',
                'There is 1 unacceptable license:',
                ' Foo License: something',
            ]


def test_license_checker_full_scenario_failing():

    print()  # start on a fresh line

    with mock.patch.object(LicenseChecker, "get_licenses") as mock_get_licenses:

        with mock.patch.object(license_logger, 'warning') as mock_license_logger:

            license_warnings = []

            def mocked_license_logger(message):
                license_warnings.append(message)

            mock_license_logger.side_effect = mocked_license_logger

            class MyLicenseChecker(LicenseChecker):

                EXPECTED_MISSING_LICENSES = ['library1', 'library5']

                ALLOWED = ['Foo License']

                EXCEPTIONS = {
                    'Big-Org-Approved': ['libraryA'],
                    'Misc-Copyleft': ['libraryC'],
                }

            mock_get_licenses.return_value = [
                {'name': 'library1', 'license_classifier': ['Foo License'], 'language': 'python'},
                {'name': 'library2', 'license_classifier': ['Bar License'], 'language': 'python'},
                {'name': 'library3', 'license_classifier': ['Foo License'], 'language': 'python'},
                {'name': 'library4', 'license_classifier': ['Baz License'], 'language': 'python'},
                {'name': 'library5', 'license_classifier': [], 'language': 'python'},
                {'name': 'library6', 'license_classifier': [], 'language': 'python'},
                {'name': 'library7', 'license_classifier': ['Baz License'], 'language': 'python'},
                {'name': 'library8', 'license_classifier': ['Foo License', 'Bar License'], 'language': 'python'},
                {'name': 'library9', 'license_classifier': ['Baz License'], 'language': 'python'},
                {'name': 'libraryA', 'license_classifier': ['Big-Org-Approved', 'Bar License'], 'language': 'python'},
                {'name': 'libraryB', 'license_classifier': ['Big-Org-Approved', 'Bar License'], 'language': 'python'},
                {'name': 'libraryC', 'license_classifier': ['Misc-Copyleft', 'Foo License'], 'language': 'python'},
                {'name': 'libraryD', 'license_classifier': ['Misc-Copyleft'], 'language': 'python'},
            ]

            with printed_output() as printed:

                with pytest.raises(UnacceptableLicenseFailure):
                    MyLicenseChecker.validate()

                assert printed.lines == ['Checked library1: Foo License (ALLOWED)',
                                         'Checked library2: Bar License (FAILED)',
                                         'Checked library3: Foo License (ALLOWED)',
                                         'Checked library4: Baz License (FAILED)',
                                         'Checked library5: --- (EXPECTED_MISSING)',
                                         'Checked library6: --- (UNEXPECTED_MISSING)',
                                         'Checked library7: Baz License (FAILED)',
                                         'Checked library8: Foo License; Bar License (FAILED)',
                                         'Checked library9: Baz License (FAILED)',
                                         'Checked libraryA: Big-Org-Approved; Bar License (FAILED)',
                                         'Checked libraryB: Big-Org-Approved; Bar License (FAILED)',
                                         'Checked libraryC: Misc-Copyleft; Foo License (SPECIALLY_ALLOWED)',
                                         'Checked libraryD: Misc-Copyleft (FAILED)',
                                         'There are 4 unacceptable licenses:',
                                         ' Bar License: library2, library8, libraryA, libraryB',
                                         ' Baz License: library4, library7, library9',
                                         ' Big-Org-Approved: libraryB',
                                         ' Misc-Copyleft: libraryD']

                assert license_warnings == [
                    'There is 1 unexpectedly missing license: library6.',
                    'There is 1 no-longer-missing license: library1.',
                ]


def test_license_checker_full_scenario_succeeding():

    print()  # start on a fresh line

    with mock.patch.object(LicenseChecker, "get_licenses") as mock_get_licenses:

        with mock.patch.object(license_logger, 'warning') as mock_license_logger:

            license_warnings = []

            def mocked_license_logger(message):
                license_warnings.append(message)

            mock_license_logger.side_effect = mocked_license_logger

            class MyLicenseChecker(LicenseChecker):

                EXPECTED_MISSING_LICENSES = ['library5', 'library6']

                ALLOWED = ['Foo License', 'Bar License', 'Baz License']

                EXCEPTIONS = {
                    'Big-Org-Approved': ['libraryA', 'libraryB'],
                    'Misc-Copyleft': ['libraryC', 'libraryD'],
                }

            mock_get_licenses.return_value = [
                {'name': 'library1', 'license_classifier': ['Foo License'], 'language': 'python'},
                {'name': 'library2', 'license_classifier': ['Bar License'], 'language': 'python'},
                {'name': 'library3', 'license_classifier': ['Foo License'], 'language': 'python'},
                {'name': 'library4', 'license_classifier': ['Baz License'], 'language': 'python'},
                {'name': 'library5', 'license_classifier': [], 'language': 'python'},
                {'name': 'library6', 'license_classifier': [], 'language': 'python'},
                {'name': 'library7', 'license_classifier': ['Baz License'], 'language': 'python'},
                {'name': 'library8', 'license_classifier': ['Foo License', 'Bar License'], 'language': 'python'},
                {'name': 'library9', 'license_classifier': ['Baz License'], 'language': 'python'},
                {'name': 'libraryA', 'license_classifier': ['Big-Org-Approved', 'Bar License'], 'language': 'python'},
                {'name': 'libraryB', 'license_classifier': ['Big-Org-Approved', 'Bar License'], 'language': 'python'},
                {'name': 'libraryC', 'license_classifier': ['Misc-Copyleft', 'Foo License'], 'language': 'python'},
                {'name': 'libraryD', 'license_classifier': ['Misc-Copyleft'], 'language': 'python'},
            ]

            with printed_output() as printed:

                MyLicenseChecker.validate()  # not expected to raise an error

                assert printed.lines == [
                    'Checked library1: Foo License (ALLOWED)',
                    'Checked library2: Bar License (ALLOWED)',
                    'Checked library3: Foo License (ALLOWED)',
                    'Checked library4: Baz License (ALLOWED)',
                    'Checked library5: --- (EXPECTED_MISSING)',
                    'Checked library6: --- (EXPECTED_MISSING)',
                    'Checked library7: Baz License (ALLOWED)',
                    'Checked library8: Foo License; Bar License (ALLOWED)',
                    'Checked library9: Baz License (ALLOWED)',
                    'Checked libraryA: Big-Org-Approved; Bar License (SPECIALLY_ALLOWED)',
                    'Checked libraryB: Big-Org-Approved; Bar License (SPECIALLY_ALLOWED)',
                    'Checked libraryC: Misc-Copyleft; Foo License (SPECIALLY_ALLOWED)',
                    'Checked libraryD: Misc-Copyleft (SPECIALLY_ALLOWED)',
                ]

                assert license_warnings == []