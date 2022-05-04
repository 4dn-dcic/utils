import argparse
import datetime
import io
import json
import os
import pytest
import re
import subprocess
import sys

from io import StringIO
from unittest import mock

from dcicutils.deployment_utils import (
    IniFileManager, boolean_setting, CreateMappingOnDeployManager,
    BasicOrchestratedCGAPIniFileManager, BasicLegacyCGAPIniFileManager,
    create_file_from_template,
    # TODO: This isn't yet tested.
    # EBDeployer,
)
from dcicutils.env_utils import is_cgap_env, data_set_for_env
from dcicutils.exceptions import InvalidParameterError
from dcicutils.qa_utils import MockFileSystem, printed_output, MockedCommandArgs
from dcicutils.misc_utils import ignored, file_contents
from dcicutils.qa_utils import override_environ


_MY_DIR = os.path.dirname(__file__)


class FakeDistribution:
    version = "simulated"


class TestDeployer(IniFileManager):
    TEMPLATE_DIR = os.path.join(_MY_DIR, "ini_files")
    PYPROJECT_FILE_NAME = os.path.join(os.path.dirname(_MY_DIR), "pyproject.toml")


class TestOrchestratedCgapDeployer(BasicOrchestratedCGAPIniFileManager):
    TEMPLATE_DIR = os.path.join(_MY_DIR, "ini_files")
    PYPROJECT_FILE_NAME = os.path.join(os.path.dirname(_MY_DIR), "pyproject.toml")


class TestLegacyCgapDeployer(BasicLegacyCGAPIniFileManager):
    TEMPLATE_DIR = os.path.join(_MY_DIR, "ini_files")
    PYPROJECT_FILE_NAME = os.path.join(os.path.dirname(_MY_DIR), "pyproject.toml")


def test_deployment_utils_omittable():

    assert not TestDeployer.omittable("foo", "foo")
    assert not TestDeployer.omittable(" foo", " foo")
    assert not TestDeployer.omittable("foo=", "foo=")
    assert not TestDeployer.omittable(" foo=", " foo=")
    assert not TestDeployer.omittable("foo =", "foo=")
    assert not TestDeployer.omittable(" foo =", " foo=")
    assert not TestDeployer.omittable("foo=$X", "foo=bar")
    assert not TestDeployer.omittable(" foo=$X", " foo=$X")
    assert not TestDeployer.omittable("foo = $X", "foo = bar")
    assert not TestDeployer.omittable(" foo = $X", " foo = $X")
    assert not TestDeployer.omittable("foo=${X}", "foo=bar")
    assert not TestDeployer.omittable(" foo=${X}", " foo=${X}")
    assert not TestDeployer.omittable("foo = ${X}", "foo = bar")
    assert not TestDeployer.omittable(" foo = ${X}", " foo = ${X}")
    assert TestDeployer.omittable("foo=$X", "foo=")
    assert TestDeployer.omittable("foo=$X", "foo= ")
    assert TestDeployer.omittable("foo=$X", "foo= ")
    assert TestDeployer.omittable("foo=$X", "foo= \r")
    assert TestDeployer.omittable("foo=$X", "foo= \r\n")
    assert TestDeployer.omittable("foo=$X", "foo=   \r\n \r\n ")
    assert TestDeployer.omittable("foo=${X}", "foo=")
    assert TestDeployer.omittable("foo=${X}", "foo=")
    assert TestDeployer.omittable("foo=${X}", "foo= ")
    assert TestDeployer.omittable("foo=${X}", "foo= \r")
    assert TestDeployer.omittable("foo=${X}", "foo= \r\n")
    assert TestDeployer.omittable("foo=${X}", "foo=   \r\n \r\n ")
    assert TestDeployer.omittable(" foo = $X", " foo =")
    assert TestDeployer.omittable(" foo = $X", " foo = ")
    assert TestDeployer.omittable(" foo = $X", " foo = ")
    assert TestDeployer.omittable(" foo = $X", " foo = \r")
    assert TestDeployer.omittable(" foo = $X", " foo = \r\n")
    assert TestDeployer.omittable(" foo = $X", " foo =   \r\n \r\n ")


def test_deployment_utils_environment_template_filename():

    with pytest.raises(ValueError):
        TestDeployer.environment_template_filename('foo')

    actual = os.path.abspath(TestDeployer.environment_template_filename('cgapdev'))

    assert actual.endswith("/ini_files/cgapdev.ini")
    assert os.path.exists(actual)

    assert (TestDeployer.environment_template_filename('cgapdev') ==
            TestDeployer.environment_template_filename('fourfront-cgapdev'))


def test_deployment_utils_template_environment_names():

    names = TestDeployer.template_environment_names()

    required_names = ['blue', 'green', 'cgap', 'cgapdev', 'cgaptest', 'webdev', 'webprod', 'webprod2']

    for required_name in required_names:
        assert required_name in names


MOCKED_SOURCE_BUNDLE = "/some/source/bundle"
MOCKED_BUNDLE_VERSION = 'v-12345-simulated-bundle-version'
MOCKED_LOCAL_GIT_VERSION = 'v-67890-simulated-git-version'
MOCKED_PROJECT_VERSION = '11.22.33'


def make_mocked_check_output_for_get_version(simulate_git_command=True, simulate_git_repo=True):
    def mocked_check_output(command):
        if simulate_git_command and command[0] == 'git':
            assert command == ['git', 'describe', '--dirty']  # This is the only case we handle
            if simulate_git_repo:
                return bytes(MOCKED_LOCAL_GIT_VERSION, 'utf-8')
            else:
                raise subprocess.CalledProcessError(returncode=1, cmd=command)
        else:
            raise FileNotFoundError("Simulated absence of 'git'.")
    return mocked_check_output


def test_deployment_utils_build_ini_file_from_template():
    """
    Fully mocked test of building the ini file.
    NOTE: This implicitly also tests build_ini_file_from_stream.
    """

    # All variables named ENCODED_xxx1, ENCODED_xxx2, etc. are expected to be available in the template files
    # as just xxx1, xxx2, etc. AFTER being merged with command line arguments (which take precedence). If not
    # supplied, defaulting will in some cases occur. So the order of precedence for assigning values to the xxxN
    # variables is:
    #
    # 1. Command arguments always take precedence over all else, as they are regarded as most specific.
    # 2. If no command argument has intervened, ENCODED_xxxN values take precedence over any would-be-defaulting
    #    because it represents the ambient default of the command and is presumably there to back up commands.
    # 3. If neither a command argument nor an ENCODED_xxxN variable has assigned a default, argument-specific
    #    defaulting might in some cases be attempted. In a few places, this defaulting uses previously-resolved
    #    values of other variables (as happens where the last-resort default for bucket naming is composed from
    #    s3_bucket_org and s3_bucket_env, but can be overridden on a bucket-kind by bucket-kind basis with
    #    a <kind>_bucket argument to the command or an ENCODED_<kind>_BUCKET environment variable).

    with mock.patch("pkg_resources.get_distribution", return_value=FakeDistribution()):

        some_template_file_name = "mydir/whatever"
        some_ini_file_name = "mydir/production.ini"

        # This is our simulation of data that is coming in from ElasticBeanstalk
        env_vars = dict(RDS_DB_NAME='snow_white', RDS_USERNAME='user', RDS_PASSWORD='my-secret',
                        RDS_HOSTNAME='unittest', RDS_PORT="6543")

        # Establish our environment variables
        with override_environ(**env_vars):

            # Check that our abstraction is working. There is a bit of paranoia here,
            # but since we'll be dealing with deployment configurations, the idea is to be sure
            # we're in the land of mocking only.
            for env_var in env_vars:
                assert env_var in os.environ and os.environ[env_var] == env_vars[env_var], (
                        "os.environ[%r] did not get added correctly" % env_var
                )

            # NOTE: With a small amount of extra effort, this might be possible to separate into a mock_utils for
            #       other tests to use. But it's not quite there. -kmp 27-Apr-2020
            class MockFileStream:
                """
                This represents files in the file system so that other mocks can create them and we can later
                inquire about their contents.
                """

                FILE_SYSTEM = {}  # A dictionary of files in our fake file system.

                @classmethod
                def reset(cls):
                    """Resets the fake file system to empty."""
                    cls.FILE_SYSTEM = {}

                def __init__(self, filename, mode):
                    assert 'w' in mode
                    self.filename = filename
                    self.output_string_stream = StringIO()  # A string stream to use rather than a file stream.

                def __enter__(self):
                    """
                    When doing a mocked call to io.open, this arranges for a StringIO() object to gather file output.
                    """
                    return self.output_string_stream

                def __exit__(self, type_, value, traceback):
                    """
                    What we actually store in the file system is a list of lines, not a big string,
                    though that design choice might not stand up to scrutiny if this mock were ever reused.
                    In the current implementation, this takes care of getting the final data out of the string stream,
                    breaking it into lines, and storing the lines in the mock file system.
                    """
                    self.FILE_SYSTEM[self.filename] = self.output_string_stream.getvalue().strip().split('\n')

            # NOTE: This mock_open might be simpler and more general if we just called our mock I/O to write the files.
            #       In effect, it is pretending as if files are there which aren't, which weird because that pretense
            #       is inside the pretense that we have a file system at all. But it suffices for now. -kmp 27-Apr-2020
            def mocked_open(filename, mode='r', encoding=None):
                """
                On read (mode=='r'), this simulates presence of several files in an ad hoc way, not by mock file system.
                On write, this uses StringIO and stores the output in the mock file system as a list of lines.
                """
                # Our mock does nothing with the encoding, but wants to make sure no one is asking us for
                # things we might have had to do something special with.
                assert encoding in (None, 'utf-8')

                # In this test there are two opens, one for read and one for write, so we discriminate on that basis.
                print("Enter mock_open", filename, mode)
                if mode == 'r':

                    if filename == TestDeployer.EB_MANIFEST_FILENAME:

                        print("reading mocked EB MANIFEST:", TestDeployer.EB_MANIFEST_FILENAME)
                        return StringIO('{"Some": "Stuff", "VersionLabel": "%s", "Other": "Stuff"}\n'
                                        % MOCKED_BUNDLE_VERSION)

                    elif filename == some_template_file_name:

                        print("reading mocked TEMPLATE FILE", some_ini_file_name)
                        return StringIO(
                            '[Foo]\n'
                            'database = "${RDS_DB_NAME}"\n'
                            'some_url = "http://${RDS_USERNAME}@$RDS_HOSTNAME:$RDS_PORT/"\n'
                            'oops = "$NOT_AN_ENV_VAR"\n'
                            'hmmm = "${NOT_AN_ENV_VAR_EITHER}"\n'
                            'shhh = "$RDS_PASSWORD"\n'
                            'version = "${APP_VERSION}"\n'
                            'project_version = "${PROJECT_VERSION}"\n'
                            'indexer = ${INDEXER}\n'
                            'index_server = ${INDEX_SERVER}\n'
                        )

                    elif filename == TestDeployer.PYPROJECT_FILE_NAME:

                        print("reading mocked TOML FILE", TestDeployer.PYPROJECT_FILE_NAME)
                        return StringIO(
                            '[something]\n'
                            'version = "5.6.7"\n'
                            '[tool.poetry]\n'
                            'author = "somebody"\n'
                            'version = "%s"\n' % MOCKED_PROJECT_VERSION
                        )

                    else:

                        raise AssertionError("mocked_open(%r, %r) unsupported." % (filename, mode))

                else:

                    assert mode == 'w'
                    assert filename == some_ini_file_name
                    return MockFileStream(filename, mode)

            with mock.patch("subprocess.check_output") as mock_check_output:
                mock_check_output.side_effect = make_mocked_check_output_for_get_version()
                with mock.patch("os.path.exists") as mock_exists:
                    def mocked_exists(filename):
                        # This cheats on the mock file system and just knows about two specific names we care about.
                        # If we had used the mock file system to store the files, it would be a little cleaner.
                        # But it's close enough for now. -kmp 27-Apr-2020
                        return filename in [TestDeployer.EB_MANIFEST_FILENAME, some_template_file_name]
                    mock_exists.side_effect = mocked_exists
                    with mock.patch("io.open", side_effect=mocked_open):
                        # Here we finally call the builder. Output will be a list of lines in the mock file system.
                        TestDeployer.build_ini_file_from_template(some_template_file_name, some_ini_file_name)

            # The subtle thing here is that if it were a multi-line string,
            # all the "%" substitutions would have to be on the final line, not line-by-line where needed.
            assert MockFileStream.FILE_SYSTEM[some_ini_file_name] == [
                '[Foo]',
                'database = "snow_white"',
                'some_url = "http://user@unittest:6543/"',
                'oops = "$NOT_AN_ENV_VAR"',
                'hmmm = "${NOT_AN_ENV_VAR_EITHER}"',
                'shhh = "my-secret"',
                'version = "%s"' % MOCKED_BUNDLE_VERSION,
                'project_version = "%s"' % MOCKED_PROJECT_VERSION,
                'indexer = true',
            ]

            MockFileStream.reset()

            with mock.patch("subprocess.check_output") as mock_check_output:
                mock_check_output.side_effect = make_mocked_check_output_for_get_version()
                with mock.patch("os.path.exists") as mock_exists:
                    def mocked_exists(filename):
                        # Important to this test: This will return False for EB_MANIFEST_FILENAME,
                        # causing the strategy of using the version there to fall through,
                        # so we expect to try using the git version instead.
                        return filename in [some_template_file_name]
                    mock_exists.side_effect = mocked_exists
                    with mock.patch("io.open", side_effect=mocked_open):
                        TestDeployer.build_ini_file_from_template(some_template_file_name, some_ini_file_name)

            assert MockFileStream.FILE_SYSTEM[some_ini_file_name] == [
                '[Foo]',
                'database = "snow_white"',
                'some_url = "http://user@unittest:6543/"',
                'oops = "$NOT_AN_ENV_VAR"',
                'hmmm = "${NOT_AN_ENV_VAR_EITHER}"',
                'shhh = "my-secret"',
                'version = "%s"' % MOCKED_LOCAL_GIT_VERSION,  # This is the result of no manifest file existing
                'project_version = "%s"' % MOCKED_PROJECT_VERSION,
                'indexer = true',
            ]

            MockFileStream.reset()

            with mock.patch("subprocess.check_output") as mock_check_output:
                # Note that here we simulate the absence of the 'git' command, so we also can't expect a git tag
                # as part of the version output.
                mock_check_output.side_effect = make_mocked_check_output_for_get_version(simulate_git_command=False)
                with mock.patch("os.path.exists") as mock_exists:

                    def mocked_exists(filename):
                        # Important to this test: This will return False for EB_MANIFEST_FILENAME,
                        # causing the strategy of using the version there to fall through,
                        # so we expect to try using the git version instead, which will also fail
                        # because we're simulating the absence of Git.
                        return filename in [some_template_file_name]

                    mock_exists.side_effect = mocked_exists

                    class MockDateTime:

                        DATETIME = datetime.datetime

                        @classmethod
                        def now(cls):
                            return cls.DATETIME(2001, 2, 3, 4, 55, 6)

                    with mock.patch("io.open", side_effect=mocked_open):
                        with mock.patch.object(datetime, "datetime", MockDateTime()):
                            TestDeployer.build_ini_file_from_template(some_template_file_name, some_ini_file_name)

            assert MockFileStream.FILE_SYSTEM[some_ini_file_name] == [
                '[Foo]',
                'database = "snow_white"',
                'some_url = "http://user@unittest:6543/"',
                'oops = "$NOT_AN_ENV_VAR"',
                'hmmm = "${NOT_AN_ENV_VAR_EITHER}"',
                'shhh = "my-secret"',
                'version = "unknown-version-at-20010203045506000000"',  # We mocked datetime.datetime.now() to get this
                'project_version = "%s"' % MOCKED_PROJECT_VERSION,
                'indexer = true',
            ]

            MockFileStream.reset()

            truth = ["TRUE", "True", "true", "something"]
            falsity = ["FALSE", "False", "false"]

            for indexer_truth in truth:
                for index_server_truth in truth:
                    print("indexer_truth=", indexer_truth, "index_server_truth=", index_server_truth)
                    # For this test, we check if the 'indexer' option being set correctly sets ENCODED.INDEXER
                    with mock.patch("os.path.exists") as mock_exists:
                        mock_exists.return_value = True
                        with mock.patch("io.open", side_effect=mocked_open):
                            with override_environ(ENCODED_INDEXER=indexer_truth,
                                                  ENCODED_INDEX_SERVER=index_server_truth):
                                TestDeployer.build_ini_file_from_template(some_template_file_name, some_ini_file_name)

                    assert MockFileStream.FILE_SYSTEM[some_ini_file_name] == [
                        '[Foo]',
                        'database = "snow_white"',
                        'some_url = "http://user@unittest:6543/"',
                        'oops = "$NOT_AN_ENV_VAR"',
                        'hmmm = "${NOT_AN_ENV_VAR_EITHER}"',
                        'shhh = "my-secret"',
                        'version = "%s"' % MOCKED_BUNDLE_VERSION,
                        'project_version = "11.22.33"',
                        'indexer = true',  # the value will have been canonicalized
                        'index_server = true',
                    ]

                    MockFileStream.reset()

            for indexer_falsity in falsity:
                for index_server_falsity in falsity:
                    print("indexer_falsity=", indexer_falsity, "index_server_falsity=", index_server_falsity)
                    # For this test, we check if the 'indexer' option being set correctly sets ENCODED.INDEXER
                    with mock.patch("os.path.exists") as mock_exists:
                        mock_exists.return_value = True
                        with mock.patch("io.open", side_effect=mocked_open):
                            with override_environ(ENCODED_INDEXER=indexer_falsity,
                                                  ENCODED_INDEX_SERVER=index_server_falsity):
                                TestDeployer.build_ini_file_from_template(some_template_file_name, some_ini_file_name)

                    assert MockFileStream.FILE_SYSTEM[some_ini_file_name] == [
                        '[Foo]',
                        'database = "snow_white"',
                        'some_url = "http://user@unittest:6543/"',
                        'oops = "$NOT_AN_ENV_VAR"',
                        'hmmm = "${NOT_AN_ENV_VAR_EITHER}"',
                        'shhh = "my-secret"',
                        'version = "%s"' % MOCKED_BUNDLE_VERSION,
                        'project_version = "11.22.33"',
                        # (The 'indexer =' line will be suppressed.)
                        # (The 'indexer_server =' line will be suppressed.)
                    ]

                MockFileStream.reset()

            # For this test, we check if the 'indexer' option being set correctly sets the ENCODED.INDEXER option
            with mock.patch("os.path.exists") as mock_exists:
                mock_exists.return_value = True
                with mock.patch("io.open", side_effect=mocked_open):
                    TestDeployer.build_ini_file_from_template(some_template_file_name, some_ini_file_name, indexer=True)

            assert MockFileStream.FILE_SYSTEM[some_ini_file_name] == [
                '[Foo]',
                'database = "snow_white"',
                'some_url = "http://user@unittest:6543/"',
                'oops = "$NOT_AN_ENV_VAR"',
                'hmmm = "${NOT_AN_ENV_VAR_EITHER}"',
                'shhh = "my-secret"',
                'version = "%s"' % MOCKED_BUNDLE_VERSION,
                'project_version = "11.22.33"',
                'indexer = true',
            ]

            MockFileStream.reset()

            # For these next three tests, we're going to pretend we deployed with
            # bs_name == 'fourfront-indexer' in various ways. We expect an exception to be raised.

            with pytest.raises(RuntimeError):
                with mock.patch("os.path.exists") as mock_exists:
                    mock_exists.return_value = True
                    with mock.patch("io.open", side_effect=mocked_open):
                        with override_environ(ENCODED_BS_ENV='fourfront-indexer'):
                            TestDeployer.build_ini_file_from_template(some_template_file_name,
                                                                      some_ini_file_name, indexer=True)

            MockFileStream.reset()

            with pytest.raises(RuntimeError):
                with mock.patch("os.path.exists") as mock_exists:
                    mock_exists.return_value = True
                    with mock.patch("io.open", side_effect=mocked_open):
                        TestDeployer.build_ini_file_from_template(some_template_file_name,
                                                                  some_ini_file_name,
                                                                  bs_env='fourfront-indexer', indexer=True)

            MockFileStream.reset()

            with pytest.raises(RuntimeError):
                with mock.patch("os.path.exists") as mock_exists:
                    mock_exists.return_value = True
                    with mock.patch("io.open", side_effect=mocked_open):
                        TestDeployer.build_ini_file_from_template(some_template_file_name,
                                                                  some_ini_file_name,
                                                                  bs_env='fourfront-indexer')

            # Uncomment this for debugging...
            # assert False, "PASSED"


def test_deployment_utils_get_app_version():

    with mock.patch('subprocess.check_output') as mock_check_output:

        with mock.patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True
            with mock.patch("io.open") as mock_open:
                mock_open.return_value = StringIO('{"VersionLabel": "%s"}' % MOCKED_BUNDLE_VERSION)
                mock_check_output.side_effect = make_mocked_check_output_for_get_version()
                assert IniFileManager.get_app_version() == MOCKED_BUNDLE_VERSION

        mock_check_output.side_effect = make_mocked_check_output_for_get_version()
        assert TestDeployer.get_app_version() == MOCKED_LOCAL_GIT_VERSION
        assert TestOrchestratedCgapDeployer.get_app_version() == MOCKED_LOCAL_GIT_VERSION
        assert TestLegacyCgapDeployer.get_app_version() == MOCKED_LOCAL_GIT_VERSION

        # Simulate 'git' command not found.
        mock_check_output.side_effect = make_mocked_check_output_for_get_version(simulate_git_command=False)
        v = TestDeployer.get_app_version()
        assert re.match("^unknown-version-at-[0-9]+$", v)

        assert not os.environ.get('EB_CONFIG_SOURCE_BUNDLE')
        # Simulate 'git' repo not found.
        mock_check_output.side_effect = make_mocked_check_output_for_get_version(simulate_git_repo=False)
        v = TestDeployer.get_app_version()
        assert re.match("^unknown-version-at-[0-9]+$", v)


def test_deployment_utils_get_local_git_version():

    with mock.patch('subprocess.check_output') as mock_check_output:

        mock_check_output.side_effect = make_mocked_check_output_for_get_version()
        assert TestDeployer.get_local_git_version() == MOCKED_LOCAL_GIT_VERSION

        mock_check_output.side_effect = make_mocked_check_output_for_get_version(simulate_git_command=False)
        with pytest.raises(FileNotFoundError):
            TestDeployer.get_local_git_version()

        mock_check_output.side_effect = make_mocked_check_output_for_get_version(simulate_git_repo=False)
        with pytest.raises(subprocess.CalledProcessError):
            TestDeployer.get_local_git_version()


def test_deployment_utils_get_eb_bundled_version():

    with mock.patch("os.path.exists") as mock_exists:
        mock_exists.return_value = True
        with mock.patch("io.open") as mock_open:
            mock_open.return_value = StringIO('{"VersionLabel": "%s"}' % MOCKED_BUNDLE_VERSION)
            assert TestDeployer.get_eb_bundled_version() == MOCKED_BUNDLE_VERSION

    with mock.patch("os.path.exists") as mock_exists:
        mock_exists.return_value = True
        with mock.patch("io.open") as mock_open:
            def mocked_open_error(filename, mode='r'):
                ignored(filename, mode)
                raise Exception("Simulated file error (file not found or permissions problem).")
            mock_open.side_effect = mocked_open_error
            assert TestDeployer.get_eb_bundled_version() is None

    with mock.patch("os.path.exists") as mock_exists:
        mock_exists.return_value = False
        with mock.patch("io.open") as mock_open:
            def mocked_open_error(filename, mode='r'):
                ignored(filename, mode)
                raise AssertionError("This shouldn't happen but will get caught.")
            mock_open.side_effect = mocked_open_error
            assert TestDeployer.get_eb_bundled_version() is None  # Because of os.path.exists returned False
            assert mock_open.call_count == 0  # This proves that we didn't try to open the file


def test_deployment_utils_any_environment_template_filename():

    with mock.patch("os.path.exists", return_value=True):
        any_ini = TestDeployer.any_environment_template_filename()
        assert any_ini.endswith("any.ini")

    with mock.patch("os.path.exists", return_value=False):
        with pytest.raises(ValueError):
            TestDeployer.any_environment_template_filename()


def test_deployment_utils_transitional_equivalence():
    """
    We used to use separate files for each environment. This tests that the new any.ini technology,
    with a few new environment variables, will produce the same thing.

    This proves that if we set at least "ENCODED_ES_SERVER" and "ENCODED_BS_ENV" environment variables,
    or invoke generate_ini_file adding the "--es_server" nad "--bs_env" arguments, we should get a proper
    production.ini.
    """

    # TODO: Once this mechanism is in place, the files cgap.ini, cgapdev.ini, cgaptest.ini, and cgapwolf.ini
    #       can either be removed (and these transitional tests removed) or transitioned to be test data.

    def tester(ref_ini, bs_env, data_set, es_server, *, any_ini=None, es_namespace=None, line_checker=None,
               use_ini_file_manager_kind=None, **others):
        """
        This common tester program checks that the any.ini does the same thing as a given ref ini,
        given a particular set of environment variables.  It does the output to a string in both cases
        and then compares the result.
        """

        if use_ini_file_manager_kind == 'legacy-cgap':
            class SelectedTestDeployer (TestLegacyCgapDeployer):
                pass
        elif use_ini_file_manager_kind == 'orchestrated-cgap':
            class SelectedTestDeployer (TestOrchestratedCgapDeployer):
                pass
        elif use_ini_file_manager_kind is None:
            class SelectedTestDeployer (TestDeployer):
                pass
        else:
            raise InvalidParameterError('use_ini_file_manager_kind', use_ini_file_manager_kind,
                                        ['legacy-cgap', 'orchestrated-cgap'])

        def fix_(x):
            return x.replace('_', '-')

        if bs_env.startswith("fourfront"):
            assert fix_(ref_ini[:-4]) == fix_(bs_env[10:])  # "xyz.ini" needs to match "fourfront-xyz"
        else:
            assert fix_(ref_ini[:-4]) == fix_(bs_env)  # In a post-fourfront world, "xyz.ini" needs to match "xyz"

        es_namespace = es_namespace or bs_env

        # Test of build_ini_from_template with just 2 keyword arguments explicitly supplied (bs_env, es_server),
        # and others defaulted.

        ref_output = StringIO()
        any_output = StringIO()

        any_ini_path = os.path.join(SelectedTestDeployer.TEMPLATE_DIR,
                                    any_ini or ("cg_any.ini" if is_cgap_env(bs_env) else "ff_any.ini"))
        ref_ini_path = os.path.join(SelectedTestDeployer.TEMPLATE_DIR, ref_ini)

        with override_environ(APP_VERSION='externally_defined'):
            # We don't expect variables like APP_VERSION to be globally available because
            # we'd end up shadowing them and confusion could result about which variable
            # the template wanted to reference, ours or theirs. -kmp 9-May-2020
            with pytest.raises(RuntimeError):
                SelectedTestDeployer.build_ini_stream_from_template(ref_ini_path, "this shouldn't get used")

        SelectedTestDeployer.build_ini_stream_from_template(ref_ini_path, ref_output,
                                                            bs_env=bs_env, es_server=es_server, **others)
        SelectedTestDeployer.build_ini_stream_from_template(any_ini_path, any_output,
                                                            # we should be able to default data_env & es_namespace
                                                            bs_env=bs_env, es_server=es_server, **others)

        ref_content = ref_output.getvalue()
        any_content = any_output.getvalue()
        assert ref_content == any_content

        # Test of build_ini_from_template with all 4 keyword arguments explicitly supplied (bs_env, data_set,
        # es_server, es_namespace), none defaulted.

        ref_output = StringIO()
        any_output = StringIO()

        SelectedTestDeployer.build_ini_stream_from_template(ref_ini_path, ref_output,
                                                            bs_env=bs_env, data_set=data_set,
                                                            es_server=es_server, es_namespace=es_namespace, **others)
        SelectedTestDeployer.build_ini_stream_from_template(any_ini_path, any_output,
                                                            bs_env=bs_env, data_set=data_set,
                                                            es_server=es_server, es_namespace=es_namespace, **others)

        ref_content = ref_output.getvalue()
        any_content = any_output.getvalue()
        assert ref_content == any_content

        problems = []

        if line_checker:

            for raw_line in io.StringIO(any_content):
                line = raw_line.rstrip()
                problem = line_checker.check(line)
                if problem:
                    problems.append(problem)

            line_checker.check_finally()

            assert problems == [], "Problems found:\n%s" % "\n".join(problems)

    with mock.patch("pkg_resources.get_distribution", return_value=FakeDistribution()):
        with mock.patch.object(IniFileManager, "get_app_version",
                               return_value=MOCKED_PROJECT_VERSION) as mock_get_app_version:
            with mock.patch("toml.load", return_value={"tool": {"poetry": {"version": MOCKED_LOCAL_GIT_VERSION}}}):
                with override_environ(ENCODED_INDEXER=None, ENCODED_INDEX_SERVER=None):

                    class Checker:

                        def __init__(self, expect_indexer, expect_index_server, expected_values=None):
                            self.indexer = None
                            self.expect_indexer = expect_indexer
                            self.expect_index_server = expect_index_server
                            self.indexer = None
                            self.index_server = None
                            self.expected_values = expected_values or {}
                            self.actual_values = {}

                        def check_any(self, line):
                            if line.startswith('indexer ='):
                                print("saw indexer line:", repr(line))
                                self.indexer = line.split('=')[1].strip()
                            if line.startswith('index_server ='):
                                print("saw index server line:", repr(line))
                                self.index_server = line.split('=')[1].strip()
                            for key in self.expected_values.keys():
                                if line.startswith(f"{key} ="):
                                    print("saw expected line:", repr(line))
                                    self.actual_values[key] = line.split('=')[1].strip()
                                    break  # very minor optimization, but not strictly needed

                        def check(self, line):
                            self.check_any(line)

                        def check_finally(self):
                            assert self.indexer == self.expect_indexer
                            assert self.index_server == self.expect_index_server
                            assert self.expected_values == self.actual_values

                    class CGAPProdChecker(Checker):
                        pass

                    class FFProdChecker(Checker):

                        def check(self, line):
                            if 'bucket =' in line:
                                fragment = 'fourfront-webprod'
                                if fragment not in line:
                                    return "'%s' missing in '%s'" % (fragment, line)
                            self.check_any(line)

                    # CGAP uses data_set='prod' for everything but 'fourfront-cgapdev', which uses 'test'.
                    # But that logic is hidden in data_set_for_env, in case it changes.

                    us_east = "us-east-1.es.amazonaws.com:80"
                    index_default = "true"
                    index_server_default = None

                    bs_env = "fourfront-cgap"
                    data_set = data_set_for_env(bs_env)
                    tester(ref_ini="cgap.ini", bs_env=bs_env, data_set=data_set,
                           es_server="search-fourfront-cgap-ewf7r7u2nq3xkgyozdhns4bkni.%s" % us_east,
                           line_checker=CGAPProdChecker(expect_indexer=index_default,
                                                        expect_index_server=index_server_default))

                    test_alpha_org = "alphatest"
                    test_encrypt_key_id = 'sample-encrypt-key-id-for-testing'
                    bs_env = "cgap-alpha"
                    data_set = data_set_for_env(bs_env) or "prod"

                    tester(ref_ini="cgap_alpha.ini", any_ini="cg_any_alpha.ini", bs_env=bs_env, data_set=data_set,
                           use_ini_file_manager_kind="orchestrated-cgap",
                           es_server="search-fourfront-cgap-ewf7r7u2nq3xkgyozdhns4bkni.%s" % us_east,
                           line_checker=CGAPProdChecker(expect_indexer=index_default,
                                                        expect_index_server=index_server_default,
                                                        expected_values={
                                                            "file_upload_bucket": f"acme-hospital-{bs_env}-files",
                                                            "app_kind": "cgap",
                                                            "app_deployment": "orchestrated",
                                                        }),
                           application_bucket_prefix="acme-hospital-",
                           s3_bucket_env=bs_env)

                    tester(ref_ini="cgap_alpha.ini", any_ini="cg_any_alpha.ini", bs_env=bs_env, data_set=data_set,
                           use_ini_file_manager_kind="legacy-cgap",
                           es_server="search-fourfront-cgap-ewf7r7u2nq3xkgyozdhns4bkni.%s" % us_east,
                           line_checker=CGAPProdChecker(expect_indexer=index_default,
                                                        expect_index_server=index_server_default,
                                                        expected_values={
                                                            "file_upload_bucket": f"elasticbeanstalk-{bs_env}-files",
                                                            "app_kind": "cgap",
                                                            "app_deployment": "beanstalk",
                                                        }),
                           s3_bucket_env=bs_env)

                    tester(ref_ini="cgap_alpha.ini", any_ini="cg_any_alpha.ini", bs_env=bs_env, data_set=data_set,
                           use_ini_file_manager_kind='orchestrated-cgap',
                           es_server="search-fourfront-cgap-ewf7r7u2nq3xkgyozdhns4bkni.%s" % us_east,
                           line_checker=CGAPProdChecker(expect_indexer=index_default,
                                                        expect_index_server=index_server_default,
                                                        expected_values={
                                                            "file_upload_bucket": f"some-prefix-{bs_env}-files",
                                                        }),
                           application_bucket_prefix="some-prefix-",
                           s3_bucket_env=bs_env)

                    tester(ref_ini="cgap_alpha.ini", any_ini="cg_any_alpha.ini", bs_env=bs_env, data_set=data_set,
                           use_ini_file_manager_kind="orchestrated-cgap",
                           es_server="search-fourfront-cgap-ewf7r7u2nq3xkgyozdhns4bkni.%s" % us_east,
                           line_checker=CGAPProdChecker(expect_indexer=index_default,
                                                        expect_index_server=index_server_default,
                                                        expected_values={
                                                            "file_upload_bucket": f"{test_alpha_org}-{bs_env}-files",
                                                        }),
                           s3_bucket_env=bs_env,
                           s3_bucket_org=test_alpha_org,
                           s3_encrypt_key_id=test_encrypt_key_id,
                           application_bucket_prefix=f"{test_alpha_org}-")

                    tester(ref_ini="cgap_alpha.ini", any_ini="cg_any_alpha.ini", bs_env=bs_env, data_set=data_set,
                           use_ini_file_manager_kind='orchestrated-cgap',
                           es_server="search-fourfront-cgap-ewf7r7u2nq3xkgyozdhns4bkni.%s" % us_east,
                           line_checker=CGAPProdChecker(expect_indexer=index_default,
                                                        expect_index_server=index_server_default,
                                                        expected_values={
                                                            "file_upload_bucket": f"{test_alpha_org}-{bs_env}-files",
                                                        }),
                           s3_bucket_env=bs_env,
                           s3_bucket_org=test_alpha_org,
                           s3_encrypt_key_id=test_encrypt_key_id,
                           application_bucket_prefix=f"{test_alpha_org}-")

                    bs_env = "cgap-alfa"
                    tester(ref_ini="cgap_alfa.ini", any_ini="cg_any_alpha.ini", bs_env=bs_env, data_set=data_set,
                           es_server="search-fourfront-cgap-ewf7r7u2nq3xkgyozdhns4bkni.%s" % us_east,
                           use_ini_file_manager_kind='orchestrated-cgap',
                           line_checker=CGAPProdChecker(expect_indexer=index_default,
                                                        expect_index_server=index_server_default,
                                                        expected_values={
                                                            "file_upload_bucket": 'fu-bucket',
                                                            "file_wfout_bucket": 'fw-bucket',
                                                            "blob_bucket": "b-bucket",
                                                            "system_bucket": "s-bucket",
                                                            "metadata_bundles_bucket": "md-bucket",
                                                            "identity": "ThisIsMyIdentity",
                                                            "tibanna_cwls_bucket": "cwls-bucket",
                                                            "tibanna_output_bucket": "tb-bucket",
                                                            "s3_encrypt_key_id": "MyKey",
                                                        }),
                           s3_bucket_env=bs_env,
                           s3_bucket_org=test_alpha_org,
                           file_upload_bucket='fu-bucket',
                           file_wfout_bucket='fw-bucket',
                           blob_bucket='b-bucket',
                           system_bucket='s-bucket',
                           metadata_bundles_bucket='md-bucket',
                           sentry_dsn="https://123sample123sample123@o1111111.ingest.sentry.io/1234567",
                           identity='ThisIsMyIdentity',
                           auth0_client="31415926535",
                           auth0_secret="piepipiepipiepi",
                           tibanna_cwls_bucket="cwls-bucket",
                           tibanna_output_bucket="tb-bucket",
                           s3_encrypt_key_id="MyKey",
                           )

                    with override_environ(ENCODED_FILE_UPLOAD_BUCKET='decoy1',
                                          ENCODED_FILE_WFOUT_BUCKET='decoy2',
                                          ENCODED_BLOB_BUCKET='decoy3',
                                          ENCODED_SYSTEM_BUCKET='decoy4',
                                          ENCODED_METADATA_BUNDLES_BUCKET='decoy5',
                                          ENCODED_S3_BUCKET_ORG='decoy6',
                                          ENCODED_TIBANNA_OUTPUT_BUCKET='decoy7',
                                          ENCODED_TIBANNA_CWLS_BUCKET='decoy8',
                                          ENCODED_S3_ENCRYPT_KEY_ID='decoy9',
                                          ):
                        # The decoy values in the environment variables don't matter because we'll be passing
                        # explicit values for these to the builder that will take precedence.
                        tester(ref_ini="cgap_alfa.ini", any_ini="cg_any_alpha.ini", bs_env=bs_env, data_set=data_set,
                               use_ini_file_manager_kind="orchestrated-cgap",
                               es_server="search-fourfront-cgap-ewf7r7u2nq3xkgyozdhns4bkni.%s" % us_east,
                               line_checker=CGAPProdChecker(expect_indexer=index_default,
                                                            expect_index_server=index_server_default,
                                                            expected_values={
                                                                "file_upload_bucket": 'fu-bucket',
                                                                "file_wfout_bucket": 'fw-bucket',
                                                                "blob_bucket": "b-bucket",
                                                                "system_bucket": "s-bucket",
                                                                "metadata_bundles_bucket": "md-bucket",
                                                                "tibanna_cwls_bucket": "cwls-bucket",
                                                                "tibanna_output_bucket": "tb-bucket",
                                                                "identity": "ThisIsMyIdentity",
                                                                "s3_encrypt_key_id": "MyKeyId",
                                                            }),
                               s3_bucket_env=bs_env,
                               s3_bucket_org=test_alpha_org,
                               file_upload_bucket='fu-bucket',
                               file_wfout_bucket='fw-bucket',
                               blob_bucket='b-bucket',
                               system_bucket='s-bucket',
                               metadata_bundles_bucket='md-bucket',
                               tibanna_cwls_bucket="cwls-bucket",
                               tibanna_output_bucket="tb-bucket",
                               identity='ThisIsMyIdentity',
                               s3_encrypt_key_id='MyKeyId',
                               )

                    with override_environ(ENCODED_FILE_UPLOAD_BUCKET='fu-bucket',
                                          ENCODED_FILE_WFOUT_BUCKET='fw-bucket',
                                          ENCODED_BLOB_BUCKET='b-bucket',
                                          ENCODED_SYSTEM_BUCKET='s-bucket',
                                          ENCODED_METADATA_BUNDLES_BUCKET='md-bucket',
                                          ENCODED_S3_BUCKET_ORG=test_alpha_org,
                                          ENCODED_TIBANNA_OUTPUT_BUCKET='tb-bucket',
                                          ENCODED_TIBANNA_CWLS_BUCKET='cwls-bucket',
                                          ENCODED_IDENTITY='ThisIsMyIdentity',
                                          ENCODED_S3_ENCRYPT_KEY_ID='MyKeyId'):
                        # If no explicit args are passed to the builder, the ENCODED_xxx arguments DO matter.
                        tester(ref_ini="cgap_alfa.ini", any_ini="cg_any_alpha.ini", bs_env=bs_env, data_set=data_set,
                               use_ini_file_manager_kind="orchestrated-cgap",
                               es_server="search-fourfront-cgap-ewf7r7u2nq3xkgyozdhns4bkni.%s" % us_east,
                               line_checker=CGAPProdChecker(expect_indexer=index_default,
                                                            expect_index_server=index_server_default,
                                                            expected_values={
                                                                "file_upload_bucket": 'fu-bucket',
                                                                "file_wfout_bucket": 'fw-bucket',
                                                                "blob_bucket": "b-bucket",
                                                                "system_bucket": "s-bucket",
                                                                "metadata_bundles_bucket": "md-bucket",
                                                                "tibanna_cwls_bucket": "cwls-bucket",
                                                                "tibanna_output_bucket": "tb-bucket",
                                                                "identity": "ThisIsMyIdentity",
                                                                "s3_encrypt_key_id": "MyKeyId",
                                                            }),
                               s3_bucket_env=bs_env)

                    bs_env = "fourfront-cgapdev"
                    data_set = data_set_for_env(bs_env)
                    tester(ref_ini="cgapdev.ini", bs_env=bs_env, data_set=data_set,
                           es_server="search-fourfront-cgapdev-gnv2sgdngkjbcemdadmaoxcsae.%s" % us_east,
                           line_checker=Checker(expect_indexer=index_default,
                                                expect_index_server=index_server_default))

                    bs_env = "fourfront-cgaptest"
                    data_set = data_set_for_env(bs_env)
                    tester(ref_ini="cgaptest.ini", bs_env=bs_env, data_set=data_set,
                           es_server="search-fourfront-cgaptest-dxiczz2zv7f3nshshvevcvmpmy.%s" % us_east,
                           line_checker=Checker(expect_indexer=index_default,
                                                expect_index_server=index_server_default))

                    bs_env = "fourfront-cgapwolf"
                    data_set = data_set_for_env(bs_env)
                    tester(ref_ini="cgapwolf.ini", bs_env=bs_env, data_set=data_set,
                           # This ini file will have 'app_kind = ccgap' rather than 'app_kind = unknown'.
                           use_ini_file_manager_kind='legacy-cgap',
                           es_server="search-fourfront-cgapwolf-r5kkbokabymtguuwjzspt2kiqa.%s" % us_east,
                           line_checker=Checker(expect_indexer=index_default,
                                                expect_index_server=index_server_default))

                    # Fourfront uses data_set='prod' for everything but 'fourfront-mastertest',
                    # which uses data_set='test'

                    bs_env = "fourfront-blue"
                    data_set = data_set_for_env(bs_env)
                    tester(ref_ini="blue.ini", bs_env=bs_env, data_set=data_set,
                           es_server="search-fourfront-blue-xkkzdrxkrunz35shbemkgrmhku.%s" % us_east,
                           line_checker=FFProdChecker(expect_indexer=index_default,
                                                      expect_index_server=index_server_default))

                    bs_env = "fourfront-green"
                    data_set = data_set_for_env(bs_env)
                    tester(ref_ini="green.ini", bs_env=bs_env, data_set=data_set,
                           es_server="search-fourfront-green-cghpezl64x4uma3etijfknh7ja.%s" % us_east,
                           line_checker=FFProdChecker(expect_indexer=index_default,
                                                      expect_index_server=index_server_default))

                    bs_env = "fourfront-hotseat"
                    data_set = data_set_for_env(bs_env)
                    tester(ref_ini="hotseat.ini", bs_env=bs_env, data_set=data_set,
                           es_server="search-fourfront-hotseat-f3oxd2wjxw3h2wsxxbcmzhhd4i.%s" % us_east,
                           line_checker=Checker(expect_indexer=index_default,
                                                expect_index_server=index_server_default))

                    bs_env = "fourfront-mastertest"
                    data_set = data_set_for_env(bs_env)
                    tester(ref_ini="mastertest.ini", bs_env=bs_env, data_set=data_set,
                           es_server="search-fourfront-mastertest-wusehbixktyxtbagz5wzefffp4.%s" % us_east,
                           line_checker=Checker(expect_indexer=index_default,
                                                expect_index_server=index_server_default))

                    bs_env = "fourfront-webdev"
                    data_set = data_set_for_env(bs_env)
                    tester(ref_ini="webdev.ini", bs_env=bs_env, data_set=data_set,
                           es_server="search-fourfront-webdev-5uqlmdvvshqew46o46kcc2lxmy.%s" % us_east,
                           line_checker=Checker(expect_indexer=index_default,
                                                expect_index_server=index_server_default))

                    bs_env = "fourfront-webprod"
                    data_set = data_set_for_env(bs_env)
                    tester(ref_ini="webprod.ini", bs_env=bs_env, data_set=data_set,
                           es_server="search-fourfront-webprod-hmrrlalm4ifyhl4bzbvl73hwv4.%s" % us_east,
                           line_checker=FFProdChecker(expect_indexer=index_default,
                                                      expect_index_server=index_server_default))

                    bs_env = "fourfront-webprod2"
                    data_set = data_set_for_env(bs_env)
                    tester(ref_ini="webprod2.ini", bs_env=bs_env, data_set=data_set,
                           es_server="search-fourfront-webprod2-fkav4x4wjvhgejtcg6ilrmczpe.%s" % us_east,
                           line_checker=FFProdChecker(expect_indexer=index_default,
                                                      expect_index_server=index_server_default))

                    with override_environ(ENCODED_INDEXER="FALSE", ENCODED_INDEX_SERVER="TRUE"):

                        tester(ref_ini="cgap.ini", bs_env="fourfront-cgap", data_set="prod",
                               es_server="search-fourfront-cgap-ewf7r7u2nq3xkgyozdhns4bkni.%s" % us_east,
                               line_checker=CGAPProdChecker(expect_indexer=None,
                                                            expect_index_server="true"))

                        tester(ref_ini="blue.ini", bs_env="fourfront-blue", data_set="prod",
                               es_server="search-fourfront-blue-xkkzdrxkrunz35shbemkgrmhku.%s" % us_east,
                               line_checker=FFProdChecker(expect_indexer=None,
                                                          expect_index_server="true"))

                        tester(ref_ini="webdev.ini", bs_env="fourfront-webdev", data_set="prod",
                               es_server="search-fourfront-webdev-5uqlmdvvshqew46o46kcc2lxmy.%s" % us_east,
                               line_checker=Checker(expect_indexer=None,
                                                    expect_index_server="true"))

                    # === Beyond this is auot-indexer tests ===

                    # If the app version name contains the special token in AUTO_INDEX_SERVER_TOKEN,
                    # AND if there is no explicit argument OR environment variable setting,
                    # then use index_server=True.

                    # In other words, the app_version will be something like "foo__index_server",
                    # which has magic effect of becoming an index server when not specified otherwise.
                    mock_get_app_version.return_value = "foo" + TestDeployer.AUTO_INDEX_SERVER_TOKEN

                    # The next three cases test the useful case where those conditions are met.

                    tester(ref_ini="cgap.ini", bs_env="fourfront-cgap", data_set="prod",
                           es_server="search-fourfront-cgap-ewf7r7u2nq3xkgyozdhns4bkni.%s" % us_east,
                           line_checker=CGAPProdChecker(expect_indexer=index_default,
                                                        expect_index_server="true"))

                    tester(ref_ini="blue.ini", bs_env="fourfront-blue", data_set="prod",
                           es_server="search-fourfront-blue-xkkzdrxkrunz35shbemkgrmhku.%s" % us_east,
                           line_checker=FFProdChecker(expect_indexer=index_default,
                                                      expect_index_server="true"))

                    tester(ref_ini="webdev.ini", bs_env="fourfront-webdev", data_set="prod",
                           es_server="search-fourfront-webdev-5uqlmdvvshqew46o46kcc2lxmy.%s" % us_east,
                           line_checker=Checker(expect_indexer=index_default,
                                                expect_index_server="true"))

                    # The next 6 tests just test that the other cases are not perturbed.

                    with override_environ(ENCODED_INDEX_SERVER="FALSE"):

                        # This tests we can override the app version by setting an explicit env variable value.

                        tester(ref_ini="cgap.ini", bs_env="fourfront-cgap", data_set="prod",
                               es_server="search-fourfront-cgap-ewf7r7u2nq3xkgyozdhns4bkni.%s" % us_east,
                               line_checker=CGAPProdChecker(expect_indexer=index_default,
                                                            expect_index_server=None))

                        tester(ref_ini="blue.ini", bs_env="fourfront-blue", data_set="prod",
                               es_server="search-fourfront-blue-xkkzdrxkrunz35shbemkgrmhku.%s" % us_east,
                               line_checker=FFProdChecker(expect_indexer=index_default,
                                                          expect_index_server=None))

                        tester(ref_ini="webdev.ini", bs_env="fourfront-webdev", data_set="prod",
                               es_server="search-fourfront-webdev-5uqlmdvvshqew46o46kcc2lxmy.%s" % us_east,
                               line_checker=Checker(expect_indexer=index_default,
                                                    expect_index_server=None))

                    with override_environ(ENCODED_INDEX_SERVER="TRUE"):

                        # We can override the app version by setting an explicit env variable value,
                        # although we wouldn't know the difference here because it's true either way.

                        tester(ref_ini="cgap.ini", bs_env="fourfront-cgap", data_set="prod",
                               es_server="search-fourfront-cgap-ewf7r7u2nq3xkgyozdhns4bkni.%s" % us_east,
                               line_checker=CGAPProdChecker(expect_indexer=index_default,
                                                            expect_index_server="true"))

                        tester(ref_ini="blue.ini", bs_env="fourfront-blue", data_set="prod",
                               es_server="search-fourfront-blue-xkkzdrxkrunz35shbemkgrmhku.%s" % us_east,
                               line_checker=FFProdChecker(expect_indexer=index_default,
                                                          expect_index_server="true"))

                        tester(ref_ini="webdev.ini", bs_env="fourfront-webdev", data_set="prod",
                               es_server="search-fourfront-webdev-5uqlmdvvshqew46o46kcc2lxmy.%s" % us_east,
                               line_checker=Checker(expect_indexer=index_default,
                                                    expect_index_server="true"))


def test_deployment_utils_main_no_env_name():

    # If there were no ENV_NAME, nothing
    with override_environ(ENV_NAME=None):
        with mock.patch("argparse.ArgumentParser") as mock_argparser:
            def mocked_fail(*arg, **kwargs):
                ignored(arg, kwargs)
                raise AssertionError("ENV_NAME=None did not get noticed.")
            mock_argparser.side_effect = mocked_fail
            with pytest.raises(SystemExit):
                IniFileManager.main()
            assert mock_argparser.call_count == 0


def test_deployment_utils_main():

    # This is just a standard unit test that mocks out all the callouts and tests that the arguments are coming
    # in and being passed along correctly to the underlying program.

    fake_template = "something.ini"  # It doesn't matter what we use as a template for this test. we don't open it.
    with override_environ(ENV_NAME='fourfront-foo'):
        with mock.patch.object(IniFileManager, "build_ini_file_from_template") as mock_build:
            # These next two mocks are just incidental to offering help in arg parsing.
            # Those functions are tested elsewhere and are just plain bypassed here.
            with mock.patch.object(IniFileManager, "environment_template_filename", return_value=fake_template):
                with mock.patch.object(IniFileManager, "template_environment_names", return_value=["something, foo"]):

                    # This function is the core fo the testing, which just sets up a deployer to get called
                    # with an input template name and a target filename, and then calls the Deployer.
                    def check_for_mocked_build(expected_kwargs=None, expected_code=0):
                        def mocked_build(*args, **kwargs):
                            assert args == (fake_template, 'production.ini')
                            assert kwargs == (expected_kwargs or {})
                        mock_build.side_effect = mocked_build
                        try:
                            IniFileManager.main()
                        except SystemExit as e:
                            assert e.code == expected_code

                    # sys.argv gets as its first element the command name, and the rest is command line args.
                    # The '' is just an ignored command name, so [''] is no args. Command line args start with arg 1.

                    # This tests that when given no command line args, all the kwargs passed through
                    # to build_ini_file_from_template default to None.
                    with mock.patch.object(sys, "argv", ['']):
                        check_for_mocked_build({
                            'bs_env': None,
                            'bs_mirror_env': None,
                            'data_set': None,
                            'es_namespace': None,
                            'es_server': None,
                            'identity': None,
                            'index_server': None,
                            'indexer': None,
                            's3_bucket_org': None,
                            's3_bucket_env': None,
                            's3_encrypt_key_id': None,
                            'sentry_dsn': None,
                            'auth0_client': None,
                            'auth0_secret': None,
                            'file_upload_bucket': None,
                            'file_wfout_bucket': None,
                            'blob_bucket': None,
                            'system_bucket': None,
                            'metadata_bundles_bucket': None,
                            'tibanna_cwls_bucket': None,
                            'tibanna_output_bucket': None,
                            'application_bucket_prefix': None,
                            'foursight_bucket_prefix': None,
                        })

                    # Next 2 tests some sample settings, in particular the settings of indexer and index_server
                    # when given on the command line, and what gets passed through to build_ini_file_from_template.

                    with mock.patch.object(sys, "argv", ['', '--indexer', 'false', '--index_server', 'true']):
                        check_for_mocked_build({
                            'bs_env': None,
                            'bs_mirror_env': None,
                            'data_set': None,
                            'es_namespace': None,
                            'es_server': None,
                            'identity': None,
                            'index_server': 'true',
                            'indexer': 'false',
                            's3_bucket_org': None,
                            's3_bucket_env': None,
                            's3_encrypt_key_id': None,
                            'sentry_dsn': None,
                            'auth0_client': None,
                            'auth0_secret': None,
                            'file_upload_bucket': None,
                            'file_wfout_bucket': None,
                            'blob_bucket': None,
                            'system_bucket': None,
                            'metadata_bundles_bucket': None,
                            'tibanna_cwls_bucket': None,
                            'tibanna_output_bucket': None,
                            'application_bucket_prefix': None,
                            'foursight_bucket_prefix': None,
                        })

                    with mock.patch.object(sys, "argv", ['', '--indexer', 'foo']):
                        with pytest.raises(Exception):
                            check_for_mocked_build({
                                'bs_env': None,
                                'bs_mirror_env': None,
                                'data_set': None,
                                'es_namespace': None,
                                'es_server': None,
                                'identity': None,
                                'index_server': 'true',
                                'indexer': 'false',
                                's3_bucket_org': None,
                                's3_bucket_env': None,
                                's3_encrypt_key_id': None,
                                'sentry_dsn': None,
                                'auth0_client': None,
                                'auth0_secret': None,
                                'file_upload_bucket': None,
                                'file_wfout_bucket': None,
                                'blob_bucket': None,
                                'system_bucket': None,
                                'metadata_bundles_bucket': None,
                                'tibanna_cwls_bucket': None,
                                'tibanna_output_bucket': None,
                                'application_bucket_prefix': None,
                                'foursight_bucket_prefix': None,
                            })


def test_deployment_utils_boolean_setting():

    assert boolean_setting({'foo': 'true'}, 'foo') is True
    assert boolean_setting({'foo': 'false'}, 'foo') is False
    assert boolean_setting({'foo': ''}, 'foo') is False
    assert boolean_setting({'foo': None}, 'foo') is None
    assert boolean_setting({'foo': 'maybe'}, 'foo') == 'maybe'
    assert boolean_setting({}, 'foo') is None
    assert boolean_setting({}, 'foo', default='surprise') == 'surprise'


@pytest.mark.integrated
def test_eb_deployer():
    """ Tests some basic aspects of EBDeployer """
    pass  # TODO: write this test!


class MockedCreateMappingArgs(MockedCommandArgs):
    VALID_ARGS = ['wipe_es', 'skip', 'strict']


class MockedInfoLog:

    def __init__(self):
        self.last_msg = None

    def info(self, msg):
        self.last_msg = msg


def _get_deploy_config(*, env, args=None, log=None, allow_other_prod=False):
    return CreateMappingOnDeployManager.get_deploy_config(env=env,
                                                          args=args or MockedCreateMappingArgs(),
                                                          log=log or MockedInfoLog(),
                                                          client='create_mapping_on_deploy',
                                                          allow_other_prod=allow_other_prod)


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-webprod2'))
def test_get_deployment_config_ff_staging_old():
    """ Tests get_deployment_config in the old staging case """
    my_env = 'fourfront-webprod'
    my_log = MockedInfoLog()
    cfg = _get_deploy_config(env=my_env, log=my_log)
    assert cfg['ENV_NAME'] == my_env  # sanity
    assert cfg['SKIP'] is False
    assert cfg['WIPE_ES'] is True
    assert cfg['STRICT'] is True
    assert my_log.last_msg == ("Environment fourfront-webprod is currently the staging environment."
                               " Processing mode: STRICT,WIPE_ES")


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-green'))
def test_get_deployment_config_ff_staging_new():
    """ Tests get_deployment_config in the new staging case """
    my_env = 'fourfront-blue'
    my_log = MockedInfoLog()
    cfg = _get_deploy_config(env=my_env, log=my_log)
    assert cfg['ENV_NAME'] == my_env  # sanity
    assert cfg['SKIP'] is False
    assert cfg['WIPE_ES'] is True
    assert cfg['STRICT'] is True
    assert my_log.last_msg == ("Environment fourfront-blue is currently the staging environment."
                               " Processing mode: STRICT,WIPE_ES")


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-webprod2'))
def test_get_deployment_config_ff_prod_old():
    """ Tests get_deployment_config in the old production case """
    my_env = 'fourfront-webprod2'
    my_log = MockedInfoLog()
    with pytest.raises(RuntimeError):
        _get_deploy_config(env=my_env, log=my_log)
    assert my_log.last_msg == ("Environment fourfront-webprod2 is currently the production environment."
                               " Something is definitely wrong. We never deploy there, we always CNAME swap."
                               " This deploy cannot proceed. DeploymentFailure will be raised.")


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-green'))
def test_get_deployment_config_ff_prod_new():
    """ Tests get_deployment_config in the new production case """
    my_env = 'fourfront-green'
    my_log = MockedInfoLog()
    with pytest.raises(RuntimeError):
        _get_deploy_config(env=my_env, log=my_log)
    assert my_log.last_msg == ("Environment fourfront-green is currently the production environment."
                               " Something is definitely wrong. We never deploy there, we always CNAME swap."
                               " This deploy cannot proceed. DeploymentFailure will be raised.")


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-green'))
def test_get_deployment_config_ff_prod_uncorrelated():
    """ Tests get_deployment_config in the new production case """
    my_env = 'fourfront-webprod2'
    my_log = MockedInfoLog()
    with pytest.raises(RuntimeError):
        _get_deploy_config(env=my_env, log=my_log)
    assert my_log.last_msg == ("Environment fourfront-webprod2 is an uncorrelated production-class environment."
                               " Something is definitely wrong. This deploy cannot proceed."
                               " DeploymentFailure will be raised.")


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-green'))
def test_get_deployment_config_ff_prod_uncorrelated_but_allowed():
    """ Tests get_deployment_config in the new production case """
    my_env = 'fourfront-webprod2'
    my_log = MockedInfoLog()
    cfg = _get_deploy_config(env=my_env, log=my_log, allow_other_prod=True)
    assert cfg['ENV_NAME'] == my_env  # sanity
    assert cfg['SKIP'] is True  # If SKIP is returned, the other values don't really matter.
    # assert cfg['WIPE_ES'] is ...
    # assert cfg['STRICT'] is ...
    assert my_log.last_msg == ("Environment fourfront-webprod2 is an uncorrelated production-class environment"
                               " (neither production nor its staging mirror). Processing mode: SKIP")


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-green'))
def test_get_deployment_config_ff_prod_uncorrelated_but_allowed_with_args():
    """ Tests get_deployment_config in the new production case """
    my_env = 'fourfront-webprod2'
    my_log = MockedInfoLog()
    cfg = _get_deploy_config(env=my_env,
                             # These args can't be turned off, only on, so really none have effect
                             # because skip is defaultly on and cannot be disabled,
                             # and the rest don't matter if skip is True. -kmp 24-Apr-2022
                             args=MockedCreateMappingArgs(skip=False, wipe_es=False, strict=True),
                             log=my_log,
                             allow_other_prod=True)
    assert cfg['ENV_NAME'] == my_env  # sanity
    assert cfg['SKIP'] is True  # If SKIP is returned, the other values don't really matter.
    # assert cfg['WIPE_ES'] is ...
    # assert cfg['STRICT'] is ...
    assert my_log.last_msg == ("Environment fourfront-webprod2 is an uncorrelated production-class environment"
                               " (neither production nor its staging mirror). Processing mode: SKIP")


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-webprod2'))
def test_get_deployment_config_ff_mastertest_old():
    """ Tests get_deployment_config in the mastertest case with an old-style ecosystem. """
    my_env = 'fourfront-mastertest'
    my_log = MockedInfoLog()
    cfg = _get_deploy_config(env=my_env, log=my_log)
    assert cfg['ENV_NAME'] == my_env  # sanity
    assert cfg['SKIP'] is False
    assert cfg['WIPE_ES'] is True
    assert cfg['STRICT'] is False
    assert my_log.last_msg == ('Environment fourfront-mastertest is a non-hotseat test environment.'
                               ' Processing mode: WIPE_ES')


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-webprod2'))
def test_get_deployment_config_ff_mastertest_old_with_args():
    """ Tests get_deployment_config in the mastertest case with an old-style ecosystem. """
    my_env = 'fourfront-mastertest'
    my_log = MockedInfoLog()
    cfg = _get_deploy_config(env=my_env,
                             # These args can't be turned off, only on, so only the skip and strict args have effect.
                             args=MockedCreateMappingArgs(skip=True, wipe_es=False, strict=True),
                             log=my_log)
    assert cfg['ENV_NAME'] == my_env  # sanity
    assert cfg['SKIP'] is True
    assert cfg['WIPE_ES'] is True  # An arg to create mapping cannot disable the environment's default
    assert cfg['STRICT'] is True
    assert my_log.last_msg == ('Environment fourfront-mastertest is a non-hotseat test environment.'
                               ' Processing mode: SKIP')


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-green'))
def test_get_deployment_config_ff_mastertest_new():
    """ Tests get_deployment_config in the mastertest case with a new-style ecosystem. """
    my_env = 'fourfront-mastertest'
    my_log = MockedInfoLog()
    cfg = _get_deploy_config(env=my_env, log=my_log)
    assert cfg['ENV_NAME'] == my_env  # sanity
    assert cfg['SKIP'] is False
    assert cfg['WIPE_ES'] is True
    assert cfg['STRICT'] is False
    assert my_log.last_msg == ('Environment fourfront-mastertest is a non-hotseat test environment.'
                               ' Processing mode: WIPE_ES')


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-green'))
def test_get_deployment_config_ff_mastertest_new_with_args():
    """ Tests get_deployment_config in the mastertest case with a new-style ecosystem. """
    my_env = 'fourfront-mastertest'
    my_log = MockedInfoLog()
    cfg = _get_deploy_config(env=my_env,
                             # These args can't be turned off, only on, so only the skip and strict args have effect.
                             args=MockedCreateMappingArgs(skip=True, wipe_es=False, strict=True),
                             log=my_log)
    assert cfg['ENV_NAME'] == my_env  # sanity
    assert cfg['SKIP'] is True
    assert cfg['WIPE_ES'] is True  # An arg to create mapping cannot disable the environment's default
    assert cfg['STRICT'] is True
    assert my_log.last_msg == ('Environment fourfront-mastertest is a non-hotseat test environment.'
                               ' Processing mode: SKIP')


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-webprod2'))
def test_get_deployment_config_ff_hotseat_old():
    """ Tests get_deployment_config in the hotseat case with an old-style ecosystem. """
    my_env = 'fourfront-hotseat'
    my_log = MockedInfoLog()
    cfg = _get_deploy_config(env=my_env, log=my_log)
    assert cfg['ENV_NAME'] == my_env  # sanity
    assert cfg['SKIP'] is True  # If SKIP is returned, the other values don't really matter.
    # assert cfg['WIPE_ES'] is ...
    # assert cfg['STRICT'] is ...
    assert my_log.last_msg == ('Environment fourfront-hotseat is a hotseat test environment.'
                               ' Processing mode: SKIP')


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-webprod2'))
def test_get_deployment_config_ff_hotseat_old_with_args():
    """ Tests get_deployment_config in the hotseat case with an old-style ecosystem. """
    my_env = 'fourfront-hotseat'
    my_log = MockedInfoLog()
    cfg = _get_deploy_config(env=my_env,
                             # These args can't be turned off, only on, so really none have effect
                             # because skip is defaultly on and cannot be disabled,
                             # and the rest don't matter if skip is True. -kmp 24-Apr-2022
                             args=MockedCreateMappingArgs(skip=False, wipe_es=False, strict=True),
                             log=my_log)
    assert cfg['ENV_NAME'] == my_env  # sanity
    assert cfg['SKIP'] is True  # If SKIP is returned, the other values don't really matter.
    # assert cfg['WIPE_ES'] is ...
    # assert cfg['STRICT'] is ...
    assert my_log.last_msg == ('Environment fourfront-hotseat is a hotseat test environment.'
                               ' Processing mode: SKIP')


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-green'))
def test_get_deployment_config_ff_hotseat_new():
    """ Tests get_deployment_config in the hotseat case with a new-style ecosystem. """
    my_env = 'fourfront-hotseat'
    my_log = MockedInfoLog()
    cfg = _get_deploy_config(env=my_env, log=my_log)
    assert cfg['ENV_NAME'] == my_env  # sanity
    assert cfg['SKIP'] is True  # If SKIP is returned, the other values don't really matter.
    # assert cfg['WIPE_ES'] is ...
    # assert cfg['STRICT'] is ...
    assert my_log.last_msg == ('Environment fourfront-hotseat is a hotseat test environment.'
                               ' Processing mode: SKIP')


@mock.patch('dcicutils.deployment_utils.compute_ff_prd_env', mock.MagicMock(return_value='fourfront-green'))
def test_get_deployment_config_ff_hotseat_new_with_args():
    """ Tests get_deployment_config in the hotseat case with a new-style ecosystem. """
    my_env = 'fourfront-hotseat'
    my_log = MockedInfoLog()
    cfg = _get_deploy_config(env=my_env,
                             # These args can't be turned off, only on, so really none have effect
                             # because skip is defaultly on and cannot be disabled,
                             # and the rest don't matter if skip is True. -kmp 24-Apr-2022
                             args=MockedCreateMappingArgs(skip=False, wipe_es=False, strict=True),
                             log=my_log)
    assert cfg['ENV_NAME'] == my_env  # sanity
    assert cfg['SKIP'] is True  # If SKIP is returned, the other values don't really matter.
    # assert cfg['WIPE_ES'] is ...
    # assert cfg['STRICT'] is ...
    assert my_log.last_msg == ('Environment fourfront-hotseat is a hotseat test environment.'
                               ' Processing mode: SKIP')


# There is no old-style cgap staging

# Eventually cgap staging will look like this.
@mock.patch('dcicutils.deployment_utils.compute_cgap_prd_env', mock.MagicMock(return_value='cgap-green'))
def test_get_deployment_config_cgap_staging_new():
    """ Tests get_deployment_config in the new staging case """
    my_env = 'cgap-blue'
    my_log = MockedInfoLog()
    cfg = _get_deploy_config(env=my_env, log=my_log)
    assert cfg['ENV_NAME'] == my_env  # sanity
    assert cfg['SKIP'] is False
    assert cfg['WIPE_ES'] is True
    assert cfg['STRICT'] is True
    assert my_log.last_msg == ('Environment cgap-blue is currently the staging environment.'
                               ' Processing mode: STRICT,WIPE_ES')


@mock.patch('dcicutils.deployment_utils.compute_cgap_prd_env', mock.MagicMock(return_value='fourfront-cgap'))
def test_get_deployment_config_cgap_prod_old():
    """ Tests get_deployment_config in the old production case """
    my_env = 'fourfront-cgap'
    my_log = MockedInfoLog()
    with pytest.raises(RuntimeError):
        _get_deploy_config(env=my_env, log=my_log)
    assert my_log.last_msg == ("Environment fourfront-cgap is currently the production environment."
                               " Something is definitely wrong. We never deploy there, we always CNAME swap."
                               " This deploy cannot proceed. DeploymentFailure will be raised.")


# Eventually cgap production will look like this.
@mock.patch('dcicutils.deployment_utils.compute_cgap_prd_env', mock.MagicMock(return_value='cgap-green'))
def test_get_deployment_config_cgap_prod_new():
    """ Tests get_deployment_config in the new production case """
    my_env = 'cgap-green'
    my_log = MockedInfoLog()
    with pytest.raises(RuntimeError):
        _get_deploy_config(env=my_env, log=my_log)
    assert my_log.last_msg == ("Environment cgap-green is currently the production environment."
                               " Something is definitely wrong. We never deploy there, we always CNAME swap."
                               " This deploy cannot proceed. DeploymentFailure will be raised.")


@mock.patch('dcicutils.deployment_utils.compute_cgap_prd_env', mock.MagicMock(return_value='fourfront-cgap'))
def test_get_deployment_config_cgap_prod_uncorrelated():
    """ Tests get_deployment_config in the new production case """
    my_env = 'cgap-blue'
    my_log = MockedInfoLog()
    with pytest.raises(RuntimeError):
        _get_deploy_config(env=my_env, log=my_log)
    assert my_log.last_msg == ("Environment cgap-blue is an uncorrelated production-class environment."
                               " Something is definitely wrong. This deploy cannot proceed."
                               " DeploymentFailure will be raised.")


@mock.patch('dcicutils.deployment_utils.compute_cgap_prd_env', mock.MagicMock(return_value='fourfront-cgap'))
def test_get_deployment_config_cgap_prod_uncorrelated_but_allowed():
    """ Tests get_deployment_config in the new production case """
    my_env = 'cgap-blue'
    my_log = MockedInfoLog()
    cfg = _get_deploy_config(env=my_env, log=my_log, allow_other_prod=True)
    assert cfg['ENV_NAME'] == my_env  # sanity
    assert cfg['SKIP'] is True  # If SKIP is returned, the other values don't really matter.
    # assert cfg['WIPE_ES'] is ...
    # assert cfg['STRICT'] is ...
    assert my_log.last_msg == ("Environment cgap-blue is an uncorrelated production-class environment"
                               " (neither production nor its staging mirror). Processing mode: SKIP")


def test_create_file_from_template():

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove():

        with io.open("my.template", "w") as fp:
            fp.write('{\n "x": "$X",\n "y": "$Y",\n "xy": "($X,$Y)",\n "another": "value"\n}')

        # If all lines from the above-written expression get written, this will be the JSON it describes:
        full_expectation = {"x": "1", "y": "2", "xy": "(1,2)", "another": "value"}

        variables = {"X": "1", "Y": "2"}

        s = StringIO()
        create_file_from_template("my.template", to_stream=s, extra_environment_variables=variables)
        assert json.loads(s.getvalue()) == full_expectation

        assert os.path.exists("my.template")
        assert not os.path.exists("my.file")

        create_file_from_template("my.template", to_file="my.file", extra_environment_variables=variables)
        assert os.path.exists("my.file")
        assert json.loads(file_contents("my.file")) == full_expectation

        def line_contains_1(line, expanded):
            ignored(expanded)
            return "1" in line

        def expanded_contains_1(line, expanded):
            ignored(line)
            return "1" in expanded

        os.remove("my.file")
        assert not os.path.exists("my.file")

        create_file_from_template("my.template", to_file="my.file", extra_environment_variables=variables,
                                  omittable=line_contains_1)
        assert os.path.exists("my.file")
        filtered = file_contents("my.file")
        assert json.loads(filtered) == full_expectation  # NOTE: The "1" is not visible until an expansion

        with printed_output() as printed:

            os.remove("my.file")
            assert not os.path.exists("my.file")
            create_file_from_template("my.template", to_file="my.file", extra_environment_variables=variables,
                                      omittable=expanded_contains_1, warn_if_changed="my.file changed")
            assert os.path.exists("my.file")
            filtered = file_contents("my.file")
            assert json.loads(filtered) == {"y": "2", "another": "value"}
            assert printed.lines == []

        with printed_output() as printed:

            create_file_from_template("my.template", to_file="my.file", extra_environment_variables=variables,
                                      omittable=line_contains_1, warn_if_changed="my.file changed")
            assert os.path.exists("my.file")
            filtered = file_contents("my.file")
            assert json.loads(filtered) == full_expectation  # NOTE: The "1" is not visible until an expansion
            assert printed.lines == ["Warning: my.file changed"]


def test_add_argparse_arguments():
    """Test that calling our .add_argparse_arguments() function actually adds some arguments."""
    parser = argparse.ArgumentParser(description="sample parser", formatter_class=argparse.RawDescriptionHelpFormatter)
    assert parser.parse_args([]) == argparse.Namespace()
    CreateMappingOnDeployManager.add_argparse_arguments(parser=parser)
    assert parser.parse_args([]) == argparse.Namespace(skip=False, wipe_es=False, strict=False)
