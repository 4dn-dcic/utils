import datetime
import os

import pytest
import re
import subprocess

from contextlib import contextmanager
from io import StringIO
from unittest import mock

from dcicutils.deployment_utils import Deployer
from dcicutils.env_utils import is_cgap_env
from dcicutils.misc_utils import ignored
from dcicutils.qa_utils import override_environ


_MY_DIR = os.path.dirname(__file__)


class TestDeployer(Deployer):
    TEMPLATE_DIR = os.path.join(_MY_DIR, "ini_files")
    PYPROJECT_FILE_NAME = os.path.join(os.path.dirname(_MY_DIR), "pyproject.toml")


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
MOCKED_BUNDLE_VERSION = 'v-12345-bundle-version'
MOCKED_LOCAL_GIT_VERSION = 'v-67890-git-version'
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
        #       is inside the pretense that we have a file system at all. But it will suffice for now. -kmp 27-Apr-2020
        def mocked_open(filename, mode='r', encoding=None):
            """
            On read (mode=='r'), this simulates the presence of several files in an ad hoc way, not by mock file system.
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
                        'DATABASE = "${RDS_DB_NAME}"\n'
                        'SOME_URL = "http://${RDS_USERNAME}@$RDS_HOSTNAME:$RDS_PORT/"\n'
                        'OOPS = "$NOT_AN_ENV_VAR"\n'
                        'HMMM = "${NOT_AN_ENV_VAR_EITHER}"\n'
                        'SHHH = "$RDS_PASSWORD"\n'
                        'VERSION = "${APP_VERSION}"\n'
                        'PROJECT_VERSION = "${PROJECT_VERSION}"\n'
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
                    # Here's where we finally call the builder. Output will be a list of lines in the mock file system.
                    TestDeployer.build_ini_file_from_template(some_template_file_name, some_ini_file_name)

        # The subtle thing here is that if it were a multi-line string,
        # all the "%" substitutions would have to be on the final line, not line-by-line where needed.
        assert MockFileStream.FILE_SYSTEM[some_ini_file_name] == [
            '[Foo]',
            'DATABASE = "snow_white"',
            'SOME_URL = "http://user@unittest:6543/"',
            'OOPS = "$NOT_AN_ENV_VAR"',
            'HMMM = "${NOT_AN_ENV_VAR_EITHER}"',
            'SHHH = "my-secret"',
            'VERSION = "%s"' % MOCKED_BUNDLE_VERSION,
            'PROJECT_VERSION = "%s"' % MOCKED_PROJECT_VERSION,
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
            'DATABASE = "snow_white"',
            'SOME_URL = "http://user@unittest:6543/"',
            'OOPS = "$NOT_AN_ENV_VAR"',
            'HMMM = "${NOT_AN_ENV_VAR_EITHER}"',
            'SHHH = "my-secret"',
            'VERSION = "%s"' % MOCKED_LOCAL_GIT_VERSION,  # This is the result of no manifest file existing
            'PROJECT_VERSION = "%s"' % MOCKED_PROJECT_VERSION,
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
            'DATABASE = "snow_white"',
            'SOME_URL = "http://user@unittest:6543/"',
            'OOPS = "$NOT_AN_ENV_VAR"',
            'HMMM = "${NOT_AN_ENV_VAR_EITHER}"',
            'SHHH = "my-secret"',
            'VERSION = "unknown-version-at-20010203045506000000"',  # We mocked datetime.datetime.now() to get this
            'PROJECT_VERSION = "%s"' % MOCKED_PROJECT_VERSION,
        ]

        MockFileStream.reset()

        # For this test, we check if the 'indexer' option being set correctly sets the ENCODED.INDEXER option
        with mock.patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True
            with mock.patch("io.open", side_effect=mocked_open):
                TestDeployer.build_ini_file_from_template(some_template_file_name, some_ini_file_name, indexer=True)

        assert MockFileStream.FILE_SYSTEM[some_ini_file_name] == [
            '[Foo]',
            'DATABASE = "snow_white"',
            'SOME_URL = "http://user@unittest:6543/"',
            'OOPS = "$NOT_AN_ENV_VAR"',
            'HMMM = "${NOT_AN_ENV_VAR_EITHER}"',
            'SHHH = "my-secret"',
            'VERSION = "v-12345-bundle-version"',
            'PROJECT_VERSION = "11.22.33"',
            'ENCODED.INDEXER = "true"'
        ]

        MockFileStream.reset()

        with mock.patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True
            with mock.patch("io.open", side_effect=mocked_open):

                # for this test, we're going to pretend we deployed with bs_name == 'fourfront-indexer',
                # which should throw an exception
                def mocked_os_get(val, default):
                    if val == 'ENCODED_BS_ENV':
                        return 'fourfront-indexer'
                    else:
                        return default

                with mock.patch("os.environ.get", side_effect=mocked_os_get):
                    with pytest.raises(RuntimeError):
                        TestDeployer.build_ini_file_from_template(some_template_file_name,
                                                                  some_ini_file_name, indexer=True)

        # Uncomment this for debugging...
        # assert False, "PASSED"


def test_deployment_utils_get_app_version():

    with mock.patch('subprocess.check_output') as mock_check_output:

        with mock.patch("os.path.exists") as mock_exists:
            mock_exists.return_value = True
            with mock.patch("io.open") as mock_open:
                mock_open.return_value = StringIO('{"VersionLabel": "%s"}' % MOCKED_BUNDLE_VERSION)
                mock_check_output.side_effect = make_mocked_check_output_for_get_version()
                assert TestDeployer.get_app_version() == MOCKED_BUNDLE_VERSION

        mock_check_output.side_effect = make_mocked_check_output_for_get_version()
        assert TestDeployer.get_app_version() == MOCKED_LOCAL_GIT_VERSION

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
        mock_exists.return_value = False
        with mock.patch("io.open") as mock_open:
            def mocked_open_error(filename, mode='r'):
                ignored(filename, mode)
                raise Exception("Simulated file error (file not found or permissions problem).")
            mock_open.side_effect = mocked_open_error
            assert TestDeployer.get_eb_bundled_version() is None


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

    def tester(ref_ini, bs_env, data_set, es_server, es_namespace=None):

        assert ref_ini[:-4] == bs_env[10:]  # "xxx.ini" needs to match "fourfront-xxx"

        es_namespace = es_namespace or bs_env

        # Test of build_ini_from_template with just 2 keyword arguments explicitly supplied (bs_env, es_server),
        # and others defaulted.

        old_output = StringIO()
        new_output = StringIO()

        any_ini = os.path.join(TestDeployer.TEMPLATE_DIR, "cg_any.ini" if is_cgap_env(bs_env) else "ff_any.ini")

        TestDeployer.build_ini_stream_from_template(os.path.join(TestDeployer.TEMPLATE_DIR, ref_ini), old_output)
        TestDeployer.build_ini_stream_from_template(any_ini, new_output,
                                                    # data_env & es_namespace are something we should be able to default
                                                    bs_env=bs_env, es_server=es_server)

        old_content = old_output.getvalue()
        new_content = new_output.getvalue()
        assert old_content == new_content

        # Test of build_ini_from_template with all 4 keyword arguments explicitly supplied (bs_env, data_set,
        # es_server, es_namespace), none defaulted.

        old_output = StringIO()
        new_output = StringIO()

        TestDeployer.build_ini_stream_from_template(os.path.join(TestDeployer.TEMPLATE_DIR, ref_ini), old_output)
        TestDeployer.build_ini_stream_from_template(any_ini, new_output,
                                                    bs_env=bs_env, data_set=data_set,
                                                    es_server=es_server, es_namespace=es_namespace)

        old_content = old_output.getvalue()
        new_content = new_output.getvalue()
        assert old_content == new_content

    with mock.patch.object(TestDeployer, "get_app_version", return_value=MOCKED_PROJECT_VERSION):
        with mock.patch("toml.load", return_value={"tool": {"poetry": {"version": MOCKED_LOCAL_GIT_VERSION}}}):

            # CGAP uses data_set='prod' for 'fourfront-cgap' and data_set='test' for all others.

            tester(ref_ini="cgap.ini", bs_env="fourfront-cgap", data_set="prod",
                   es_server="search-fourfront-cgap-ewf7r7u2nq3xkgyozdhns4bkni.us-east-1.es.amazonaws.com:80")

            tester(ref_ini="cgapdev.ini", bs_env="fourfront-cgapdev", data_set="test",
                   es_server="search-fourfront-cgapdev-gnv2sgdngkjbcemdadmaoxcsae.us-east-1.es.amazonaws.com:80")

            tester(ref_ini="cgaptest.ini", bs_env="fourfront-cgaptest", data_set="test",
                   es_server="search-fourfront-cgaptest-dxiczz2zv7f3nshshvevcvmpmy.us-east-1.es.amazonaws.com:80")

            tester(ref_ini="cgapwolf.ini", bs_env="fourfront-cgapwolf", data_set="test",
                   es_server="search-fourfront-cgapwolf-r5kkbokabymtguuwjzspt2kiqa.us-east-1.es.amazonaws.com:80")

            # Fourfront uses data_set='prod' for everything but 'fourfront-mastertest', which uses data_set='test'

            tester(ref_ini="webprod.ini", bs_env="fourfront-webprod", data_set="prod",
                   es_server="search-fourfront-webprod-hmrrlalm4ifyhl4bzbvl73hwv4.us-east-1.es.amazonaws.com:80")

            tester(ref_ini="webprod2.ini", bs_env="fourfront-webprod2", data_set="prod",
                   es_server="search-fourfront-webprod2-fkav4x4wjvhgejtcg6ilrmczpe.us-east-1.es.amazonaws.com:80")

            tester(ref_ini="blue.ini", bs_env="fourfront-blue", data_set="prod",
                   es_server="search-fourfront-blue-xkkzdrxkrunz35shbemkgrmhku.us-east-1.es.amazonaws.com:80")

            tester(ref_ini="green.ini", bs_env="fourfront-green", data_set="prod",
                   es_server="search-fourfront-green-cghpezl64x4uma3etijfknh7ja.us-east-1.es.amazonaws.com:80")

            tester(ref_ini="webdev.ini", bs_env="fourfront-webdev", data_set="prod",
                   es_server="search-fourfront-webdev-5uqlmdvvshqew46o46kcc2lxmy.us-east-1.es.amazonaws.com:80")

            tester(ref_ini="mastertest.ini", bs_env="fourfront-mastertest", data_set="test",
                   es_server="search-fourfront-mastertest-wusehbixktyxtbagz5wzefffp4.us-east-1.es.amazonaws.com:80")

