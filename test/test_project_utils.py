import contextlib
import io
import os
import pytest

from dcicutils.misc_utils import ignorable, override_environ, local_attrs
from dcicutils.qa_utils import MockFileSystem
from dcicutils.project_utils import (
    ProjectIdentity, C4ProjectIdentity, Project, C4Project, ProjectRegistry, C4ProjectRegistry,
)


@contextlib.contextmanager
def project_registry_test_context(registry=True):
    attrs = {'APPLICATION_PROJECT_HOME': None, 'PYPROJECT_TOML_FILE': None, 'PYPROJECT_TOML': None,
             'POETRY_DATA': None, '_app_project': None, '_PYPROJECT_NAME': None}
    if registry:
        attrs['REGISTERED_PROJECTS'] = {}
    with local_attrs(ProjectRegistry, **attrs):
        yield


def test_project_registry_register():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            @ProjectRegistry.register('foo')
            class FooProject(Project):
                IDENTITY = {"NAME": "foo", "PRETTY_NAME": "Fu"}

            foo_project = FooProject()

            assert foo_project.NAME == 'foo'
            assert foo_project.PRETTY_NAME == 'Fu'

            assert foo_project.PYPROJECT_NAME == 'foo'

            @ProjectRegistry.register('foobar')
            class FooBarProject(Project):
                IDENTITY = {"NAME": "foobar"}

            foobar_project = FooBarProject()

            assert foobar_project.NAME == 'foobar'
            assert foobar_project.PRETTY_NAME == 'Foobar'

            assert foobar_project.PYPROJECT_NAME == 'foobar'

            with pytest.raises(Exception) as exc:
                @ProjectRegistry.register('foo')
                class FooProject:
                    IDENTITY = {"NAME": 'foo'}
                ignorable(FooProject)  # It won't get this far.
            assert str(exc.value) == "The class FooProject must inherit from Project."

            with pytest.raises(Exception) as exc:
                @C4ProjectRegistry.register('foo')
                class FooProject(Project):
                    IDENTITY = {"NAME": 'foo'}
                ignorable(FooProject)  # It won't get this far.
            assert str(exc.value) == "The class FooProject must inherit from C4Project."

            with pytest.raises(Exception) as exc:
                @ProjectRegistry.register('foo')
                class FooProject(Project):
                    IDENTITY = {}
                ignorable(FooProject)  # It won't get this far.
            assert str(exc.value) == "Declaration of FooProject must have a non-empty IDENTITY= specification."

            with pytest.raises(Exception) as exc:
                @ProjectRegistry.register('foo')
                class FooProject(Project):
                    IDENTITY = {"APP_NAME": 'foo'}
                ignorable(FooProject)  # It won't get this far.
            assert str(exc.value) == "Declaration of FooProject IDENTITY= is missing 'NAME'."

            with pytest.raises(Exception) as exc:
                @ProjectRegistry.register('foo')
                class FooProject(Project):
                    IDENTITY = {"PYPROJECT_NAME": 'foox'}
                ignorable(FooProject)  # It won't get this far.
            assert str(exc.value) == ("Explicitly specifying IDENTITY={'PYPROJECT_NAME': ...} is not allowed."
                                      " The PYPROJECT_NAME is managed implicitly"
                                      " using information given in the ProjectRegistry.register decorator.")

            with pytest.raises(Exception) as exc:
                @ProjectRegistry.register('foobar')
                class FooProject(Project):
                    IDENTITY = {"PYPROJECT_NAME": 'foox'}
                ignorable(FooProject)  # It won't get this far.
            assert str(exc.value) == ("Explicitly specifying IDENTITY={'PYPROJECT_NAME': ...} is not allowed."
                                      " The PYPROJECT_NAME is managed implicitly"
                                      " using information given in the ProjectRegistry.register decorator.")


def test_project_registry_register_snovault_scenario():

    # These values don't have to be precisely what snovault uses, just plausibly things it might

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            # Initialization code expects this filename to be precise
            curdir = os.path.abspath(os.curdir)
            pyproject_file = os.path.join(curdir, "pyproject.toml")

            with io.open(pyproject_file, 'w') as fp:
                fp.write(
                    '[tool.poetry]\n'
                    'name = "dcicsnovault"\n'
                    'packages = [{include="snovault", from="."}]\n')
                # ProjectRegistry.POETRY_DATA = {
                #     'name': 'dcicsnovault',
                #     'packages': [{'include': 'snovault', 'from': '.'}]
                # }

            @ProjectRegistry.register('dcicsnovault')
            class SnovaultProject(C4Project):
                IDENTITY = {"NAME": 'snovault', "PYPI_NAME": 'dcicsnovault'}
                ACCESSION_PREFIX = 'SNO'

            def test_it(package_name):

                # There are special cases for the cache and for package_name here because the only way
                # snovault knows to be dcicsnovault vs snovault is by looking in the pyproject.toml and
                # seeing that the packages info is different. -kmp 18-May-2023

                app_project = SnovaultProject.app_project_maker()

                assert isinstance(app_project(), SnovaultProject)

                app_project().__class__._PACKAGE_NAME = None  # NoQA - de-cache project

                with project_registry_test_context(registry=False):  # flush registry cache

                    assert app_project().ACCESSION_PREFIX == 'SNO'
                    assert app_project().APP_NAME == 'snovault'
                    assert app_project().APP_PRETTY_NAME == 'Snovault'
                    assert app_project().NAME == 'snovault'
                    assert app_project().PACKAGE_NAME == package_name
                    assert app_project().PRETTY_NAME == 'Snovault'
                    assert app_project().PYPI_NAME == 'dcicsnovault'
                    assert app_project().PYPROJECT_NAME == 'dcicsnovault'
                    assert app_project().REPO_NAME == 'snovault'

            test_it('snovault')

            mfs.remove(pyproject_file)

            print(f"Removed {pyproject_file}")

            with override_environ(APPLICATION_PYPROJECT_NAME='dcicsnovault'):
                test_it('dcicsnovault')


def test_project_registry_register_cgap_scenario():

    # These values don't have to be precisely what snovault and cgap-portal use, just plausibly things they might

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            # Initialization code expects this filename to be precise
            curdir = os.path.abspath(os.curdir)
            pyproject_file = os.path.join(curdir, "pyproject.toml")

            with io.open(pyproject_file, 'w') as fp:
                fp.write(
                    '[tool.poetry]\n'
                    'name = "encoded"\n'
                    'packages = [{include="encoded", from="src"}]\n')

            # ProjectRegistry.POETRY_DATA = {
            #     'name': 'encoded',
            #     'packages': [{'include': 'encoded', 'from': '.'}]
            # }

            @ProjectRegistry.register('dcicsnovault')
            class SnovaultProject(C4Project):
                IDENTITY = {"NAME": 'snovault', "PYPI_NAME": 'dcicsnovault'}
                ACCESSION_PREFIX = 'SNO'

            @ProjectRegistry.register('encoded')
            class CGAPProject(SnovaultProject):
                IDENTITY = {"NAME": 'cgap-portal'}
                ACCESSION_PREFIX = 'GAP'

            def test_it():

                app_project = CGAPProject.app_project_maker()

                assert isinstance(app_project(), CGAPProject)

                app_project().__class__._PACKAGE_NAME = None  # de-cache

                assert app_project().ACCESSION_PREFIX == 'GAP'
                assert app_project().APP_NAME == 'cgap'
                assert app_project().APP_PRETTY_NAME == 'CGAP'
                assert app_project().NAME == 'cgap-portal'
                assert app_project().PACKAGE_NAME == 'encoded'
                assert app_project().PRETTY_NAME == 'CGAP Portal'
                assert app_project().PYPI_NAME is None
                assert app_project().PYPROJECT_NAME == 'encoded'
                assert app_project().REPO_NAME == 'cgap-portal'

            test_it()

            mfs.remove(pyproject_file)

            with override_environ(APPLICATION_PYPROJECT_NAME='encoded'):
                test_it()


def test_project_identity_prettify():

    assert ProjectIdentity.prettify('foo') == 'Foo'
    assert ProjectIdentity.prettify('foo-bar') == 'Foo Bar'

    assert ProjectIdentity.prettify('cgap-portal') == 'Cgap Portal'
    assert ProjectIdentity.prettify('smaht-portal') == 'Smaht Portal'


def test_c4_project_identity_prettify():

    assert C4ProjectIdentity.prettify('foo') == 'Foo'
    assert C4ProjectIdentity.prettify('foo-bar') == 'Foo Bar'

    assert C4ProjectIdentity.prettify('cgap-portal') == 'CGAP Portal'
    assert C4ProjectIdentity.prettify('smaht-portal') == 'SMaHT Portal'


def test_project_identity_appify():

    # Just an identity function in the ProjectIdentity class

    assert ProjectIdentity.appify('snovault') == 'snovault'
    assert ProjectIdentity.appify('dcicsnovault') == 'dcicsnovault'
    assert ProjectIdentity.appify('cgap-portal') == 'cgap-portal'
    assert ProjectIdentity.appify('encoded-core') == 'encoded-core'


def test_c4_project_identity_appify():

    assert C4ProjectIdentity.appify('snovault') == 'snovault'
    assert C4ProjectIdentity.appify('dcicsnovault') == 'dcicsnovault'

    assert C4ProjectIdentity.appify('cgap-portal') == 'cgap'
    assert C4ProjectIdentity.appify('encoded-core') == 'core'


def test_project_identity_repofy():

    # Just an identity function in the ProjectIdentity class

    assert ProjectIdentity.repofy('snovault') == 'snovault'
    assert ProjectIdentity.repofy('dcicsnovault') == 'dcicsnovault'
    assert ProjectIdentity.repofy('cgap-portal') == 'cgap-portal'
    assert ProjectIdentity.repofy('encoded-core') == 'encoded-core'


def test_c4_project_identity_repofy():

    assert C4ProjectIdentity.repofy('snovault') == 'snovault'
    assert C4ProjectIdentity.repofy('dcicsnovault') == 'snovault'

    assert C4ProjectIdentity.repofy('cgap-portal') == 'cgap-portal'
    assert C4ProjectIdentity.repofy('encoded-core') == 'encoded-core'


def test_project_identity_infer_package_name():

    assert ProjectIdentity.infer_package_name(poetry_data={}, pypi_name='foo', pyproject_name='bar') == 'foo'
    assert ProjectIdentity.infer_package_name(poetry_data=None, pypi_name='foo', pyproject_name='bar') == 'foo'

    assert ProjectIdentity.infer_package_name(poetry_data={}, pypi_name=None, pyproject_name='bar') == 'bar'
    assert ProjectIdentity.infer_package_name(poetry_data=None, pypi_name=None, pyproject_name='bar') == 'bar'

    def test_package_name_from_poetry_data(*, poetry_data, expected):
        actual = ProjectIdentity.infer_package_name(poetry_data=poetry_data, pypi_name=None, pyproject_name='fallback')
        assert actual == expected

    test_package_name_from_poetry_data(poetry_data={'name': 'whatever'},
                                       expected='fallback')
    test_package_name_from_poetry_data(poetry_data={'packages': []},
                                       expected='fallback')
    test_package_name_from_poetry_data(poetry_data={'packages': [{'include': 'something'}]},
                                       expected='something')
    test_package_name_from_poetry_data(poetry_data={'packages': [{'include': 'something'},
                                                                 {'include': 'something-else'}]},
                                       expected='something')
