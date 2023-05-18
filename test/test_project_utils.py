import contextlib
import io
import os
import pytest

from dcicutils.misc_utils import ignorable, override_environ, local_attrs
from dcicutils.qa_utils import MockFileSystem
from dcicutils.project_utils import ProjectRegistry, Project, C4Project


@contextlib.contextmanager
def project_registry_test_context(registry=True):
    if registry:
        with local_attrs(ProjectRegistry, REGISTERED_PROJECTS={}, APPLICATION_PROJECT_HOME=None, PYPROJECT_TOML_FILE=None,
                         PYPROJECT_TOML=None, POETRY_DATA=None, _app_project=None, _PYPROJECT_NAME=None):
            yield
    else:
        with local_attrs(ProjectRegistry, APPLICATION_PROJECT_HOME=None, PYPROJECT_TOML_FILE=None,
                         PYPROJECT_TOML=None, POETRY_DATA=None, _app_project=None, _PYPROJECT_NAME=None):
            yield




def test_project_registry_register():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            @ProjectRegistry.register('foo')
            class FooProject(Project):
                NAME = "foo"
                PRETTY_NAME = "Fu"

            assert FooProject.NAME == 'foo'
            assert FooProject.PRETTY_NAME == 'Fu'

            @ProjectRegistry.register('foobar')
            class FooBarProject(Project):
                NAME = "foobar"

            assert FooBarProject.NAME == 'foobar'
            assert FooBarProject.PRETTY_NAME == 'Foobar'

            assert FooProject.PYPROJECT_NAME == 'foo'

            with pytest.raises(Exception) as exc:
                @ProjectRegistry.register('foo')
                class FooProject(Project):
                    PYPROJECT_NAME = 'foo'
                ignorable(FooProject)  # It won't get this far.
            assert str(exc.value) == ("Explicit FooProject.PYPROJECT_NAME='foo' is not permitted."
                                      " This assignment is intended to be managed implicitly.")

            with pytest.raises(Exception) as exc:
                @ProjectRegistry.register('foobar')
                class FooProject(Project):
                    PYPROJECT_NAME = 'foo'
                ignorable(FooProject)  # It won't get this far.
            assert str(exc.value) == ("Explicit FooProject.PYPROJECT_NAME='foo' is not permitted."
                                      " This assignment is intended to be managed implicitly.")


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
                NAME = 'snovault'
                PYPI_NAME = 'dcicsnovault'
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
                NAME = 'snovault'
                PYPI_NAME = 'dcicsnovault'
                ACCESSION_PREFIX = 'SNO'

            @ProjectRegistry.register('encoded')
            class CGAPProject(SnovaultProject):
                NAME = 'cgap-portal'
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
                assert app_project().PYPI_NAME == None
                assert app_project().PYPROJECT_NAME == 'encoded'
                assert app_project().REPO_NAME == 'cgap-portal'

            test_it()

            mfs.remove(pyproject_file)

            with override_environ(APPLICATION_PYPROJECT_NAME='encoded'):
                test_it()
