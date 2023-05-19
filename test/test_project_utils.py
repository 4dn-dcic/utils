import contextlib
import io
import os
import pytest

from dcicutils.misc_utils import ignorable, override_environ, local_attrs, StorageCell
from dcicutils.qa_utils import MockFileSystem
from dcicutils.project_utils import (
    ProjectIdentity, C4ProjectIdentity, Project, C4Project, ProjectRegistry, C4ProjectRegistry,
)
from unittest import mock
from dcicutils import project_utils as project_utils_module


@contextlib.contextmanager
def project_registry_test_context(registry=True):
    attrs = {'APPLICATION_PROJECT_HOME': None, 'PYPROJECT_TOML_FILE': None, 'PYPROJECT_TOML': None,
             'POETRY_DATA': None, '_shared_app_project_cell': StorageCell(initial_value=None),
             '_PYPROJECT_NAME': None}
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


def test_project_registry_find_pyproject():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            @ProjectRegistry.register('foo')
            class FooProject(Project):
                IDENTITY = {"NAME": "foo"}

            assert ProjectRegistry.find_pyproject('foo') == FooProject
            assert ProjectRegistry.find_pyproject('bar') is None


def test_project_registry_make_project():

    old_project_name = ProjectRegistry._PYPROJECT_NAME

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            @ProjectRegistry.register('foo')
            class FooProject(Project):
                IDENTITY = {"NAME": "foo"}

            with pytest.raises(Exception) as exc:
                ProjectRegistry._make_project()
            assert str(exc.value) == "ProjectRegistry.PROJECT_NAME not initialized properly."

            ProjectRegistry.initialize_pyproject_name(pyproject_name='foo')
            assert isinstance(ProjectRegistry._make_project(), FooProject)

            ProjectRegistry.initialize_pyproject_name(pyproject_name='foo')
            with pytest.raises(Exception) as exc:
                C4ProjectRegistry._make_project()
            assert str(exc.value) == "Registered pyproject 'foo' (FooProject) is not a subclass of C4Project."

    assert ProjectRegistry._PYPROJECT_NAME == old_project_name


def test_declare_project_registry_class_wrongly():

    old_value = Project._PROJECT_REGISTRY_CLASS

    try:

        with pytest.raises(Exception) as exc:
            Project.declare_project_registry_class('this is not a registry')  # noQA - we're testing a bug
        assert str(exc.value) == "The registry_class, 'this is not a registry', is not a subclass of ProjectRegistry."

    finally:

        # We were supposed to get an error before any side-effect happened, but we'll be careful just in case.
        Project._PROJECT_REGISTRY_CLASS = old_value


def test_app_project_bad_initialization():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            @ProjectRegistry.register('foo')
            class FooProject(Project):
                IDENTITY = {"NAME": "foo"}

            ProjectRegistry.initialize_pyproject_name(pyproject_name='foo')
            app_project = FooProject.app_project_maker()
            project = app_project()
            project._identity = None  # simulate screwing up of initialization
            with pytest.raises(Exception) as exc:
                print(project.identity)
            assert str(exc.value) == "<FooProject> failed to initialize correctly."


def test_project_registry_class():

    old_registry_class = Project.PROJECT_REGISTRY_CLASS

    try:

        mfs = MockFileSystem()
        with mfs.mock_exists_open_remove():
            with project_registry_test_context():

                @ProjectRegistry.register('foo')
                class FooProject(Project):
                    IDENTITY = {"NAME": "foo"}

                Project._PROJECT_REGISTRY_CLASS = None  # Let's just mock up the problem

                with pytest.raises(Exception) as exc:
                    print(Project.PROJECT_REGISTRY_CLASS)
                assert str(exc.value) == ('Cannot compute Project.PROJECT_REGISTRY_CLASS'
                                          ' because Project.declare_project_registry_class(...) has not been done.')

    finally:

        # We were supposed to get an error before any side-effect happened, but we'll be careful just in case.
        Project.PROJECT_REGISTRY_CLASS = old_registry_class


def test_project_filename():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            @ProjectRegistry.register('foo')
            class FooProject(Project):
                IDENTITY = {"NAME": "foo"}

            ProjectRegistry.initialize_pyproject_name(pyproject_name='foo')
            app_project = FooProject.app_project_maker()
            project = app_project()

            with mock.patch.object(project_utils_module, "resource_filename") as mock_resource_filename:

                def mocked_resource_filename(project, filename):
                    return os.path.join(f"<{project}-home>", filename)

                mock_resource_filename.side_effect = mocked_resource_filename
                assert project.project_filename('xyz') == "<foo-home>/xyz"

            ProjectRegistry._shared_app_project_cell.value = None  # Mock failure to initialize

            with pytest.raises(Exception) as exc:
                print(project.project_filename("foo"))
            assert "is not the app_project" in str(exc.value)


def test_project_registry_register_bad_name():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            with pytest.raises(Exception) as exc:
                @ProjectRegistry.register(17)
                class FooProject(Project):
                    IDENTITY = {"NAME": "foo"}
                ignorable(FooProject)
            assert str(exc.value) == "The pyprjoect_name given to ProjectRegistry.register must be a string: 17"


def test_bad_pyproject_names():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            with pytest.raises(Exception) as exc:
                @C4ProjectRegistry.register('cgap-portal')
                class CGAPProject(C4Project):
                    IDENTITY = {"NAME": "cgap-portal"}
                ignorable(CGAPProject)
            assert str(exc.value) == ("Please use C4ProjectRegistry.register('encoded'),"
                                      " not C4ProjectRegistry.register('cgap-portal')."
                                      " This name choice in project registration is just for bootstrapping."
                                      " The class can still be CGAPProject.")

            with pytest.raises(Exception) as exc:
                @C4ProjectRegistry.register('fourfront')
                class FourfrontProject(C4Project):
                    IDENTITY = {"NAME": "fourfront"}
                ignorable(FourfrontProject)
            assert str(exc.value) == ("Please use C4ProjectRegistry.register('encoded'),"
                                      " not C4ProjectRegistry.register('fourfront')."
                                      " This name choice in project registration is just for bootstrapping."
                                      " The class can still be FourfrontProject.")

            with pytest.raises(Exception) as exc:
                @C4ProjectRegistry.register('smaht-portal')
                class SMaHTProject(C4Project):
                    IDENTITY = {"NAME": "smaht-portal"}
                ignorable(SMaHTProject)
            assert str(exc.value) == ("Please use C4ProjectRegistry.register('encoded'),"
                                      " not C4ProjectRegistry.register('smaht-portal')."
                                      " This name choice in project registration is just for bootstrapping."
                                      " The class can still be SMaHTProject.")


def test_project_registry_initialize():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            @ProjectRegistry.register('foo')
            class FooProject(Project):
                IDENTITY = {"NAME": "foo"}

            ProjectRegistry.initialize_pyproject_name(pyproject_name='foo')
            app_project = FooProject.app_project_maker()
            project = app_project()

            assert ProjectRegistry.initialize() == project
            assert ProjectRegistry.initialize() == project

            new_project = ProjectRegistry.initialize(force=True)  # Creates a new instance of the app_project()

            assert isinstance(new_project, Project)
            assert new_project != project
            assert new_project.NAME == project.NAME
