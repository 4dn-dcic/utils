import contextlib
import io
import os
import pytest

from dcicutils.misc_utils import ignorable, override_environ, local_attrs, StorageCell
from dcicutils.qa_utils import MockFileSystem, printed_output
from dcicutils.project_utils import (
    ProjectNames, C4ProjectNames, Project, C4Project, ProjectRegistry, C4ProjectRegistry,
)
from unittest import mock
from dcicutils import project_utils as project_utils_module


def app_project_cell_value():
    return project_utils_module._SHARED_APP_PROJECT_CELL.value  # noQA - testing access to protected member


def set_app_project_cell_value(value):
    project_utils_module._SHARED_APP_PROJECT_CELL.value = value  # noQA - testing access to protected member


@contextlib.contextmanager
def project_registry_test_context(registry=True):
    temp_cell = StorageCell(initial_value=None)
    attrs = {'APPLICATION_PROJECT_HOME': None, 'PYPROJECT_TOML_FILE': None, 'PYPROJECT_TOML': None,
             'POETRY_DATA': None, '_PYPROJECT_NAME': None}
    if registry:
        attrs['REGISTERED_PROJECTS'] = {}
    with local_attrs(ProjectRegistry, **attrs):
        with mock.patch.object(project_utils_module, '_SHARED_APP_PROJECT_CELL', temp_cell):
            yield


def test_project_registry_register():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove_abspath_getcwd_chdir():
        with project_registry_test_context():

            @ProjectRegistry.register('foo')
            class FooProject(Project):
                NAMES = {"NAME": "foo", "PRETTY_NAME": "Fu"}

            foo_project = FooProject()

            assert foo_project.NAME == 'foo'
            assert foo_project.PRETTY_NAME == 'Fu'

            assert foo_project.PYPROJECT_NAME == 'foo'

            @ProjectRegistry.register('foobar')
            class FooBarProject(Project):
                NAMES = {"NAME": "foobar"}

            foobar_project = FooBarProject()

            assert foobar_project.NAME == 'foobar'
            assert foobar_project.PRETTY_NAME == 'Foobar'

            assert foobar_project.PYPROJECT_NAME == 'foobar'

            with pytest.raises(Exception) as exc:
                @ProjectRegistry.register('foo')
                class FooProject:
                    NAMES = {"NAME": 'foo'}
                ignorable(FooProject)  # It won't get this far.
            assert str(exc.value) == "The class FooProject must inherit from Project."

            with pytest.raises(Exception) as exc:
                @C4ProjectRegistry.register('foo')
                class FooProject(Project):
                    NAMES = {"NAME": 'foo'}
                ignorable(FooProject)  # It won't get this far.
            assert str(exc.value) == "The class FooProject must inherit from C4Project."

            with pytest.raises(Exception) as exc:
                @ProjectRegistry.register('foo')
                class FooProject(Project):
                    NAMES = {}
                ignorable(FooProject)  # It won't get this far.
            assert str(exc.value) == "Declaration of FooProject must have a non-empty NAMES= specification."

            with pytest.raises(Exception) as exc:
                @ProjectRegistry.register('foo')
                class FooProject(Project):
                    NAMES = {"APP_NAME": 'foo'}
                ignorable(FooProject)  # It won't get this far.
            assert str(exc.value) == "Declaration of FooProject NAMES= is missing 'NAME'."

            with pytest.raises(Exception) as exc:
                @ProjectRegistry.register('foo')
                class FooProject(Project):
                    NAMES = {"PYPROJECT_NAME": 'foox'}
                ignorable(FooProject)  # It won't get this far.
            assert str(exc.value) == ("Explicitly specifying NAMES={'PYPROJECT_NAME': ...} is not allowed."
                                      " The PYPROJECT_NAME is managed implicitly"
                                      " using information given in the ProjectRegistry.register decorator.")

            with pytest.raises(Exception) as exc:
                @ProjectRegistry.register('foobar')
                class FooProject(Project):
                    NAMES = {"PYPROJECT_NAME": 'foox'}
                ignorable(FooProject)  # It won't get this far.
            assert str(exc.value) == ("Explicitly specifying NAMES={'PYPROJECT_NAME': ...} is not allowed."
                                      " The PYPROJECT_NAME is managed implicitly"
                                      " using information given in the ProjectRegistry.register decorator.")


def test_project_registry_register_snovault_scenario():

    # These values don't have to be precisely what snovault uses, just plausibly things it might

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove_abspath_getcwd_chdir():
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
                NAMES = {"NAME": 'snovault', "PYPI_NAME": 'dcicsnovault'}
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
    with mfs.mock_exists_open_remove_abspath_getcwd_chdir():
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
                NAMES = {"NAME": 'snovault', "PYPI_NAME": 'dcicsnovault'}
                ACCESSION_PREFIX = 'SNO'

            @ProjectRegistry.register('encoded')
            class CGAPProject(SnovaultProject):
                NAMES = {"NAME": 'cgap-portal'}
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


def test_project_names_prettify():

    assert ProjectNames.prettify('foo') == 'Foo'
    assert ProjectNames.prettify('foo-bar') == 'Foo Bar'

    assert ProjectNames.prettify('cgap-portal') == 'Cgap Portal'
    assert ProjectNames.prettify('smaht-portal') == 'Smaht Portal'


def test_c4_project_names_prettify():

    assert C4ProjectNames.prettify('foo') == 'Foo'
    assert C4ProjectNames.prettify('foo-bar') == 'Foo Bar'

    assert C4ProjectNames.prettify('cgap-portal') == 'CGAP Portal'
    assert C4ProjectNames.prettify('smaht-portal') == 'SMaHT Portal'


def test_project_names_appify():

    # Just an identity function in the ProjectNames class

    assert ProjectNames.appify('snovault') == 'snovault'
    assert ProjectNames.appify('dcicsnovault') == 'dcicsnovault'
    assert ProjectNames.appify('cgap-portal') == 'cgap-portal'
    assert ProjectNames.appify('encoded-core') == 'encoded-core'


def test_c4_project_names_appify():

    assert C4ProjectNames.appify('snovault') == 'snovault'
    assert C4ProjectNames.appify('dcicsnovault') == 'dcicsnovault'

    assert C4ProjectNames.appify('cgap-portal') == 'cgap'
    assert C4ProjectNames.appify('encoded-core') == 'core'


def test_project_names_repofy():

    # Just an identity function in the ProjectNames class

    assert ProjectNames.repofy('snovault') == 'snovault'
    assert ProjectNames.repofy('dcicsnovault') == 'dcicsnovault'
    assert ProjectNames.repofy('cgap-portal') == 'cgap-portal'
    assert ProjectNames.repofy('encoded-core') == 'encoded-core'


def test_c4_project_names_repofy():

    assert C4ProjectNames.repofy('snovault') == 'snovault'
    assert C4ProjectNames.repofy('dcicsnovault') == 'snovault'

    assert C4ProjectNames.repofy('cgap-portal') == 'cgap-portal'
    assert C4ProjectNames.repofy('encoded-core') == 'encoded-core'


def test_project_names_infer_package_name():

    assert ProjectNames.infer_package_name(poetry_data={}, pypi_name='foo', pyproject_name='bar') == 'foo'
    assert ProjectNames.infer_package_name(poetry_data=None, pypi_name='foo', pyproject_name='bar') == 'foo'

    assert ProjectNames.infer_package_name(poetry_data={}, pypi_name=None, pyproject_name='bar') == 'bar'
    assert ProjectNames.infer_package_name(poetry_data=None, pypi_name=None, pyproject_name='bar') == 'bar'

    def test_package_name_from_poetry_data(*, poetry_data, expected):
        actual = ProjectNames.infer_package_name(poetry_data=poetry_data, pypi_name=None, pyproject_name='fallback')
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
                NAMES = {"NAME": "foo"}

            assert ProjectRegistry.find_pyproject('foo') == FooProject
            assert ProjectRegistry.find_pyproject('bar') is None


def test_project_registry_make_project():

    old_project_name = ProjectRegistry._PYPROJECT_NAME

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            @ProjectRegistry.register('foo')
            class FooProject(Project):
                NAMES = {"NAME": "foo"}

            with pytest.raises(Exception) as exc:
                ProjectRegistry._make_project()
            assert str(exc.value) == "ProjectRegistry.PROJECT_NAME not initialized properly."

            ProjectRegistry._initialize_pyproject_name(pyproject_name='foo')
            assert isinstance(ProjectRegistry._make_project(), FooProject)

            with pytest.raises(Exception) as exc:
                C4ProjectRegistry._make_project()
            assert str(exc.value) == "Registered pyproject 'foo' (FooProject) is not a subclass of C4Project."

            ProjectRegistry._PYPROJECT_NAME = 'bogus'  # Mock up a bad initialization
            set_app_project_cell_value(None)  # Clear cache
            with pytest.raises(Exception) as exc:
                ProjectRegistry._make_project()
            assert str(exc.value) == "Missing project class for pyproject 'bogus'."

    assert ProjectRegistry._PYPROJECT_NAME == old_project_name


def test_declare_project_registry_class_wrongly():

    old_value = Project._PROJECT_REGISTRY_CLASS

    try:

        with pytest.raises(Exception) as exc:
            Project.declare_project_registry_class('this is not a registry')  # noQA - we're testing a bug
        assert str(exc.value) == "The registry_class, 'this is not a registry', is not a subclass of ProjectRegistry."

    finally:

        # We were supposed to get an error before any side effect happened, but we'll be careful just in case.
        Project._PROJECT_REGISTRY_CLASS = old_value


def test_project_registry_class():

    old_registry_class = Project.PROJECT_REGISTRY_CLASS

    try:

        mfs = MockFileSystem()
        with mfs.mock_exists_open_remove():
            with project_registry_test_context():

                @ProjectRegistry.register('foo')
                class FooProject(Project):
                    NAMES = {"NAME": "foo"}
                ignorable(FooProject)

                Project._PROJECT_REGISTRY_CLASS = None  # Let's just mock up the problem

                with pytest.raises(Exception) as exc:
                    print(Project.PROJECT_REGISTRY_CLASS)
                assert str(exc.value) == ('Cannot compute Project.PROJECT_REGISTRY_CLASS'
                                          ' because Project.declare_project_registry_class(...) has not been done.')

    finally:

        # We were supposed to get an error before any side effect happened, but we'll be careful just in case.
        Project.PROJECT_REGISTRY_CLASS = old_registry_class


def test_project_filename():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            @ProjectRegistry.register('foo')
            class FooProject(Project):
                NAMES = {"NAME": "foo"}

            ProjectRegistry._initialize_pyproject_name(pyproject_name='foo')
            app_project = FooProject.app_project_maker()
            project = app_project()

            with mock.patch.object(project_utils_module, "resource_filename") as mock_resource_filename:

                def mocked_resource_filename(project, filename):
                    return os.path.join(f"<{project}-home>", filename)

                mock_resource_filename.side_effect = mocked_resource_filename
                assert project.project_filename('xyz') == "<foo-home>/xyz"

            set_app_project_cell_value(None)  # Mock failure to initialize

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
                    NAMES = {"NAME": "foo"}
                ignorable(FooProject)
            assert str(exc.value) == "The pyprjoect_name given to ProjectRegistry.register must be a string: 17"


def test_bad_pyproject_names():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            with pytest.raises(Exception) as exc:
                @C4ProjectRegistry.register('cgap-portal')
                class CGAPProject(C4Project):
                    NAMES = {"NAME": "cgap-portal"}
                ignorable(CGAPProject)
            assert str(exc.value) == ("Please use C4ProjectRegistry.register('encoded'),"
                                      " not C4ProjectRegistry.register('cgap-portal')."
                                      " This name choice in project registration is just for bootstrapping."
                                      " The class can still be CGAPProject.")

            with pytest.raises(Exception) as exc:
                @C4ProjectRegistry.register('fourfront')
                class FourfrontProject(C4Project):
                    NAMES = {"NAME": "fourfront"}
                ignorable(FourfrontProject)
            assert str(exc.value) == ("Please use C4ProjectRegistry.register('encoded'),"
                                      " not C4ProjectRegistry.register('fourfront')."
                                      " This name choice in project registration is just for bootstrapping."
                                      " The class can still be FourfrontProject.")

            with pytest.raises(Exception) as exc:
                @C4ProjectRegistry.register('smaht-portal')
                class SMaHTProject(C4Project):
                    NAMES = {"NAME": "smaht-portal"}
                ignorable(SMaHTProject)
            assert str(exc.value) == ("Please use C4ProjectRegistry.register('encoded'),"
                                      " not C4ProjectRegistry.register('smaht-portal')."
                                      " This name choice in project registration is just for bootstrapping."
                                      " The class can still be SMaHTProject.")


def test_project_initialize():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            @ProjectRegistry.register('foo')
            class FooProject(Project):
                NAMES = {"NAME": "foo"}

            ProjectRegistry._initialize_pyproject_name(pyproject_name='foo')
            project = FooProject.initialize()
            assert isinstance(project, FooProject)

            assert FooProject.initialize() == project  # Doing it again gets the same one

            new_project = Project.initialize(force=True)  # Creates a new instance of the app_project()

            assert isinstance(new_project, FooProject)
            assert new_project is not project
            assert new_project.NAME == project.NAME


def test_project_registry_initialize():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            @ProjectRegistry.register('foo')
            class FooProject(Project):
                NAMES = {"NAME": "foo"}

            ProjectRegistry._initialize_pyproject_name(pyproject_name='foo')
            project = ProjectRegistry.initialize()
            assert isinstance(project, FooProject)

            assert ProjectRegistry.initialize() == project  # Doing it again gets the same one

            new_project = ProjectRegistry.initialize(force=True)  # Creates a new instance of the app_project()

            assert isinstance(new_project, Project)
            assert new_project != project
            assert new_project.NAME == project.NAME


def test_initialize_pyproject_name_ambiguity():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            with io.open("pyproject.toml", 'w') as fp:
                fp.write('[tool.poetry]\n'
                         'name = "bar"\n')

            with pytest.raises(Exception) as exc:
                with override_environ(APPLICATION_PYPROJECT_NAME='alpha'):
                    ProjectRegistry._initialize_pyproject_name(pyproject_toml_file='pyproject.toml',
                                                               poetry_data={'name': 'omega',
                                                                            'packages': [{'include': 'omega'}]})
            assert str(exc.value) == "APPLICATION_PYPROJECT_NAME='alpha', but pyproject.toml says it should be 'omega'"


def test_app_project():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            @C4ProjectRegistry.register('super')
            class SuperProject(C4Project):
                NAMES = {"NAME": "super"}

            app_project = SuperProject.app_project_maker()

            C4ProjectRegistry._initialize_pyproject_name(pyproject_name='super')

            proj1 = app_project()
            assert app_project_cell_value() is not None
            proj2 = app_project()
            assert proj1 is proj2


def test_app_project_bad_initialization():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove():
        with project_registry_test_context():

            @ProjectRegistry.register('foo')
            class FooProject(Project):
                NAMES = {"NAME": "foo"}

            ProjectRegistry._initialize_pyproject_name(pyproject_name='foo')
            app_project = FooProject.app_project_maker()
            project = app_project()
            project._names = None  # simulate screwing up of initialization
            with pytest.raises(Exception) as exc:
                print(project.names)
            assert str(exc.value) == "<FooProject> failed to initialize correctly."


MISSING = '<missing>'


@pytest.mark.parametrize("verbose", [MISSING, None, True, False])
@pytest.mark.parametrize("detailed", [MISSING, None, True, False])
def test_project_registry_show_herald(verbose, detailed):

    print(f"Testing verbose={verbose}, detailed={detailed}")

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove_abspath_getcwd_chdir():
        with project_registry_test_context():

            @ProjectRegistry.register('foo')
            class FooProject(Project):
                NAMES = {"NAME": "foo"}
            ignorable(FooProject)

            ProjectRegistry._initialize_pyproject_name(pyproject_name='foo')

            with printed_output() as printed:

                options = {}
                if detailed is not MISSING:
                    options['detailed'] = detailed
                if verbose is not MISSING:
                    options['verbose'] = verbose
                print(f" Initialization options: {options}")
                ProjectRegistry.initialize(**options)

                if verbose is MISSING or verbose is None:
                    verbose = ProjectRegistry.PROJECT_INITIALIZE_VERBOSE

                if detailed is MISSING or detailed is None:
                    detailed = ProjectRegistry.PROJECT_INITIALIZE_DETAILED

                print(f" Expected behavior as if verbose={verbose}, detailed={detailed}")

                if not verbose:
                    assert printed.lines == []
                elif detailed:
                    assert printed.lines == [
                        "",
                        "==========================================================================================",
                        "APPLICATION_PROJECT_HOME == '/home/mock'",
                        "PYPROJECT_TOML_FILE == None",
                        "PYPROJECT_NAME == 'foo'",
                        "Project.app_project == ProjectRegistry.app_project == app_project() == <FooProject>",
                        "app_project().APP_NAME == 'foo'",
                        "app_project().APP_PRETTY_NAME == 'Foo'",
                        "app_project().NAME == 'foo'",
                        "app_project().PACKAGE_NAME == 'foo'",
                        "app_project().PRETTY_NAME == 'Foo'",
                        'app_project().PYPI_NAME == None',
                        "app_project().PYPROJECT_NAME == 'foo'",
                        "app_project().REPO_NAME == 'foo'",
                        "==========================================================================================",
                    ]
                else:
                    assert printed.lines == ["FooProject initialized with name 'foo'."]


@pytest.mark.parametrize("options_to_test,expected_caveats", [
    ({},
     "with name 'foobar'"),
    ({"PRETTY_NAME": "FooBar"},  # FooBar differs only in case, so won't be flagged
     "with name 'foobar'"),
    ({"PRETTY_NAME": "Foo+Bar"},  # Foo+Bar differs only in special characters, so won't be flagged
     "with name 'foobar'"),
    ({"PRETTY_NAME": "Fubar"},  # 'Fubar' uses a different spelling so will be noted as different
     "with name 'foobar' and notable alias 'Fubar'"),
    ({"PRETTY_NAME": "Fubar", "REPO_NAME": "foo_bar"},  # 'Fubar' uses different spelling but 'foo_bar' does not.
     "with name 'foobar' and notable alias 'Fubar'"),
    ({"PRETTY_NAME": "Super Foobar", "REPO_NAME": "FuBar"},  # Both 'FuBar' and 'Super Foobar' are spelled differently.
     "with name 'foobar' and notable aliases 'FuBar' and 'Super Foobar'"),
])
def test_project_registry_show_herald_detailed(options_to_test, expected_caveats):

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove_abspath_getcwd_chdir():
        with project_registry_test_context():

            @ProjectRegistry.register('foobar')
            class FoobarProject(Project):
                NAMES = {"NAME": "foobar", **options_to_test}
            ignorable(FoobarProject)

            ProjectRegistry._initialize_pyproject_name(pyproject_name='foobar')

            with printed_output() as printed:

                ProjectRegistry.initialize(verbose=True, detailed=False)

                assert printed.lines == [f"FoobarProject initialized {expected_caveats}."]


def test_project_registry_show_herald_consistency():

    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove_abspath_getcwd_chdir():
        with project_registry_test_context():

            @ProjectRegistry.register('foobar')
            class FoobarProject(Project):
                NAMES = {"NAME": "foobar"}
            ignorable(FoobarProject)

            ProjectRegistry._initialize_pyproject_name(pyproject_name='foobar')

            rogue_project = FoobarProject()

            # We have to work pretty hard to mess up the consistency, but this should do it.
            # The value of app_project as an instance property here overrides the class property
            # that is supposed implement sharing between all the clasess.
            rogue_project.app_project = rogue_project

            with pytest.raises(Exception) as exc:

                rogue_project.show_herald()

            assert str(exc.value) == "Project consistency check failed."


def test_project_names_items():

    names_object = ProjectNames(PYPROJECT_NAME='foo', NAME='foo')
    items = names_object.items()
    actual_keys = []
    for k, v in items:
        assert isinstance(k, str)
        assert isinstance(v, str) or (k == 'PYPI_NAME' and v is None)
        actual_keys.append(k)
    expected_keys = ['APP_NAME', 'APP_PRETTY_NAME', 'NAME', 'PACKAGE_NAME',
                     'PRETTY_NAME', 'PYPI_NAME', 'PYPROJECT_NAME', 'REPO_NAME']
    assert actual_keys == expected_keys


def test_find_notable_aliases():

    names = ProjectNames(PYPROJECT_NAME='foo', NAME='foo')
    actual_notable_aliases = names.find_notable_aliases()
    expected_notable_aliases = []
    assert actual_notable_aliases == expected_notable_aliases

    names = ProjectNames(PYPROJECT_NAME='foobar', NAME='foobar', PRETTY_NAME='FuBar')
    actual_notable_aliases = names.find_notable_aliases()
    expected_notable_aliases = ['FuBar']
    assert actual_notable_aliases == expected_notable_aliases

    names = ProjectNames(PYPROJECT_NAME='foobar', NAME='foobar', PRETTY_NAME='FuBar', PYPI_NAME='Foo Bar')
    actual_notable_aliases = names.find_notable_aliases()
    expected_notable_aliases = ['FuBar']
    assert actual_notable_aliases == expected_notable_aliases

    names = ProjectNames(PYPROJECT_NAME='encoded', NAME='cgap-portal')
    actual_notable_aliases = names.find_notable_aliases()
    expected_notable_aliases = ['encoded']
    assert actual_notable_aliases == expected_notable_aliases

    names = C4ProjectNames(PYPROJECT_NAME='encoded', NAME='cgap-portal')
    actual_notable_aliases = names.find_notable_aliases()
    expected_notable_aliases = ['cgap', 'encoded']
    assert actual_notable_aliases == expected_notable_aliases

    names = ProjectNames(PYPROJECT_NAME='dcicsnovault', NAME='snovault')
    actual_notable_aliases = names.find_notable_aliases()
    expected_notable_aliases = ['dcicsnovault']
    assert actual_notable_aliases == expected_notable_aliases

    names = C4ProjectNames(PYPROJECT_NAME='dcicsnovault', NAME='snovault')
    actual_notable_aliases = names.find_notable_aliases()
    expected_notable_aliases = ['dcicsnovault']
    assert actual_notable_aliases == expected_notable_aliases

    names = ProjectNames(PYPROJECT_NAME='encoded-core', NAME='encoded-core')
    actual_notable_aliases = names.find_notable_aliases()
    expected_notable_aliases = []
    assert actual_notable_aliases == expected_notable_aliases

    names = C4ProjectNames(PYPROJECT_NAME='encoded-core', NAME='encoded-core')
    actual_notable_aliases = names.find_notable_aliases()
    expected_notable_aliases = ['core']
    assert actual_notable_aliases == expected_notable_aliases
