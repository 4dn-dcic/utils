import os
import toml

from pkg_resources import resource_filename
from typing import Optional, Type
from .env_utils import EnvUtils
from .misc_utils import PRINT, StorageCell, classproperty


class ProjectIdentity:

    @classmethod
    def prettify(cls, name: str) -> str:
        return name.title().replace("-", " ")

    @classmethod
    def appify(cls, name: str) -> str:
        return name

    @classmethod
    def repofy(cls, name: str) -> str:
        return name

    @classmethod
    def infer_package_name(cls, poetry_data, pypi_name, pyproject_name):
        try:
            if poetry_data:
                # We expect the first package in the declared packages to be the primary export
                # Other exported dirs, such as scripts or tests should be in later entries.
                return poetry_data['packages'][0]['include']
        except Exception:
            pass
        return pypi_name or pyproject_name

    def __init__(self, *, PYPROJECT_NAME, NAME, PRETTY_NAME=None, REPO_NAME=None, PYPI_NAME=None,
                 PACKAGE_NAME=None, APP_NAME=None, APP_PRETTY_NAME=None):
        self.PYPROJECT_NAME = PYPROJECT_NAME
        self.NAME = NAME
        self.PRETTY_NAME = PRETTY_NAME or self.prettify(NAME)
        self.PYPI_NAME = PYPI_NAME or None  # canonicalize '' to None, just in case
        self.REPO_NAME = REPO_NAME = REPO_NAME or self.repofy(NAME)
        self.APP_NAME = APP_NAME = APP_NAME or self.appify(REPO_NAME)
        self.APP_PRETTY_NAME = APP_PRETTY_NAME or self.prettify(APP_NAME)
        self.PACKAGE_NAME = (PACKAGE_NAME
                             # This uses ProjectRegistry (not cls) because that part of the protocol is common to all
                             # classes and subclasses of that class. No need to worry about specialized classes.
                             # -kmp 19-May-2023
                             or self.infer_package_name(poetry_data=ProjectRegistry.POETRY_DATA,
                                                        pypi_name=PYPI_NAME, pyproject_name=PYPROJECT_NAME))


class Project:
    """
    A class that should be a superclass of all classes registered using ProjectRegistry.register

    All such classes have these names:
      .PYPROJECT_NAME - The name used in the .register decorator on the class, and in pyproject.toml
      .NAME - The customary name of the project
      .REPO_NAME - The name of the repo
      .PYPI_NAME - The name of the project in pypi (if any), else None
      .PACKAGE_NAME - The installed name of the project as a package in a venv, for use with pkg_resources
      .PRETTY_NAME - The pretty name of the package name
      .APP_NAME - The ame of the project application (see dcicutils.common and the orchestrated app in EnvUtils)
      .APP_PRETTY_NAME - The pretty name of the project application.

    Some sample usess of pre-defined attributes of a Project that may help motivate the choice of attribute names,
    though how these get initialized is easier if you're basing your package on C4Project,

    PYPROJECT_NAME | NAME        |REPO_NAME    |PYPI_NAME    |PACKAGE_NAME  |PRETTY_NAME  |APP_NAME  |APP_PRETTY_NAME
    ---------------+-------------+-------------+-------------+--------------+-------------+----------+----------------
    dcicsnovault   |snovault     |snovault     |snovault     |snovault      |Snovault     |snovault  |Snovault
    encoded-core   |encoded-core |encoded-core |encoded-core |encoded-core  |Encoded Core |core      |Core
    encoded        |cgap-portal  |cgap-portal  |None         |encoded       |CGAP Portal  |cgap      |CGAP
    encoded        |fourfront    |fourfront    |None         |encoded       |Fourfront    |fourfront |Fourfront
    encoded        |smaht-portal |smaht-portal |None         |encoded       |SMaHT Portal |smaht     |SMaHT

     The registered name is the one used with the ProjectRegistry.register() decorator.
    """

    # This contains only the initial spec and is NOT used after initialization. See .identity (and ._identity).
    # It is declared in this way so that actual declarations of project classes don't show up as syntax errors.
    IDENTITY: Optional[dict] = None

    IDENTITY_CLASS: Type[ProjectIdentity] = ProjectIdentity

    _identity: Optional[ProjectIdentity] = None  # Class-level default, overridden in instances as a ProjectIdentity

    _PROJECT_REGISTRY_CLASS = None

    @classproperty
    def PROJECT_REGISTRY_CLASS(cls):  # noQA - PyCharm thinks this should use 'self'
        registry_class: Optional[Type[ProjectRegistry]] = cls._PROJECT_REGISTRY_CLASS
        if registry_class is None:
            raise ValueError(f"Cannot compute {cls.__name__}.PROJECT_REGISTRY_CLASS"
                             f" because {cls.__name__}.declare_project_registry_class(...) has not been done.")
        return registry_class

    @classmethod
    def declare_project_registry_class(cls, registry_class: Type):
        """
        This function is needed to complete a circularity where project classes need to know the appropriate
        registry class and registry classes need to know the appropriate project class, so by convention when
        the registry class (which is defined after the project class) is fully defined, this method on the project
        class should be called with the registry class.
        """
        if not isinstance(registry_class, type) or not issubclass(registry_class, ProjectRegistry):
            raise ValueError(f"The registry_class, {registry_class!r}, is not a subclass of ProjectRegistry.")
        registry_class: Type[ProjectRegistry]
        cls._PROJECT_REGISTRY_CLASS = registry_class

    def __init__(self):
        self._identity: ProjectIdentity = self.IDENTITY_CLASS(**self.IDENTITY)

    def __str__(self):
        return f"<{self.__class__.__name__} {id(self):x}>"

    def __repr__(self):
        return f"{self.__class__.__name__}()"

    @property
    def identity(self) -> ProjectIdentity:
        if self._identity is None:
            raise ValueError(f"<{self.__class__.__name__}> failed to initialize correctly.")
        return self._identity

    @property
    def PYPROJECT_NAME(self) -> str:
        return self.identity.PYPROJECT_NAME

    @property
    def NAME(self):
        return self.identity.NAME

    @property
    def REPO_NAME(self) -> str:
        return self.identity.REPO_NAME

    @property
    def PYPI_NAME(self):
        return self.identity.PYPI_NAME

    @property
    def PACKAGE_NAME(self) -> str:
        return self.identity.PACKAGE_NAME

    @property
    def PRETTY_NAME(self):
        return self.identity.PRETTY_NAME

    @property
    def APP_NAME(self):
        return self.identity.APP_NAME

    @property
    def APP_PRETTY_NAME(self):
        return self.identity.APP_PRETTY_NAME

    @classproperty
    def app_project(cls):  # noQA - PyCharm wants the variable name to be self
        """
        Project.app_project returns the actual instantiated project for app-specific behavior,
        which might be of this class or one of its subclasses.

        This access will fail if the project has not been initialized.
        """
        # This is a shared cell, so it doesn't matter whether we read this from ProjectRegistry
        # or one of its subclasses. -kmp 19-May-2023
        return ProjectRegistry.app_project

    @classmethod
    def initialize_app_project(cls, initialize_env_utils=True):
        if initialize_env_utils:
            EnvUtils.init()
        project: Project = cls.PROJECT_REGISTRY_CLASS.initialize()
        return project

    @classmethod
    def app_project_maker(cls):

        registry_class: ProjectRegistry = cls.PROJECT_REGISTRY_CLASS

        def app_project(initialize: bool = False, initialization_options: Optional[dict] = None) -> Project:
            if initialize:
                registry_class.PROJECT_BASE_CLASS.initialize_app_project(**(initialization_options or {}))
            return Project.app_project

        return app_project

    def project_filename(self, filename):
        """Returns a filename relative to given instance."""
        current_project = self.app_project
        if self is not current_project:
            print(f"self = {self}")
            print(f"self.app_project = {current_project}")
            raise RuntimeError(f"{self}.project_filename invoked,"
                               f" but {self} is not the app_project, {current_project}.")
        return resource_filename(self.PACKAGE_NAME, filename)


class ProjectRegistry:

    PROJECT_BASE_CLASS: Type[Project] = Project

    SHOW_HERALD_WHEN_INITIALIZED = True

    REGISTERED_PROJECTS = {}

    # All of these might never be other than None so be careful when accessing them.
    APPLICATION_PROJECT_HOME = None
    PYPROJECT_TOML_FILE = None
    PYPROJECT_TOML = None
    POETRY_DATA = None
    # This is expected to ultimately be set properly.
    _PYPROJECT_NAME = None

    @classproperty
    def PYPROJECT_NAME(cls) -> str:  # noQA - PyCharm thinks this should be 'self'
        if cls._PYPROJECT_NAME is None:
            cls.initialize_pyproject_name()
        result: Optional[str] = cls._PYPROJECT_NAME
        if result is None:
            raise ValueError(f"{cls.__name__}.PROJECT_NAME not initialized properly.")
        return result

    @classmethod
    def initialize_pyproject_name(cls, project_home=None, pyproject_toml_file=None,
                                  pyproject_toml=None, poetry_data=None, pyproject_name=None):
        if ProjectRegistry._PYPROJECT_NAME is None:
            # This isn't the home of Project, but the home of the Project-based application.
            # So in CGAP, for example, this would want to be the home of the CGAP application.
            # If not set, it will be assumed that the current working directory is that.
            # print("Setting up data.")
            if not project_home:
                project_home = os.environ.get("APPLICATION_PROJECT_HOME", os.path.abspath(os.curdir))
            ProjectRegistry.APPLICATION_PROJECT_HOME = project_home
            if not pyproject_toml_file:
                expected_pyproject_toml_file = os.path.join(project_home, "pyproject.toml")
                pyproject_toml_file = (expected_pyproject_toml_file
                                       if os.path.exists(expected_pyproject_toml_file)
                                       else None)
            ProjectRegistry.PYPROJECT_TOML_FILE = pyproject_toml_file
            # print(f"Loading toml file {cls.PYPROJECT_TOML_FILE}")
            if not pyproject_toml:
                ProjectRegistry.PYPROJECT_TOML = pyproject_toml = (toml.load(ProjectRegistry.PYPROJECT_TOML_FILE)
                                                                   if ProjectRegistry.PYPROJECT_TOML_FILE
                                                                   else None)
            if not poetry_data:
                poetry_data = (pyproject_toml['tool']['poetry']
                               if pyproject_toml
                               else None)
            # print(f"Setting POETRY_DATA = {poetry_data}")
            ProjectRegistry.POETRY_DATA = poetry_data

            if not pyproject_name:
                declared_pyproject_name = os.environ.get("APPLICATION_PYPROJECT_NAME")
                inferred_pyproject_name = ProjectRegistry.POETRY_DATA['name'] if ProjectRegistry.POETRY_DATA else None
                if (declared_pyproject_name and inferred_pyproject_name
                        and declared_pyproject_name != inferred_pyproject_name):
                    raise RuntimeError(f"APPLICATION_PYPROJECT_NAME={declared_pyproject_name!r},"
                                       f" but {pyproject_toml_file} says it should be {inferred_pyproject_name!r}")
                pyproject_name = declared_pyproject_name or inferred_pyproject_name
            ProjectRegistry._PYPROJECT_NAME = pyproject_name

    REQUIRED_IDENTITY_PARAMETERS = ['NAME']

    PROTECTED_IDENTITY_PARAMETERS = ['PYPROJECT_NAME']

    BAD_PYPROJECT_NAMES = []

    @classmethod
    def register(cls, pyproject_name):
        """
        Registers a class to be used based on pyproject_name (the name in the top of pyproject.toml).
        Note that this means that cgap-portal and fourfront will both register as 'encoded',
        as in:

            @Project.register('encoded')
            class FourfrontProject(EncodedCoreProject):
                PRETTY_NAME = "Fourfront"

        Since fourfront and cgap-portal don't occupy the same space, no confusion should result.
        """

        if not isinstance(pyproject_name, str):
            raise ValueError(f"The pyprjoect_name given to {cls.__name__}.register must be a string:"
                             f" {pyproject_name!r}")

        def _wrap_class(the_class):

            the_class_name = the_class.__name__

            if not issubclass(the_class, cls.PROJECT_BASE_CLASS):
                raise ValueError(f"The class {the_class_name} must inherit from {cls.PROJECT_BASE_CLASS.__name__}.")

            explicit_attrs = the_class.__dict__

            identity_spec = explicit_attrs.get('IDENTITY')

            if not identity_spec:
                raise ValueError(f"Declaration of {the_class_name} must have a non-empty IDENTITY= specification.")

            if 'PYPROJECT_NAME' in identity_spec:
                raise ValueError(f"Explicitly specifying IDENTITY={{'PYPROJECT_NAME': ...}} is not allowed."
                                 f" The PYPROJECT_NAME is managed implicitly using information"
                                 f" given in the {cls.__name__}.register decorator.")

            for attr in cls.REQUIRED_IDENTITY_PARAMETERS:
                if attr not in identity_spec:
                    raise ValueError(f"Declaration of {the_class_name} IDENTITY= is missing {attr!r}.")

            identity_spec['PYPROJECT_NAME'] = pyproject_name

            lower_registry_name = pyproject_name.lower()
            for bad, better in cls.BAD_PYPROJECT_NAMES:
                if bad in lower_registry_name:  # substring check
                    # It's an easy error to make, but the name of the project from which we're gaining foothold
                    # in pyproject.toml is 'encoded', not 'cgap-portal', etc., so the name 'encoded' will be
                    # needed for bootstrapping. So it should look like
                    # -kmp 15-May-2023
                    raise ValueError(f"Please use {cls.__name__}.register({better!r}),"
                                     f" not {cls.__name__}.register({pyproject_name!r})."
                                     f" This name choice in project registration is just for bootstrapping."
                                     f" The class can still be {the_class_name}.")
            cls.REGISTERED_PROJECTS[pyproject_name] = the_class
            return the_class
        return _wrap_class

    @classmethod
    def find_pyproject(cls, name) -> Optional[Type[Project]]:
        """
        Returns the project object with the given name.

        :param name: a string name that was used in a ProjectRegistry.register decorator

        NOTE: There is no need for this function to be called outside of this class except for testing.
              Really only one of these should be instantiated per running application, and that's
              done automatically by this class.
        """
        project_class: Optional[Type[Project]] = cls.REGISTERED_PROJECTS.get(name)
        return project_class

    @classmethod
    def _make_project(cls) -> Project:
        """
        Creates and returns an instantiated project object for the current project.

        The project to use can be specified by setting the environment variable APPLICATION_PROJECT_HOME
        to a particular directory that contains the pyproject.toml file to use.
        If no such variable is set, the current working directory is used.

        NOTE: There is no need for this function to be called outside of this class except for testing.
              Really only one of these should be instantiated per running application, and that's
              done automatically by this class.
        """
        project_class: Optional[Type[Project]] = cls.find_pyproject(cls.PYPROJECT_NAME)
        if project_class is None:
            raise ValueError(f"Missing project class for pyproject {cls.PYPROJECT_NAME!r}.")
        if not issubclass(project_class, cls.PROJECT_BASE_CLASS):
            raise ValueError(f"Registered pyproject {cls.PYPROJECT_NAME!r} ({project_class.__name__})"
                             f" is not a subclass of {cls.PROJECT_BASE_CLASS.__name__}.")
        project: Project = project_class()
        return project  # instantiate and return

    _shared_app_project_cell = StorageCell(initial_value=None)

    @classmethod
    def initialize(cls, force=False) -> Project:
        shared_app_project: Optional[Project] = ProjectRegistry._shared_app_project_cell.value
        if shared_app_project and not force:
            return shared_app_project
        ProjectRegistry._shared_app_project_cell.value = cls._make_project()
        if cls.SHOW_HERALD_WHEN_INITIALIZED:
            cls.show_herald()
        app_project: Project = cls.app_project  # Now that it's initialized, make sure it comes from the right place
        return app_project

    @classmethod
    def show_herald(cls):
        app_project = cls.PROJECT_BASE_CLASS.app_project_maker()

        PRINT()  # start on a fresh line
        PRINT("=" * 80)
        PRINT(f"APPLICATION_PROJECT_HOME == {cls.APPLICATION_PROJECT_HOME!r}")
        PRINT(f"PYPROJECT_TOML_FILE == {cls.PYPROJECT_TOML_FILE!r}")
        PRINT(f"PYPROJECT_NAME == {cls.PYPROJECT_NAME!r}")
        the_app_project: Project = Project.app_project
        the_app_project_class: Type[Project] = the_app_project.__class__
        the_app_project_class_name: str = the_app_project_class.__name__
        assert (Project.app_project
                == app_project()
                == the_app_project_class.app_project
                == the_app_project.app_project), (
            "Project consistency check failed."
        )
        PRINT(f"{the_app_project_class_name}.app_project == Project.app_project == app_project() == {app_project()!r}")
        the_app: Project = app_project()
        for attr in sorted(dir(the_app)):
            val = getattr(the_app, attr)
            if attr.isupper() and not attr.startswith("_") and (val is None or isinstance(val, str)):
                PRINT(f"app_project().{attr} == {val!r}")
        PRINT("=" * 80)

    @classproperty
    def app_project(cls) -> Project:  # noQA - PyCharm thinks we should use 'self'
        """
        Once the project is initialized, ProjectRegistry.app_project returns the application object
        that should be used to dispatch project-dependent behavior.
        """
        app_project: Project = cls._shared_app_project_cell.value or cls.initialize()
        return app_project


Project.declare_project_registry_class(ProjectRegistry)  # Finalize a circular dependency


# --------------------------------------------------------------------------------
# C4-specific tools follow
# --------------------------------------------------------------------------------


class C4ProjectIdentity(ProjectIdentity):

    @classmethod
    def appify(cls, name: str) -> str:
        return name.replace('-portal', '').replace('encoded-', '')

    @classmethod
    def repofy(cls, name: str) -> str:
        return name.replace('dcic', '')

    @classmethod
    def prettify(cls, name: str) -> str:
        return super().prettify(name).replace("Cgap", "CGAP").replace("Smaht", "SMaHT")


class C4Project(Project):
    """
    Uses C4 functionality for naming. Outside organizations may not want such heuristics.
    """

    IDENTITY_CLASS = C4ProjectIdentity


class C4ProjectRegistry(ProjectRegistry):

    PROJECT_BASE_CLASS = C4Project

    BAD_PYPROJECT_NAMES = [('cgap-portal', 'encoded'),
                           ('fourfront', 'encoded'),
                           ('smaht', 'encoded')]


# Finalize a circular dependency

C4Project.declare_project_registry_class(C4ProjectRegistry)  # Finalize a circular dependency
