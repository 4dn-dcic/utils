import os
import toml

from pkg_resources import resource_filename
from typing import Optional
from .env_utils import EnvUtils
from .misc_utils import classproperty, check_true, ignorable, PRINT


class ProjectRegistry:

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
            raise ValueError(f"ProjectRegistry.PROJECT_NAME not initialized properly.")
        return result

    @classmethod
    def initialize_pyproject_name(cls, project_home=None, pyproject_toml_file=None):
        if cls._PYPROJECT_NAME is None:
            # This isn't the home of Project, but the home of the Project-based application.
            # So in CGAP, for example, this would want to be the home of the CGAP application.
            # If not set, it will be assumed that the current working directory is that.
            # print("Setting up data.")
            if not project_home:
                project_home = os.environ.get("APPLICATION_PROJECT_HOME", os.path.abspath(os.curdir))
            cls.APPLICATION_PROJECT_HOME = project_home
            if not pyproject_toml_file:
                expected_pyproject_toml_file = os.path.join(project_home, "pyproject.toml")
                pyproject_toml_file = (expected_pyproject_toml_file
                                       if os.path.exists(expected_pyproject_toml_file)
                                       else None)
            cls.PYPROJECT_TOML_FILE = pyproject_toml_file
            # print(f"Loading toml file {cls.PYPROJECT_TOML_FILE}")
            cls.PYPROJECT_TOML = pyproject_toml = (toml.load(cls.PYPROJECT_TOML_FILE)
                                                   if cls.PYPROJECT_TOML_FILE
                                                   else None)
            poetry_data = (pyproject_toml['tool']['poetry']
                           if pyproject_toml
                           else None)
            # print(f"Setting POETRY_DATA = {poetry_data}")
            cls.POETRY_DATA = poetry_data

            declared_pyproject_name = os.environ.get("APPLICATION_PYPROJECT_NAME")
            inferred_pyproject_name = cls.POETRY_DATA['name'] if cls.POETRY_DATA else None
            if (declared_pyproject_name and inferred_pyproject_name
                    and declared_pyproject_name != inferred_pyproject_name):
                raise RuntimeError(f"APPLICATION_PYPROJECT_NAME={declared_pyproject_name!r},"
                                   f" but {pyproject_toml_file} says it should be {inferred_pyproject_name!r}")

            cls._PYPROJECT_NAME = declared_pyproject_name or inferred_pyproject_name

    REQUIRED_PROJECT_ATTRS = ['NAME']

    NON_INHERITED_PROJECT_ATTRS = [
        # PYPROJECT_NAME and PACKAGE_NAME are also in this set but handled by special case.
        'REPO_NAME', 'PYPI_NAME', 'PRETTY_NAME', 'APP_NAME', 'APP_PRETTY_NAME'
    ]

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

        protected_project_bindings = [('PYPROJECT_NAME', '_PYPROJECT_NAME', pyproject_name, False),
                                      ('PACKAGE_NAME', '_PACKAGE_NAME', None, True)]

        def _wrap_class(the_class):
            the_class_name = the_class.__name__

            base_class = Project
            registered_classes = set(cls.REGISTERED_PROJECTS.values())
            for c in the_class.__mro__[1:]:
                # Prefer an unregistered subclass of Project if one was used (such as C4Project)
                if c not in registered_classes and issubclass(c, base_class):
                    # print(f"Using base_class = {c}")
                    base_class = c
                    break

            def reset_attr(the_class, attr):
                # We can't just delete the property because an inherited value might show through.
                # We need to actually force a local copy of the base class value.
                val = None
                found_class = None
                for c in the_class.__mro__[1:]:
                    class_dict = c.__dict__
                    if attr in class_dict and c not in registered_classes:
                        found_class = c
                        val = class_dict[attr]
                        break
                ignorable(found_class)
                # print(f"Resetting {the_class}.{attr} to {val!r} from {found_class!r}.")
                setattr(the_class, attr, val)

            if not issubclass(the_class, base_class):
                raise ValueError(f"The class {the_class_name} must inherit from {base_class.__name__}.")

            explicit_attrs = the_class.__dict__

            for public_attr, private_attr, default_val, assign_ok in protected_project_bindings:
                if private_attr in explicit_attrs:
                    raise ValueError(f"{private_attr} is an internally managed variable. You must not set it.")
                if public_attr in explicit_attrs:
                    explicit_val = explicit_attrs[public_attr]
                    if assign_ok:
                        reset_attr(the_class, public_attr)
                        setattr(the_class, private_attr, explicit_val)
                    else:
                        raise ValueError(f"Explicit {the_class_name}.{public_attr}={explicit_val!r} is not permitted."
                                         f" This assignment is intended to be managed implicitly.")
                else:
                    setattr(the_class, private_attr, default_val)

            for attr in cls.REQUIRED_PROJECT_ATTRS:
                if attr not in explicit_attrs:
                    raise ValueError(f".{attr} is required")

            for attr in cls.NON_INHERITED_PROJECT_ATTRS:
                if attr not in explicit_attrs:
                    reset_attr(the_class, attr)

            lower_registry_name = pyproject_name.lower()
            for x in ['cgap-portal', 'fourfront', 'smaht']:
                if x in lower_registry_name:
                    # It's an easy error to make, but the name of the project from which we're gaining foothold
                    # in pyproject.toml is 'encoded', not 'cgap-portal', etc., so the name 'encoded' will be
                    # needed for bootstrapping. So it should look like
                    # -kmp 15-May-2023
                    raise ValueError(f"Please use ProjectRegistry.register('encoded'),"
                                     f" not ProjectRegistry.register({pyproject_name!r})."
                                     f" This registration is just for bootstrapping."
                                     f" The class can still be {the_class_name}.")
            cls.REGISTERED_PROJECTS[pyproject_name] = the_class
            return the_class
        return _wrap_class

    @classmethod
    def _lookup(cls, name):
        """
        Returns the project object with the given name.

        :param name: a string name that was used in a ProjectRegistry.register decorator

        NOTE: There is no need for this function to be called outside of this class except for testing.
              Really only one of these should be instantiated per running application, and that's
              done automatically by this class.
        """
        project_class = cls.REGISTERED_PROJECTS.get(name)
        return project_class

    @classmethod
    def _make_project(cls):
        """
        Creates and returns an instantiated project object for the current project.

        The project to use can be specified by setting the environment variable APPLICATION_PROJECT_HOME
        to a particular directory that contains the pyproject.toml file to use.
        If no such variable is set, the current working directory is used.

        NOTE: There is no need for this function to be called outside of this class except for testing.
              Really only one of these should be instantiated per running application, and that's
              done automatically by this class.
        """
        project_class = cls._lookup(cls.PYPROJECT_NAME)
        check_true(project_class is not None, error_class=RuntimeError,
                   message=f"Missing project class {cls.PYPROJECT_NAME}.",)
        check_true(issubclass(project_class, Project), error_class=ValueError,
                   message=f"Registered project class is not a subclass of Project.")
        project: Project = project_class()
        return project  # instantiate and return

    _app_project = None

    @classmethod
    def initialize(cls, force=False):
        if cls._app_project and not force:
            return cls._app_project
        cls._app_project = cls._make_project()
        if cls.SHOW_HERALD_WHEN_INITIALIZED:
            cls.show_herald()
        app_project: Project = cls.app_project  # Now that it's initialized, make sure it comes from the right place
        return app_project

    @classmethod
    def show_herald(cls):
        app_project = Project.app_project_maker()

        PRINT("=" * 80)
        PRINT(f"APPLICATION_PROJECT_HOME == {cls.APPLICATION_PROJECT_HOME!r}")
        PRINT(f"PYPROJECT_TOML_FILE == {cls.PYPROJECT_TOML_FILE!r}")
        PRINT(f"PYPROJECT_NAME == {cls.PYPROJECT_NAME!r}")
        the_app_project = Project.app_project
        the_app_project_class = the_app_project.__class__
        the_app_project_class_name = the_app_project_class.__name__
        assert (Project.app_project
                == app_project()
                == the_app_project_class.app_project
                == the_app_project.app_project), (
            "Project consistency check failed."
        )
        PRINT(f"{the_app_project_class_name}.app_project == Project.app_project == app_project() == {app_project()!r}")
        the_app = app_project()
        for attr in sorted(dir(the_app)):
            val = getattr(the_app, attr)
            if attr.isupper() and not attr.startswith("_") and (val is None or isinstance(val, str)):
                PRINT(f"app_project().{attr} == {val!r}")
        PRINT("=" * 80)

    @classproperty
    def app_project(cls):  # noQA - PyCharm thinks we should use 'self'
        """
        Once the project is initialized, ProjectRegistry.app_project returns the application object
        that should be used to dispatch project-dependent behavior.
        """
        app_project: Project = cls._app_project or cls.initialize()
        return app_project


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

    NAME = 'project'
    PYPI_NAME = None

    _PYPROJECT_NAME = None
    _PACKAGE_NAME = None

    @classmethod
    def _prettify(cls, name):
        return name.title().replace("-", " ")

    @classproperty
    def REPO_NAME(cls):  # noQA - PyCharm wants the variable name to be self
        return cls.NAME

    @classproperty
    def PACKAGE_NAME(cls):  # noQA - PyCharm wants the variable name to be self
        if cls._PACKAGE_NAME:
            return cls._PACKAGE_NAME
        poetry_data = ProjectRegistry.POETRY_DATA
        if poetry_data:
            # print(f"There is poetry data: {ProjectRegistry.POETRY_DATA}")
            # We expect the first package in the declared packages to be the primary export
            # Other exported dirs, such as scripts or tests should be in later entries.
            package_name = poetry_data['packages'][0]['include']
        else:
            # print(f"No poetry data. cls.PYPI_NAME={cls.PYPI_NAME} cls.PYPROJECT_NAME={cls.PYPROJECT_NAME}")
            package_name = cls.PYPI_NAME or cls.PYPROJECT_NAME
        cls._PACKAGE_NAME = package_name
        return package_name

    @classproperty
    def PYPROJECT_NAME(cls) -> str:  # noQA - PyCharm wants the variable name to be self
        pyproject_name = cls._PYPROJECT_NAME
        if not pyproject_name:
            raise RuntimeError(f"Class {cls} was not defined with ProjectRegistry.register")
        return pyproject_name

    @classproperty
    def PRETTY_NAME(cls):  # noQA - PyCharm wants the variable name to be self
        return cls._prettify(cls.NAME)

    @classproperty
    def APP_NAME(cls):  # noQA - PyCharm wants the variable name to be self
        return cls.REPO_NAME.replace('-portal', '').replace('encoded-', '')

    @classproperty
    def APP_PRETTY_NAME(cls):  # noQA - PyCharm wants the variable name to be self
        return cls._prettify(cls.APP_NAME)

    @classproperty
    def app_project(cls):  # noQA - PyCharm wants the variable name to be self
        """
        Project.app_project returns the actual instantiated project for app-specific behavior,
        which might be of this class or one of its subclasses.

        This access will fail if the project has not been initialized.
        """
        return ProjectRegistry.app_project

    @classmethod
    def initialize_app_project(cls, initialize_env_utils=True):
        if initialize_env_utils:
            EnvUtils.init()
        project: Project = ProjectRegistry.initialize()
        return project

    @classmethod
    def app_project_maker(cls):

        def app_project(initialize=False, initialization_options: Optional[dict] = None):
            if initialize:
                Project.initialize_app_project(**(initialization_options or {}))
            return Project.app_project

        return app_project

    def project_filename(self, filename):
        """Returns a filename relative to given instance."""
        if self != self.app_project:
            raise RuntimeError(f"{self}.project_filename invoked,"
                               f" but {self} is not the app_project, {self.app_project}.")
        return resource_filename(self.PACKAGE_NAME, filename)


class C4Project(Project):
    """
    Collect C4-specific functionality so that outside organizations don't have to use these heuristics
    if they don't want to.
    """

    @classproperty
    def REPO_NAME(cls):  # noQA - PyCharm wants the variable name to be self
        return cls.NAME.replace('dcic', '')

    @classmethod
    def _prettify(cls, name):
        return super()._prettify(name).replace("Cgap", "CGAP").replace("Smaht", "SMaHT")
