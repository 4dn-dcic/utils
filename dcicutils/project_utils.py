import importlib
import os
import toml

from pkg_resources import resource_filename
from typing import Callable, Optional, Type
from .misc_utils import PRINT, StorageCell, classproperty, environ_bool, ignored, get_error_message
from .lang_utils import conjoined_list, maybe_pluralize


class ProjectNames:

    def __init__(self, *, PYPROJECT_NAME, NAME, PRETTY_NAME=None, REPO_NAME=None, PYPI_NAME=None,
                 PACKAGE_NAME=None, APP_NAME=None, APP_PRETTY_NAME=None):
        self.PYPROJECT_NAME = PYPROJECT_NAME
        self.NAME = NAME
        self.PRETTY_NAME = PRETTY_NAME or self.prettify(NAME)
        self.PYPI_NAME = PYPI_NAME or None  # canonicalize '' to None, just in case
        self.REPO_NAME = REPO_NAME = REPO_NAME or self.repofy(NAME)
        self.APP_NAME = APP_NAME = APP_NAME or self.appify(REPO_NAME)
        self.APP_PRETTY_NAME = APP_PRETTY_NAME or self.prettify(APP_NAME)
        # This uses ProjectRegistry (not cls) because that part of the protocol is common to all
        # classes and subclasses of that class. No need to worry about specialized classes.
        # -kmp 19-May-2023
        inferred_package_name = self.infer_package_name(poetry_data=ProjectRegistry.POETRY_DATA,
                                                        pyproject_name=PYPROJECT_NAME)
        if PACKAGE_NAME:
            if PACKAGE_NAME != inferred_package_name:
                raise Exception(f"Explicit PACKAGE_NAME={PACKAGE_NAME}"
                                f" does not match inferred name {inferred_package_name}.")
        self.PACKAGE_NAME = inferred_package_name

    @classmethod
    def prettify(cls, name: str) -> str:
        """
        Turns a token name (possibly hyphenated) into a pretty name with capitalized words.
        e.g, "foo-bar" => "Foo Bar"

        :param name: the string to transform
        :return: the pretty version of the name
        """
        return name.title().replace("-", " ")

    @classmethod
    def appify(cls, name: str) -> str:
        """
        Turns a repo name into an app name.
        This method is an identity operation in this class, but might be subclassed in subclasses.
        For example, it might remove superfluous modifiers that are not distinguishing characteristics of the app.
        e.g., in this class "cgap-portal" is like "foo-bar-baz" and is just returned directly,
        but in a subclass this method might be customized to shorten "cgap-portal" to "cgap".
        Or if there was a "cgap-support" repo, it might also want to refer to an app name of "cgap".
        """
        return name

    @classmethod
    def repofy(cls, name: str) -> str:
        """
        Turns a pypi name into a repo name.
        In this class, this is just an identity, but it might be customized in a subclass.
        For example, in the C4 projects, "dcicutils" uses the repo name "utils", and "dcicsnovault" uses
        the repo "snovault", so a subclass of this class might be customized to strip "dcic" from the name.
        """
        return name

    @classmethod
    def infer_package_name(cls, poetry_data, pyproject_name, as_dir=False):
        try:
            if poetry_data:
                # We expect the first package in the declared packages to be the primary export
                # Other exported dirs, such as scripts or tests should be in later entries.
                entry = poetry_data['packages'][0]
                include = entry['include']
                from_dir = entry.get('from', '.')
                return os.path.join(from_dir, include) if as_dir else include
        except Exception:
            pass
        result = pyproject_name
        if not result:
            raise ValueError(f"Unable to infer package name given"
                             f" poetry_data={poetry_data!r} and pyproject_name={pyproject_name!r}.")
        return result

    def items(self):
        """
        Given a ProjectNames instance, this returns a list of the form [(name_key1, name_val1), ...].
        """
        result = []
        for attr in sorted(dir(self)):
            val = getattr(self, attr)
            if attr.isupper() and not attr.startswith("_") and (val is None or isinstance(val, str)):
                result.append((attr, val))
        return sorted(result)

    def find_notable_aliases(self):
        """
        This function is intended to call out unexpected name variations so that if inheritance goes wrong,
        or there's a typo, it will get noticed for review.

        :return: a representative list of aliases that are substantively distinct from the main name
        """
        def shorten(name):
            return "".join([ch for ch in name.lower() if ch.isalnum()])
        short_name = shorten(self.NAME)
        seen = set()
        notable_aliases = []
        for attr, val in self.items():
            ignored(attr)
            # We assume the shortened name (removing weird syntax characters) of this project is a canonical form
            # that is included in the other names. Where this is not so is not necessarily an error, but it's
            # worth calling out in case it's a configuration problem of some sort.
            if val:
                short_val = shorten(val)
                if short_name != short_val:
                    if short_val not in seen:
                        seen.add(short_val)
                        notable_aliases.append(val)
        notable_aliases = sorted(notable_aliases)
        return notable_aliases


_SHARED_APP_PROJECT_CELL = StorageCell(initial_value=None)


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

    # This contains only the initial spec and is NOT used after initialization. See .names (and ._names).
    # It is declared in this way so that actual declarations of project classes don't show up as syntax errors.
    NAMES: Optional[dict] = None

    NAMES_CLASS: Type[ProjectNames] = ProjectNames

    _names: Optional[ProjectNames] = None  # Class-level default, overridden in instances as a ProjectNames

    _PROJECT_REGISTRY_CLASS = None

    def __init__(self):
        self._names: ProjectNames = self.NAMES_CLASS(**self.NAMES)

    def __str__(self):
        return f"<{self.__class__.__name__}>"

    def __repr__(self):
        return self.__str__()

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

    @property
    def names(self) -> ProjectNames:
        if self._names is None:
            raise ValueError(f"<{self.__class__.__name__}> failed to initialize correctly.")
        return self._names

    @property
    def PYPROJECT_NAME(self) -> str:
        return self.names.PYPROJECT_NAME

    @property
    def NAME(self):
        return self.names.NAME

    @property
    def REPO_NAME(self) -> str:
        return self.names.REPO_NAME

    @property
    def PYPI_NAME(self):
        return self.names.PYPI_NAME

    @property
    def PACKAGE_NAME(self) -> str:
        return self.names.PACKAGE_NAME

    @property
    def PRETTY_NAME(self):
        return self.names.PRETTY_NAME

    @property
    def APP_NAME(self):
        return self.names.APP_NAME

    @property
    def APP_PRETTY_NAME(self):
        return self.names.APP_PRETTY_NAME

    @classproperty
    def app_project(cls):  # noQA - PyCharm wants the variable name to be self
        """
        Project.app_project returns the actual instantiated project for app-specific behavior,
        which might be of this class or one of its subclasses.
        """
        return cls.PROJECT_REGISTRY_CLASS.app_project

    @classmethod
    def app_project_maker(cls):
        """
        Returns a function that, when invoked, will yield the proper app project,
        initializing that value in demand if it has not been previously initialized.

        NOTES:
        * When using C4Project classes, please always use <your-class>.app_project_maker() or
          C4ProjectRegistry.app_project_maker() so that C4 policies will be applied upon demand-creation.

        * Note that by the time of first call to that function, the appropriate environment must be in place,
          or an autoload will be attempted from your project's project_defs.py file.
          If you want advance control of the specific environment in which the initialization will occur,
          use <registry-class>.initialize().
        """
        return cls.PROJECT_REGISTRY_CLASS.app_project_maker()

    def project_filename(self, filename):
        """Returns a filename relative to given instance."""
        current_project = self.app_project
        if self is not current_project:
            raise RuntimeError(f"{self}.project_filename invoked,"
                               f" but {self} is not the app_project, {current_project}.")
        return resource_filename(self.PACKAGE_NAME, filename)

    @classmethod
    def initialize(cls, force=False, verbose: Optional[bool] = None, detailed: Optional[bool] = None):  # -> Project
        app_project: Project = cls.PROJECT_REGISTRY_CLASS.initialize(force=force, verbose=verbose, detailed=detailed)
        return app_project

    def show_herald(self, detailed: bool = False):

        app_project = self.PROJECT_REGISTRY_CLASS.app_project_maker()

        the_app_project: Project = self.app_project
        the_app_project_class: Type[Project] = the_app_project.__class__
        the_app_project_class_name: str = the_app_project_class.__name__

        # Take this moment to do some important consistency checks to make sure all is well.
        if not (self
                == self.__class__.app_project
                == Project.app_project
                == ProjectRegistry.app_project
                == self.PROJECT_REGISTRY_CLASS.app_project
                == app_project()
                == the_app_project_class.app_project
                == the_app_project.app_project):
            raise RuntimeError("Project consistency check failed.")

        if detailed:
            PRINT()  # start on a fresh line
            PRINT("=" * 90)
            PRINT(f"APPLICATION_PROJECT_HOME == {self.PROJECT_REGISTRY_CLASS.APPLICATION_PROJECT_HOME!r}")
            PRINT(f"PYPROJECT_TOML_FILE == {self.PROJECT_REGISTRY_CLASS.PYPROJECT_TOML_FILE!r}")
            PRINT(f"PYPROJECT_NAME == {self.PROJECT_REGISTRY_CLASS.PYPROJECT_NAME!r}")
            PRINT(f"Project.app_project == ProjectRegistry.app_project == app_project() == {app_project()!r}")
            for attr, val in self.names.items():
                PRINT(f"app_project().{attr} == {val!r}")
            PRINT("=" * 90)
        else:
            aliases = self.names.find_notable_aliases()
            extra = (f" and {maybe_pluralize(aliases, 'notable alias')} {conjoined_list(sorted(map(repr, aliases)))}"
                     if aliases
                     else "")
            PRINT(f"{the_app_project_class_name} initialized with name {self.NAME!r}{extra}.")


"""

Design Notes relating to Project.app_project_maker
---------------------------------------------------

In your `project_defs.rst` file, you should write something like::

    from dcicutils.project_utils import C4ProjectRegistry, C4Project

    @C4ProjectRegistry.register('dcicsnovault')
    class SnovaultProject(C4Project):
        NAMES = {'NAME': 'snovault', 'PYPI_NAME': 'dcicsnovault'}
        ACCESSION_PREFIX = 'SNO'

    app_project = SnovaultProject.app_project_maker()

You might very reasonably ask, why isn't `app_project` a method on this class.
There are several conspiring reasons:

* Invoking it would then involve more text than we want. We would have to write
  Project.app_project, for example, rather than app_project().

* If it's a C4Project, not a Project, we'd need to use C4Project.app_project in
  case demand-initialization happens, because that needs to call C4ProjectRegistry.initialize(),
  NOT ProjectRegistry.initialize(), to get appropriate initialization methods.
  Since we're already presumably importing just C4Project to make our SnovaultProject,
  or importing SnovaultProject to make EncodedCoreProject, or importing EncodedCoreProject
  or SnovaultProject to make a portal project, it's best to just call that class
  to make the maker, and then you can't accidentally do it wrong.

* If the method is already on the class, you might be tempted in another file to import the
  method from that file, without loading your repository's own project definition. By putting
  the creation of this function in the same file as the class definition, you are assured that
  all appropriate support will be loaded. Fortunately, there is another cross-check on this as
  well, since the computation of what class to load will compute a name that is only defined in
  the .register() call, and if you haven't already loaded that file, the lookup of the proper
  class should efail because things are undefined.

Note that if you don't a pyproject.toml and you also have not yet set the environment variable
APPLICATION_PROJECT_NAME to an appropriate value by time of first call, initialization will (rightly)
fail, so you'll know there is a problem. At least in a production system.

The primary thing to worry about is in development if you have globally assigned APPLICATION_PROJECT_NAME
because if it's set to something more low-level than you want, like to snovault, that's what's going
to launch. That risk is small and it should be easy to notice that problem. If you've set it to 'encoded',
it should launch the portal for whatever repo you're in, which won't be that terrible. If you meant
fourfront but are in a cgap-portal repo, you it's not clear what would have been better.

"""


class ProjectRegistry:

    PROJECT_BASE_CLASS: Type[Project] = Project

    # If true, a herald will be shown; otherwise (if false), no herald will be shown
    PROJECT_INITIALIZE_VERBOSE = environ_bool("PROJECT_INITIALIZE_VERBOSE", default=True)

    # If true, any herald shown will have multi-line details; otherwise (if false), it'll be a single-line summary.
    PROJECT_INITIALIZE_DETAILED = environ_bool("PROJECT_INITIALIZE_DETAILED", default=False)

    REGISTERED_PROJECTS = {}

    # All of these might never be other than None so be careful when accessing them.
    APPLICATION_PROJECT_HOME = None
    PYPROJECT_TOML_FILE = None
    PYPROJECT_TOML = None
    POETRY_DATA = None

    # This is expected to ultimately be set properly. It is only None while bootstrapping.
    _PYPROJECT_NAME = None

    @classproperty
    def PYPROJECT_NAME(cls) -> str:  # noQA - PyCharm thinks this should be 'self'
        if cls._PYPROJECT_NAME is None:
            cls._initialize_pyproject_name()
        result: Optional[str] = cls._PYPROJECT_NAME
        if result is None:
            raise ValueError(f"{cls.__name__}.PYPROJECT_NAME not initialized properly.")
        return result

    @classmethod
    def _initialize_pyproject_name(cls, project_home=None, pyproject_toml_file=None,
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
                expected_pyproject_toml_file = (os.path.join(project_home, "pyproject.toml")
                                                if project_home
                                                else "pyproject.toml")
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

    REQUIRED_NAMES_KEYS = ['NAME']

    PROTECTED_NAMES_KEYS = ['PYPROJECT_NAME']

    DISALLOWED_PYPROJECT_NAME_HINTS = []

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

            names_spec = explicit_attrs.get('NAMES')

            if not names_spec:
                raise ValueError(f"Declaration of {the_class_name} must have a non-empty NAMES= specification.")

            if 'PYPROJECT_NAME' in names_spec:
                raise ValueError(f"Explicitly specifying NAMES={{'PYPROJECT_NAME': ...}} is not allowed."
                                 f" The PYPROJECT_NAME is managed implicitly using information"
                                 f" given in the {cls.__name__}.register decorator.")

            for attr in cls.REQUIRED_NAMES_KEYS:
                if attr not in names_spec:
                    raise ValueError(f"Declaration of {the_class_name} NAMES= is missing {attr!r}.")

            names_spec['PYPROJECT_NAME'] = pyproject_name

            lower_pyproject_name = pyproject_name.lower()
            for disallowed, hint in cls.DISALLOWED_PYPROJECT_NAME_HINTS:
                if disallowed.lower() == lower_pyproject_name:  # substring check
                    # It's an easy error to make, but the name of the project from which we're gaining foothold
                    # in pyproject.toml is 'encoded', not 'cgap-portal', etc., so the name 'encoded' will be
                    # needed for bootstrapping. So it should look like
                    # -kmp 15-May-2023
                    raise ValueError(f"Please use {cls.__name__}.register({hint!r}),"
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
            package_name = ProjectNames.infer_package_name(poetry_data=cls.POETRY_DATA,
                                                           pyproject_name=cls.PYPROJECT_NAME)
            clarification = ""
            if package_name != cls.PYPROJECT_NAME:
                clarification = f" (package {package_name!r})"
            PRINT(f"Autoloading project_defs.py for pyproject {cls.PYPROJECT_NAME!r}{clarification}.")
            try:
                # PRINT(f"package_name={package_name}")
                importlib.import_module(name=".project_defs", package=package_name)
            except Exception as e:
                PRINT(f"Autoload failed for project_defs in pyproject {cls.PYPROJECT_NAME!r}{clarification}."
                      f" {get_error_message(e)}")
            project_class: Optional[Type[Project]] = cls.find_pyproject(cls.PYPROJECT_NAME)
        if project_class is None:
            raise ValueError(f"Missing project class for pyproject {cls.PYPROJECT_NAME!r}.")
        if not issubclass(project_class, cls.PROJECT_BASE_CLASS):
            raise ValueError(f"Registered pyproject {cls.PYPROJECT_NAME!r} ({project_class.__name__})"
                             f" is not a subclass of {cls.PROJECT_BASE_CLASS.__name__}.")
        project: Project = project_class()
        return project  # instantiate and return

    @classmethod
    def initialize(cls, force=False, verbose: Optional[bool] = None, detailed: Optional[bool] = None) -> Project:
        show_herald = cls.PROJECT_INITIALIZE_VERBOSE if verbose is None else verbose
        detailed = cls.PROJECT_INITIALIZE_DETAILED if detailed is None else detailed
        shared_app_project: Optional[Project] = _SHARED_APP_PROJECT_CELL.value
        if shared_app_project and not force:
            return shared_app_project
        _SHARED_APP_PROJECT_CELL.value = cls._make_project()
        app_project: Project = cls.app_project  # Now that it's initialized, make sure it comes from the right place
        if show_herald:
            app_project.show_herald(detailed=detailed)
        return app_project

    @classproperty
    def app_project(cls) -> Project:  # noQA - PyCharm thinks we should use 'self'
        """
        Once the project is initialized, ProjectRegistry.app_project returns the application object
        that should be used to dispatch project-dependent behavior.
        """
        app_project: Project = _SHARED_APP_PROJECT_CELL.value or cls.initialize()
        return app_project

    @classmethod
    def app_project_maker(cls) -> Callable[[], Project]:
        """
        Returns a function that, when invoked, will yield the proper app project,
        initializing that value in demand if it has not been previously initialized.

        NOTES:
        * When using C4Project classes, please always use <your-class>.app_project_maker() or
          C4ProjectRegistry.app_project_maker() so that C4 policies will be applied upon demand-creation.

        * Note that by the time of first call to that function, the appropriate environment must be in place,
          or an autoload will be attempted from your project's project_defs.py file.
          If you want advance control of the specific environment in which the initialization will occur,
          use <registry-class>.initialize().
        """
        def app_project() -> Project:
            return cls.app_project

        return app_project


Project.declare_project_registry_class(ProjectRegistry)  # Finalize a circular dependency


"""

C4-specific Subclasses
======================

The above classes are general-purpose for all potential users, which their C4-specific
subclasses add additional policy detailing that is specific to the work of the C4 team.

"""


class C4ProjectNames(ProjectNames):

    @classmethod
    def prettify(cls, name: str) -> str:
        """
        Tries to prettify a string name within the C4 world.

        C4 is the shorthand name for 4DN, CGAP, and other projects at DBMI.

        :param name: the name to transform
        :return: the pretty name
        """
        return super().prettify(name).replace("Cgap", "CGAP").replace("Smaht", "SMaHT")

    @classmethod
    def appify(cls, name: str) -> str:
        """
        Tries to transform a C4 repo name into a C4 app name.

        C4 is the shorthand name for 4DN, CGAP, and other projects at DBMI.

        :param name: the name to transform
        :return: the app-style name
        """
        return name.replace('-portal', '').replace('encoded-', '')

    @classmethod
    def repofy(cls, name: str) -> str:
        """
        Tries to transform a C4 pypi name into a C4 repo name.

        C4 is the shorthand name for 4DN, CGAP, and other projects at DBMI.

        :param name: the name to transform
        :return: the repo-style name
        """
        return name.replace('dcic', '')


class C4Project(Project):
    """
    Uses C4 functionality for naming. Outside organizations may not want such heuristics.
    """

    NAMES_CLASS = C4ProjectNames


class C4ProjectRegistry(ProjectRegistry):
    """
    Allows the same kinds of registration operations that its parent, ProjectRegistry, would
    allow, but with additional error-checks that are C4-specific. Also, this is expected to be
    used in conjunction with C4Project, rather than just Project.
    """

    PROJECT_BASE_CLASS = C4Project

    DISALLOWED_PYPROJECT_NAME_HINTS = [('cgap-portal', 'encoded'),
                                       ('fourfront', 'encoded'),
                                       ('smaht-portal', 'encoded')]


# Finalize a circular dependency

C4Project.declare_project_registry_class(C4ProjectRegistry)  # Finalize a circular dependency


"""

About the name "C4"
-------------------

The name C4 originally was used as a way of speaking about the combined efforts of the CGAP and 4DN
teams at Harvard's Park Lab, part of Harvard Medical School (HMS) / Department of Biomedical Informatics (DBMI).
The mission of that team of of people has broadened further to include work on SMaHT, but we continue to use
the term C4 to refer to the space of tools that span all of these projects because they all use a common base of tools.

"""
