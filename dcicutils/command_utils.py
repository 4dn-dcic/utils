import contextlib
import glob
import logging
import os
import requests
import subprocess

from dcicutils.exceptions import InvalidParameterError
from dcicutils.lang_utils import there_are
from typing import Optional
from .misc_utils import PRINT


def _ask_boolean_question(question, quick=None, default=None):
    """
    Loops asking a question interactively until it gets a 'yes' or 'no' response. Returns True or False accordingly.

    :param question: The question to ask (without prompts for possible responses, which will be added automatically).
    :param quick: Whether to allow short-form responses.
    :param default: Whether to provide a default obtained by just pressing Enter.
    :return: True or False
    """

    if quick is None:
        # If the default is not None, we're accepting Enter for yes, so we might as well accept 'y'.
        quick = (default is not None)

    affirmatives = ['y', 'yes'] if quick else ['yes']
    negatives = ['n', 'no'] if quick else ['no']
    affirmative = affirmatives[0]
    negative = negatives[0]
    prompt = ("%s [%s/%s]: "
              % (question,
                 affirmative.upper() if default is True else affirmative,
                 negative.upper() if default is False else negative))
    while True:
        answer = input(prompt).strip().lower()
        if answer in affirmatives:
            return True
        elif answer in negatives:
            return False
        elif answer == "" and default is not None:
            return default
        else:
            PRINT("Please answer '%s' or '%s'." % (affirmative, negative))
            if default is not None:
                PRINT("The default if you just press Enter is '%s'."
                      % (affirmative if default else negative))


def yes_or_no(question):
    """
    Asks a 'yes' or 'no' question.

    Either 'y' or 'yes' (in any case) is an acceptable affirmative.
    Either 'n' or 'no' (in any case) is an acceptable negative.
    The response must be confirmed by pressing Enter.
    There is no default. If Enter is pressed after other text than the valid responses, the user is reprompted.
    """
    return _ask_boolean_question(question, quick=False)


def y_or_n(question, default=None):
    """
    Asks a 'y' or 'n' question.

    Either 'y' or 'yes' (in any case) is an acceptable affirmative.
    Either 'n' or 'no' (in any case) is an acceptable negative.
    The response must be confirmed by pressing Enter.
    If Enter is pressed with no input text, the default is returned if there is one, or else the user is re-prompted.
    If Enter is pressed after other text than the valid responses, the user is reprompted.
    """
    return _ask_boolean_question(question, quick=True, default=default)


class ShellScript:
    """
    This is really an internal class. You're intended to work via the shell_script context manager.
    But there might be uses for this class, too, so we'll give it a pretty name.
    """

    # This is the shell the script will use to execute
    EXECUTABLE = "/bin/bash"

    def __init__(self, executable: Optional[str] = None, simulate=False, no_execute: bool = False, **script_options):
        """
        Creates an object that will let you compose a script and eventually execute it as a subprocess.

        :param executable: the executable file to use when executing the script (default /bin/bash)
        :param simulate: a boolean that says whether to simulate the script without executing it (default False)
        """

        if script_options:
            raise ValueError(f"Unknown script_options supplied: {script_options}")
        self.executable = executable or self.EXECUTABLE
        self.simulate = simulate
        self.no_execute = no_execute
        self.script = ""

    def do_first(self, command: str):
        """
        Adds the command to the front of the list of commands to be executed.
        This isn't really executing the command, just attaching it to the script being built (at the start).
        """
        if self.script:
            self.script = f'{command}; {self.script}'
        else:
            self.script = command

    def do(self, command: str):
        """
        Adds the command to the list of commands to be executed.
        This isn't really executing the command, just attaching it to the script being built (at the end).
        """
        if self.script:
            self.script = f'{self.script}; {command}'
        else:
            self.script = command

    def pushd(self, working_dir):
        """
        Adds a script element that pushes to a given working directory. See .popd()
        """
        self.do(f'pushd {working_dir} > /dev/null')
        self.do(f'echo "Selected working directory $(pwd)."')

    def popd(self):
        """
        Adds a script element that pops back to the last directory pushed from. See .pushd().
        """
        self.do(f'popd > /dev/null')
        self.do(f'echo "Restored working directory $(pwd)."')

    @contextlib.contextmanager
    def using_working_dir(self, working_dir):
        """
        When composing a shell script, this will bracket any commands created in the body context
        with commands that push to the indicated working dir, and then pop back from it after.
        """
        if working_dir:
            self.pushd(working_dir)
        yield self
        if working_dir:
            self.popd()

    def execute(self, **pipe_args):
        """This is where it's really executed."""

        # Well, it MIGHT be really executed. The no_execute option can fully suppress that so
        # that the commands are accumulated for delayed execution. Not even simulation is done
        # here in that case. (e.g., see the 'done_first' context manager)
        # But this option is subprimitive. Most users will want 'simulate'.
        # -kmp 2-Nov-2021

        if not self.no_execute:  # fully suppresses execution. see rationale above
            if self.simulate:
                PRINT("SIMULATED:")
                PRINT("=" * 80)
                PRINT(self.script.replace('; ', ';\\\n '))
                PRINT("=" * 80)
            elif self.script:
                return subprocess.run(self.script, shell=True, executable=self.executable, **pipe_args)

    @contextlib.contextmanager
    def done_first(self):
        with shell_script(script_class=self.__class__, no_execute=True) as script_segment:
            yield script_segment
            self.do_first(script_segment.script)


@contextlib.contextmanager
def shell_script(working_dir=None, executable=None, simulate=False, script_class=ShellScript, **script_options):
    """
    Context manager for the creation and execution of shell scripts.

    Example:

        with shell_script(working_dir="/some/working/dir") as script:
            script.do("echo hello from the shell")
            script.do("pwd")  # should print out /some/working/dir

    """
    # Note that we deliberately don't offer the 'no_execute' option in the argument list,
    # since it can be provided in **script_options in the few cases where it's needed,
    # and to not distract from 'the 'simulate' option which is probably what users usually want.  -kmp 2-Nov-2021
    script = script_class(executable=executable, simulate=simulate, **script_options)
    if working_dir:
        with script.using_working_dir(working_dir):
            yield script
    else:
        yield script
    script.execute()


@contextlib.contextmanager
def module_warnings_as_ordinary_output(module):
    """
    Allows warnings to be turned into regular output so they aren't so scary looking.

    Example:

        > s3Utils()
        WARNING:dcicutils.s3_utils:Fetching bucket data via global env bucket: cgap-whatever-main-foursight-envs
        WARNING:dcicutils.s3_utils:No env was specified, but cgap-something is the only one available, so using that.
        WARNING:dcicutils.s3_utils:health json url: http://cgap-something-blah-blah/health?format=json
        WARNING:dcicutils.s3_utils:Buckets resolved successfully.
        <dcicutils.s3_utils.s3Utils object at 0x10ca77048>
        > with module_warnings_as_ordinary_output(module='dcicutils.s3_utils'):
            s3Utils()
        Fetching bucket data via global env bucket: cgap-whatever-main-foursight-envs
        No env was specified, but cgap-something is the only one available, so using that.
        health json url: http://cgap-something-blah-blah/health?format=json
        Buckets resolved successfully.
        <dcicutils.s3_utils.s3Utils object at 0x10e518a90>

    """

    logger: logging.Logger = logging.getLogger(module)

    def just_print_text(record):
        PRINT(record.getMessage())
        return False

    try:
        logger.addFilter(just_print_text)
        yield

    finally:
        # This operation is safe even if the filter didn't get as far as being added.
        logger.removeFilter(just_print_text)


C4_PROJECTS = ["dbmi-bgm", "4dn-dcic"]


def guess_c4_git_url(c4_repo, all_projects=None):
    for project in all_projects or C4_PROJECTS:
        base_url = f"https://github.com/{project}/{c4_repo}"
        response = requests.get(base_url)
        if response.status_code == 200:
            return base_url + ".git"
    raise ValueError(f"Unknown C4 repository: {c4_repo}")


def setup_subrepo(subrepo_name, subrepo_git_url=None, parent_root=None,
                  branch='master', simulate=False, parent_repos_subdir='repositories',
                  all_projects=None):

    if parent_root is None:
        parent_root = os.path.abspath(os.curdir)
    if subrepo_git_url is None:
        subrepo_git_url = guess_c4_git_url(c4_repo=subrepo_name, all_projects=all_projects)

    all_subrepos_path = os.path.join(parent_root, parent_repos_subdir)
    subrepo_path = os.path.join(all_subrepos_path, subrepo_name)
    venv_name = subrepo_name + "_env"

    if not os.path.exists(all_subrepos_path):
        print(f"Creating {all_subrepos_path}...")
        os.mkdir(all_subrepos_path)

    if not os.path.exists(subrepo_path):
        with shell_script(working_dir=all_subrepos_path, simulate=simulate) as script:
            script.do(f'echo "Cloning {subrepo_name}..."')
            script.do(f'git clone {subrepo_git_url} {subrepo_name}')
            script.pushd(subrepo_name)
            script.do(f'git checkout {branch}')
            script.popd()
            script.do(f'echo "Done cloning {subrepo_path}."')
    else:

        with shell_script(working_dir=subrepo_path, simulate=simulate) as script:
            script.do(f'echo "Pulling latest changes for {subrepo_name} in branch {branch} ..."')
            script.do(f'git checkout {branch}')
            script.do(f'git pull')
            script.do(f'echo "Done pulling latest changes for {subrepo_name}."')

    with shell_script(working_dir=subrepo_path, simulate=simulate) as script:
        script_assure_venv(repo_path=subrepo_path, script=script, default_venv_name=venv_name)


def script_assure_venv(repo_path, script, default_venv_name, method='poetry'):

    venv_names = glob.glob(os.path.join(repo_path, "*env"))
    n_venv_names = len(venv_names)
    if n_venv_names == 0:
        venv_name = default_venv_name
        script.do(f'echo "Creating virtual environment {venv_name}."')
        script.do(f'pyenv exec python -m venv {venv_name} || python3 -m venv {venv_name}')
        script.do(f'source {venv_name}/bin/activate')
        script.do(f'echo "The installation process for {venv_name} may take about 3-5 minutes. Please be patient."')
        script.do(f'echo "Starting installation of {venv_name} requirements at $(date)."')
        if method == 'setup':
            script.do(f'echo "Using setup.py to install requirements for virtual environment {venv_name}."')
            script.do(f'python setup.py develop')
        elif method == 'poetry':
            script.do(f'echo "Using poetry to install requirements for virtual environment {venv_name}."')
            script.do(f'poetry install')
        elif method == 'requirements':
            script.do(f'echo "Using requirements to install requirements for virtual environment {venv_name}."')
            script.do(f'pip install -r requirements.txt')
        else:
            raise InvalidParameterError(parameter='method', value=method, options=['setup', 'requirements', 'poetry'])
        script.do(f'echo "Finished installation of {venv_name} requirements at $(date)."')
    elif n_venv_names == 1:
        [venv_name] = venv_names
        script.do(f'echo "Activating existing virtual environment {venv_name}."')
        script.do(f'source {venv_name}/bin/activate')
    else:
        raise RuntimeError(there_are(venv_names, kind="virtual environment"))
