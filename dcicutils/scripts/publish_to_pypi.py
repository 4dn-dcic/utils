# Script to publish the Python package in the CURRENT git repo to PyPi.
# Does the following checks before allowing poetry to publish:
#
# 0. Current directory MUST be a git repo.
# 1. The git repo MUST NOT contain unstaged changes.
# 2. The git repo MUST NOT contain staged but uncommitted changes.
# 3. The git repo MUST NOT contain committed but unpushed changes.
# 4. The git repo package directories MUST NOT contain untracked files,
#    OR if they do contain untracked files then you must confirm this is OK.
# 5. The version being published must NOT have already been published.
#
# ASSUMES you have these credentials environment variables correctly set for PyPi publishing;
# although a --username and --password are also supported to set these via command-line.
#
# - PYPI_USER
# - PYPI_PASSWORD
#
# Prints a warning if the username is NOT "__token__" meaning a PyPi API token is being
# used, as using a simple username/password (as opposed to API token) is deprecated.
#
# Prompts for yes or no before publish is actually done. There is a --noconfirm
# option to skip this confimation, however it is only allowed when running in the
# context of GitHub actions - it checks for the GITHUB_ACTIONS environment variable.
#
# Prints warning if PY
#
# FYI: This was created late April 2023 after a junk file containing development
# logging output containing passwords was accidentally published to PyPi;
# item #4 above specifically addresses/prevents this. Perhaps better
# would be if publishing only happened via GitHub actions.

import argparse
import os
import requests
import subprocess
import toml

from typing import Tuple, Union


PYPI_BASE_URL = "https://pypi.org"
PYPI_API_TOKEN_USERNAME = "__token__"
DEBUG = False


def main() -> None:

    def is_github_actions_context():
        return "GITHUB_ACTIONS" in os.environ

    argp = argparse.ArgumentParser()
    argp.add_argument("--noconfirm", required=False, dest="noconfirm", action="store_true")
    argp.add_argument("--debug", required=False, dest="debug", action="store_true")
    argp.add_argument("--username", required=False, dest="username")
    argp.add_argument("--password", required=False, dest="password")
    argp.add_argument("--force-allow-username", required=False, action="store_true")
    args = argp.parse_args()

    if args.debug:
        global DEBUG
        DEBUG = True

    if args.noconfirm and not is_github_actions_context():
        ERROR_PRINT("The --noconfirm flag is only allowed within GitHub actions!")
        exit_with_no_action()

    if not verify_git_repo():
        exit_with_no_action()

    if not verify_unstaged_changes():
        exit_with_no_action()

    if not verify_uncommitted_changes():
        exit_with_no_action()

    if not verify_unpushed_changes():
        exit_with_no_action()

    if not verify_tagged():
        exit_with_no_action()

    if not verify_untracked_files():
        exit_with_no_action()

    package_name = get_package_name()
    package_version = get_package_version()

    if not verify_not_already_published(package_name, package_version):
        exit_with_no_action()

    if not args.noconfirm:
        if not answered_yes_to_confirmation(f"Do you want to publish {package_name} {package_version} to PyPi?"):
            exit_with_no_action()

    PRINT(f"Publishing {package_name} {package_version} to PyPi ...")

    if not publish_package(args.username, args.password, args.force_allow_username):
        exit_with_no_action()

    PRINT(f"Publishing {package_name} {package_version} to PyPi complete.")


def publish_package(pypi_username: str = None, pypi_password: str = None, force_allow_username: bool = False) -> bool:
    if not pypi_username:
        pypi_username = os.environ.get("PYPI_USER")
    if not pypi_password:
        pypi_password = os.environ.get("PYPI_PASSWORD")
    if not pypi_username or not pypi_password:
        ERROR_PRINT(f"No PyPi credentials; you should set the PYPI_USER and PYPI_PASSWORD environment variables.")
        return False
    if pypi_username != PYPI_API_TOKEN_USERNAME:
        if not force_allow_username:
            # Just in case someone really really needs this we will allow for now: --force-allow-username
            ERROR_PRINT(f"Publishing with username/pasword is no longer allowed; must use API token instead.")
            return False
        WARNING_PRINT(f"Publishing with username/pasword is NOT recommmended; use API token instead;"
                      f"only allowing because you said: --force-allow-username")
    poetry_publish_command = [
        "poetry", "publish",
        "--no-interaction", "--build",
        f"--username={pypi_username}", f"--password={pypi_password}"
    ]
    poetry_publish_results, status_code = execute_command(poetry_publish_command)
    PRINT("\n".join(poetry_publish_results))
    if status_code != 0:
        # TODO: Maybe retry once or twice (with prompt) if (perhaps spurious) failure.
        ERROR_PRINT(f"Publish to PyPi failed!")
        return False
    return True


def verify_git_repo() -> bool:
    """
    If this (the current directory) looks like a git repo then return True,
    otherwise prints an error message and returns False.
    """
    _, status = execute_command("git rev-parse --is-inside-work-tree")
    if status != 0:
        ERROR_PRINT("You are not in a git repo directory!")
        return False
    return True


def verify_unstaged_changes() -> bool:
    """
    If the current git repo has NO unstaged changes then returns True,
    otherwise prints an error message and returns False. HOWEVER, we DO
    allow unstaged changes to just the file gitinfo.json if such exists; to
    allow GitHub Actions to update with latest git (repo, branch, commit) info.
    """
    git_diff_results, _ = execute_command("git diff --name-only")
    if git_diff_results and not (len(git_diff_results) == 1 and
                                 os.path.basename(git_diff_results[0]) == "gitinfo.json"):
        ERROR_PRINT("You have changes to this branch that you have not staged for commit.")
        return False
    return True


def verify_uncommitted_changes() -> bool:
    """
    If the current git repo has no staged but uncommitted changes then returns True,
    otherwise prints an error message and returns False.
    """
    git_diff_staged_results, _ = execute_command("git diff --staged")
    if git_diff_staged_results:
        ERROR_PRINT("You have changes to this branch that you have staged but not committed.")
        return False
    return True


def verify_unpushed_changes() -> bool:
    """
    If the current git repo committed but unpushed changes then returns True,
    otherwise prints an error message and returns False.
    """
    git_uno_results, _ = execute_command("git status -uno", lines_containing="is ahead of")
    if git_uno_results:
        ERROR_PRINT("You have committed changes to this branch that you committed but not pushed.")
        return False
    return True


def verify_tagged() -> bool:
    """
    If the current git repo has a tag as its most recent commit then returns True,
    otherwise prints an error message and returns False.
    """
    git_most_recent_commit, _ = execute_command("git log -1 --decorate", lines_containing="tag:")
    if not git_most_recent_commit:
        ERROR_PRINT("You can only publish a tagged commit.")
        return False
    return True


def verify_untracked_files() -> bool:
    """
    If the current git repo has no untracked files then returns True,
    otherwise prints an error message, with the list of untracked files,
    and prompts the user for a yes/no confirmation on whether or to
    continue, and returns True for a yes response, otherwise returns False.
    """
    untracked_files = get_untracked_files()
    if untracked_files:
        PRINT(f"You are about to PUBLISH the following ({len(untracked_files)})"
              f" UNTRACKED file{'' if len(untracked_files) == 1 else 's' } -> SECURITY risk:")
        for untracked_file in untracked_files:
            PRINT(f"-- {untracked_file}")
        PRINT("DO NOT continue UNLESS you KNOW what you are doing!")
        if not answered_yes_to_confirmation("Do you really want to continue?"):
            return False
    return True


def verify_not_already_published(package_name: str, package_version: str) -> bool:
    """
    If the given package and version has not already been published to PyPi then returns True,
    otherwise prints an error message and returns False.
    """
    url = f"{PYPI_BASE_URL}/project/{package_name}/{package_version}/"
    DEBUG_PRINT(f"curl {url}")
    response = requests.get(url)
    if response.status_code == 200:
        ERROR_PRINT(f"Package {package_name} {package_version} has already been published to PyPi.")
        return False
    return True


def get_untracked_files() -> list:
    """
    Returns a list of untracked files for the current git repo; empty list if no untracked changes.
    We ignore __pycache__ directories for this which are already excluded by poetry publish.
    We ignore the .gitignore file for this.
    """
    package_directories = get_package_directories()
    untracked_files = []
    for package_directory in package_directories:
        # Note that the output of "git status -s --ignored" looks something like this:
        # ?? chalicelib_fourfront/some_untracked_file.py
        # !! chalicelib_fourfront/__pycache__/
        # Note that the --ignored option ignores the .gitignore file.
        git_status_results, _ = execute_command(f"git status -s --ignored {package_directory}")
        for git_status_result in git_status_results:
            if git_status_result and (git_status_result.startswith("??") or git_status_result.startswith("!!")):
                untracked_file = git_status_result[2:].strip()
                if untracked_file:
                    # Ignore any __pycache__ directories as they are already ignored by poetry publish.
                    if os.path.isdir(untracked_file) and os.path.basename(untracked_file.rstrip("/")) == "__pycache__":
                        continue
                    untracked_files.append(untracked_file)
    return untracked_files


def get_package_version() -> str:
    """
    Returns the tag name of the most recently created tag in the current git repo.
    """
    tag_commit, _ = execute_command("git rev-list --tags --max-count=1")
    tag_name, _ = execute_command(f"git  describe --tags  {tag_commit[0]}")
    package_version = tag_name[0]
    if package_version.startswith("v"):
        package_version = package_version[1:]
    return package_version


def get_package_name() -> str:
    """
    Returns the base name of the current git repo name.
    """
    # No, the package name should come from pyproject.toml not from the git repo name ...
    # package_name, _ = execute_command("git config --get remote.origin.url")
    # package_name = os.path.basename(package_name[0])
    # if package_name.endswith(".git"):
    #     package_name = package_name[:-4]
    # return package_name
    pyproject_toml = get_pyproject_toml()
    return pyproject_toml["tool"]["poetry"]["name"]


def get_package_directories() -> list:
    """
    Returns a list of directories constituting the Python package of the current repo,
    according to the pyproject.toml file.
    """
    package_directories = []
    pyproject_toml = get_pyproject_toml()
    pyproject_package_directories = pyproject_toml["tool"]["poetry"]["packages"]
    for pyproject_package_directory in pyproject_package_directories:
        package_directory = pyproject_package_directory.get("include")
        if package_directory:
            package_directory_from = pyproject_package_directory.get("from")
            if package_directory_from:
                package_directory = os.path.join(package_directory_from, package_directory)
            package_directories.append(package_directory)
    return package_directories


def get_pyproject_toml() -> list:
    with open("pyproject.toml", "r") as f:
        return toml.load(f)


def execute_command(command_argv: Union[list, str], lines_containing: str = None) -> Tuple[list, int]:
    """
    Executes the given command as a command-line subprocess, and returns a tuple whose first element
    is the list of lines from the output of the command, and the second element is the exit status code.
    """
    def cleanup_funny_output(output: str) -> str:
        return output.replace("('", "").replace("',)", "").replace("\\n\\n", "\n").replace("\\n", "\n")

    if isinstance(command_argv, str):
        command_argv = [arg for arg in command_argv.split(" ") if arg.strip()]
    DEBUG_PRINT(" ".join(command_argv))
    result = subprocess.run(command_argv, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    lines = result.stdout.decode("utf-8").split("\n")
    if lines_containing:
        lines = [line for line in lines if lines_containing in line]
    return [cleanup_funny_output(line.strip()) for line in lines if line.strip()], result.returncode


def answered_yes_to_confirmation(message: str) -> bool:
    """
    Prompts the user with the given message and asks for a yes or no answer,
    and if yes is the user response then returns True, otherwise returns False.
    """
    response = input(f"{message} [yes | no]: ").lower()
    if response == "yes":
        return True
    return False


def exit_with_no_action() -> None:
    """
    Exits this process immediately with status 1;
    first prints a message saying no action was taken.
    """
    PRINT("Exiting without taking action.")
    exit(1)


PRINT = print


def WARNING_PRINT(s):
    PRINT(f"WARNING: {s}")


def ERROR_PRINT(s):
    PRINT(f"ERROR: {s}")


def DEBUG_PRINT(s):
    if DEBUG:
        PRINT(f"DEBUG: {s}")


if __name__ == "__main__":
    main()
