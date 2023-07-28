import argparse

from dcicutils.command_utils import script_catch_errors, ScriptFailure
from .contribution_utils import Contributions, PROJECT_HOME


EPILOG = __doc__


def show_contributors(repo, exclude_fork=None, verbose=False, save_contributors=False, test=False):
    contributions = Contributions(repo=repo, exclude_fork=exclude_fork, verbose=verbose)
    if save_contributors:
        contributions.save_contributor_data()
    contributions.show_repo_contributors(error_class=ScriptFailure if test else None)


def show_contributors_main(*, simulated_args=None):
    parser = argparse.ArgumentParser(  # noqa - PyCharm wrongly thinks the formatter_class is specified wrong here.
        description=(f"Show authors of a specified repository, which will be presumed"
                     f" to have been cloned as a subdirectory of $PROJECT_HOME ({PROJECT_HOME})"),
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('repo', default=None,
                        help="name of repository to show contributors for")
    parser.add_argument('--exclude', '-x', default=None,
                        help="name of repository that repo was forked from, whose contributors to exclude")
    parser.add_argument('--save-contributors', '-s', action="store_true", default=False,
                        help="whether to store contributor data to CONTRIBUTORS.json")
    parser.add_argument('--test', '-t', action="store_true", default=False,
                        help="whether to treat this as a test, erring if a cache update is needed")
    parser.add_argument('--verbose', '-v', action="store_true", default=False,
                        help="whether to do verbose output while working")
    args = parser.parse_args(args=simulated_args)

    with script_catch_errors():

        show_contributors(repo=args.repo, exclude_fork=args.exclude, verbose=args.verbose,
                          save_contributors=args.save_contributors, test=args.test)
