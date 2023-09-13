import argparse

from dcicutils.command_utils import script_catch_errors, ScriptFailure
from dcicutils.lang_utils import there_are, conjoined_list
from dcicutils.license_utils import LicenseCheckerRegistry, LicenseChecker
from dcicutils.misc_utils import PRINT
from typing import Optional, Type


EPILOG = __doc__


ALL_CHECKER_NAMES = LicenseCheckerRegistry.all_checker_names()
NEWLINE = '\n'


def main():

    parser = argparse.ArgumentParser(
        description="Runs a license checker",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("name", type=str, default=None, nargs='?',
                        help=f"The name of a checker to run. "
                        + there_are(ALL_CHECKER_NAMES, kind='available checker',
                                    show=True, joiner=conjoined_list, punctuate=True))
    args = parser.parse_args()

    with script_catch_errors():
        run_license_checker(name=args.name)


def show_help_for_choosing_license_checker():
    PRINT("")
    PRINT(there_are(ALL_CHECKER_NAMES, kind='available checker', show=False, punctuation_mark=':'))
    PRINT("")
    wid = max(len(x) for x in ALL_CHECKER_NAMES) + 1
    for checker_name in ALL_CHECKER_NAMES:
        checker_class = LicenseCheckerRegistry.lookup_checker(checker_name)
        checker_doc = (checker_class.__doc__ or '<missing doc>').strip(' \t\n\r')
        PRINT(f"{(checker_name + ':').ljust(wid)} {checker_doc.split(NEWLINE)[0]}")
    PRINT("")
    PRINT("=" * 42, "NOTES & DISCLAIMERS", "=" * 42)
    PRINT("Park Lab is a research laboratory in the Department of Biomedical Informatics at Harvard Medical School.")
    PRINT("Park Lab checkers are intended for internal use and may not be suitable for other purposes.")
    PRINT("External organizations must make their own independent choices about license acceptability.")
    PRINT("Such choices can be integrated with this tool as follows:")
    PRINT(" * Import LicenseChecker and LicenseCheckerRegistry from dcicutils.license_utils.")
    PRINT(" * Make your own subclass of LicenseChecker, specifying a doc string and appropriate constraints.")
    PRINT(" * Decorate your subclass with an appropriate call to LicenseCheckerRegistry.register_checker.")
    PRINT("")


def run_license_checker(name: Optional[str]):
    if name is None:
        show_help_for_choosing_license_checker()
    else:
        try:
            checker_class: Type[LicenseChecker] = LicenseCheckerRegistry.lookup_checker(name)
        except Exception as e:
            raise ScriptFailure(str(e))
        checker_class.validate()
