import glob
import io
import os
import re

from dcicutils.misc_utils import PRINT, remove_suffix
from dcicutils.lang_utils import there_are


_MY_DIR = os.path.dirname(__file__)

_ROOT_DIR = os.path.dirname(_MY_DIR)

_DCICUTILS_DIR = os.path.join(_ROOT_DIR, "dcicutils")

_DCICUTILS_DOC_FILE = os.path.join(_ROOT_DIR, "docs/source/dcicutils.rst")

_DCICUTILS_FILES = glob.glob(os.path.join(_DCICUTILS_DIR, "*.py"))

_SECTION_OR_SUBSECTION_LINE = re.compile(r"^([=]+|[-]+)$")

_SUBSUBSECTION_LINE = re.compile(r"^[\^]+$")

_AUTOMODULE_LINE = re.compile(r"^[.][.][ ]+automodule::[ ]+dcicutils[.](.*)$")


SKIP_MODULES = {'jh_utils', '__init__'}


def test_documentation():

    with io.open(_DCICUTILS_DOC_FILE) as fp:

        line_number = 0
        current_module = None
        automodules_seen = 0
        prev_line = None
        problems = []
        expected_modules = {remove_suffix(".py", os.path.basename(file)) for file in _DCICUTILS_FILES} - SKIP_MODULES
        documented_modules = set()
        for line in fp:
            line_number += 1  # We count the first line as line 1
            line = line.strip()
            if _SUBSUBSECTION_LINE.match(line):
                if current_module and automodules_seen == 0:
                    problems.append(f"Line {line_number}: Missing automodule declaration for section {current_module}.")
                current_module = prev_line
                automodules_seen = 0
            elif _SECTION_OR_SUBSECTION_LINE.match(line):
                current_module = None
                automodules_seen = 0
            else:
                matched = _AUTOMODULE_LINE.match(line)
                if matched:
                    automodule_module = matched.group(1)
                    if not current_module:
                        problems.append(f"Line {line_number}: Unexpected automodule declaration"
                                        f" outside of module section.")
                    else:
                        documented_modules.add(automodule_module)
                        if automodules_seen == 1:
                            # If fewer than 1 seen, no issue.
                            # If more than 1 seen, we already warned, so don't duplicate.
                            # So really only the n == 1 case matters to us.
                            problems.append(f"Line {line_number}: More than one automodule"
                                            f" in section {current_module}?")
                        if automodule_module != current_module:
                            problems.append(f"Line {line_number}: Unexpected automodule declaration"
                                            f" for section {current_module}: {automodule_module}.")
                    automodules_seen += 1
            prev_line = line
        undocumented_modules = expected_modules - documented_modules
        if undocumented_modules:
            problems.append(there_are(sorted(undocumented_modules), kind="undocumented module", punctuate=True))
        if problems:
            for n, problem in enumerate(problems, start=1):
                PRINT(f"PROBLEM {n}: {problem}")
            message = there_are(problems, kind="problem", tense='past', show=False,
                                context=f"found in the readthedocs declaration file, {_DCICUTILS_DOC_FILE!r}")
            raise AssertionError(message)
