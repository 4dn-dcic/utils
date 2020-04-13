"""
This file contains functions that might be generally useful.
"""

# Using PRINT(...) for debugging, rather than its more familiar lowercase form) for intended programmatic output,
# makes it easier to find stray print statements that were left behind in debugging. -kmp 30-Mar-2020

PRINT = print


def ignored(*args, **kwargs):
    """
    This is useful for defeating flake warnings.
    Call this function to use values that really should be ignored.
    This is intended as a declaration that variables are intentionally ignored,
    but no enforcement of that is done. Some sample uses:

    def foo(x, y):
        ignored(x, y)  # so flake8 won't complain about x and y being unused.
        return 3

    def map_action(action, data, options, precheck=ignored):
        precheck(data, **options)
        action(data, **options)
    """
    return args, kwargs
