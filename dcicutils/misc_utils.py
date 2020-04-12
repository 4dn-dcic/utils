"""
This file contains functions that might be generally useful.
"""

# Using PRINT(...) for debugging, rather than its more familiar lowercase form) for intended programmatic output,
# makes it easier to find stray print statements that were left behind in debugging. -kmp 30-Mar-2020

PRINT = print


def ignored(*args):
    """
    This is useful for defeating flake warnings.
    Call this function to use values that really should be ignored.

    def foo(x, y):
        ignored(x, y)
    """
    return args
