import functools

from dcicutils.misc_utils import environ_bool, full_object_name, get_error_message, PRINT
from dcicutils.obfuscation_utils import obfuscate_dict


# You can use TRACE_REDACT=FALSE if tracing should show credentials,
# but obviously this is usually not something you'd want to do, so this
# is on a separate variable that defaults to True. -kmp 30-Mar-2023

TRACE_REDACT = environ_bool("TRACE_REDACT", default=True)


def _maybe_obfuscate(x):
    if not TRACE_REDACT:
        return x
    elif isinstance(x, dict):
        return obfuscate_dict(x, obfuscated="<REDACTED>")
    elif isinstance(x, tuple):
        return tuple(map(_maybe_obfuscate, x))
    elif isinstance(x, list):
        return list(map(_maybe_obfuscate, x))
    else:
        return x


def make_trace_decorator(enabled_by_default=False):

    def _trace_decorator(enabled=None):
        if enabled is None:
            enabled = enabled_by_default or environ_bool('TRACE_ENABLED')

        def _maybe_attach_trace(fn):
            if not enabled:
                return fn
            trace_name = full_object_name(fn)

            @functools.wraps(fn)
            def _traced(*args, **kwargs):
                PRINT(f"Entering {trace_name} with args={_maybe_obfuscate(args)!r} kwargs={_maybe_obfuscate(kwargs)!r}")
                try:
                    res = fn(*args, **kwargs)
                    PRINT(f"Function {trace_name} returned {_maybe_obfuscate(res)!r}")
                    return res
                except BaseException as exc:
                    PRINT(f"Function {trace_name} raised {get_error_message(exc)}")
                    raise

            return _traced

        return _maybe_attach_trace

    return _trace_decorator


Trace = make_trace_decorator()
