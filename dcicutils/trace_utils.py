import functools
import io

from dcicutils.misc_utils import environ_bool, full_object_name, get_error_message, PRINT
from dcicutils.obfuscation_utils import obfuscate_dict


# You can use TRACE_REDACT=FALSE if tracing should show credentials,
# but obviously this is usually not something you'd want to do, so this
# is on a separate variable that defaults to True. -kmp 30-Mar-2023

TRACE_REDACT = environ_bool("TRACE_REDACT", default=True)

def _expand_if_dict(d, indent=0):
    s = io.StringIO()
    if isinstance(d, dict):
        if not(d):
            PRINT("dict()", file=s)
        else:
            PRINT("dict(", file=s)
            for k, v in d.items():
                PRINT(f"{' ' * (indent + 2)}{k}={v!r},", file=s)
            PRINT(f"{' ' * indent})", file=s)
    else:
        PRINT(d)
    return s.getvalue()


def _obfuscate(x):
    return obfuscate_dict(x, obfuscated="<REDACTED>")


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
                args_for_display = args
                kwargs_for_display = kwargs
                if TRACE_REDACT:
                    args_for_display = tuple(map(_obfuscate, args_for_display))
                    kwargs_for_display = _obfuscate(kwargs)
                PRINT(f"Entering {trace_name} with"
                      f" args={args_for_display!r}"
                      f" kwargs={_expand_if_dict(kwargs_for_display)}")
                try:
                    res = fn(*args, **kwargs)
                    res_for_display = res
                    if TRACE_REDACT and isinstance(res_for_display, dict):
                        res_for_display = _obfuscate(res)
                    PRINT(f"Function {trace_name} returned {_expand_if_dict(res_for_display)}")
                    return res
                except BaseException as exc:
                    PRINT(f"Function {trace_name} raised {get_error_message(exc)}")
                    raise

            return _traced

        return _maybe_attach_trace

    return _trace_decorator


Trace = make_trace_decorator()
