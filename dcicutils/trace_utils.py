import functools
import io

from dcicutils.misc_utils import environ_bool, full_object_name, get_error_message, PRINT
from dcicutils.obfuscation_utils import obfuscate_dict


TRACE_REDACT = environ_bool("TRACE_REDACT", default=True)

def _expand_dict(d, indent=0):
    s = io.StringIO()
    if TRACE_REDACT:
        d = obfuscate_dict(d, obfuscated="<REDACTED>")
    if not d:
        PRINT("dict()", file=s)
    else:
        PRINT("dict(", file=s)
        for k, v in d.items():
            PRINT(f"{' ' * (indent + 2)}{k}={v!r},", file=s)
        PRINT(f"{' ' * indent})", file=s)
    return s.getvalue()


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
                PRINT(f"Entering {trace_name} with args={args!r} kwargs={_expand_dict(kwargs)}")
                try:
                    res = fn(*args, **kwargs)
                    PRINT(f"Function {trace_name} returned {res!r}")
                    return res
                except BaseException as exc:
                    PRINT(f"Function {trace_name} raised {get_error_message(exc)}")
                    raise

            return _traced

        return _maybe_attach_trace

    return _trace_decorator


Trace = make_trace_decorator()
