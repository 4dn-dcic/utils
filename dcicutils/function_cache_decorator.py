from collections import OrderedDict
from datetime import datetime, timedelta
import sys


def function_cache(*decorator_args, **decorator_kwargs):
    """
    Exactly analogous to the functools.lru_cache decorator, but also allows
    specifying that if the return value of the function is None then no caching
    is to be done; use the do_not_cache_none=True as a decorator argument to do this.

    Also like @lru_cache, this supports the maxsize decorator argument, as well
    as the cache_info and cache_clear functions on the decorated function.
    The maxsize can be specified either as the first argument to the
    decorator or as a maxsize kwarg to the decorator.

    In addition we support a time_to_live (ttl) decorator kwarg which can be specified,
    as a timedelta type. If the cached value for the function is more than the specified
    ttl then it is considered stale and the function will be called to get a fresh value.
    And, there is a separate time_to_live_none (ttl_none) supported which will do the
    same as ttl but will apply only if the cached value is None.

    Looked/tried and could not find an way to do this using @lru_cache;
    and also had issues trying to wrap @lru_cache with this functionality.
    First created (April 2023) to try simplify some of the caching in foursight-core APIs.
    """
    cache = OrderedDict()
    nhits = nmisses = 0

    if len(decorator_args) == 1 and callable(decorator_args[0]) and decorator_args[0].__name__ != "<lambda>":
        decorator_invoked_without_args = True
        decorator_target_function = decorator_args[0]
        decorator_args = decorator_args[1:]
    else:
        decorator_invoked_without_args = False
        decorator_target_function = None

    maxsize = sys.maxsize
    nocache_none = False
    ttl = None
    ttl_none = None
    null_object = object()

    if decorator_args:
        maxsize_arg = decorator_args[0]
        if isinstance(maxsize_arg, int) and maxsize_arg > 0:
            maxsize = maxsize_arg
    if decorator_kwargs:
        maxsize_kwarg = decorator_kwargs.get("maxsize")
        if isinstance(maxsize_kwarg, int) and maxsize_kwarg > 0:
            maxsize = maxsize_kwarg
        nocache_none_kwarg = decorator_kwargs.get("do_not_cache_none", decorator_kwargs.get("nocache_none"))
        if isinstance(nocache_none_kwarg, bool):
            nocache_none = nocache_none_kwarg
        ttl_kwarg = decorator_kwargs.get("time_to_live", decorator_kwargs.get("ttl"))
        if isinstance(ttl_kwarg, timedelta):
            ttl = ttl_kwarg
        ttl_none_kwarg = decorator_kwargs.get("time_to_live_none", decorator_kwargs.get("ttl_none"))
        if isinstance(ttl_none_kwarg, timedelta):
            ttl_none = ttl_none_kwarg

    def function_cache_decorator_registration(wrapped_function):

        def wrapper_function(*args, **kwargs):

            key = args + tuple(sorted(kwargs.items()))
            cached = cache.get(key, null_object)
            now = None

            if cached is not null_object:

                if ttl or ttl_none:
                    now = datetime.now()

                def is_stale():
                    if ttl and now > cached["timestamp"] + ttl:
                        return True
                    if ttl_none and cached["value"] is None and now > cached["timestamp"] + ttl_none:
                        return True
                    return False

                if not is_stale():
                    nonlocal nhits
                    nhits += 1
                    cache.move_to_end(key)
                    return cached["value"]

            nonlocal nmisses
            nmisses += 1
            value = wrapped_function(*args, **kwargs)

            if value is not None or not nocache_none:
                if len(cache) >= maxsize:
                    cache.popitem(last=False)
                if not now:
                    now = datetime.now()
                cache[key] = {"value": value, "timestamp": now}

            return value

        def cache_info():
            info = {
                "hits": nhits,
                "misses": nmisses,
                "size": len(cache)
            }
            if maxsize != sys.maxsize:
                info["maxsize"] = maxsize
            if ttl:
                info["ttl"] = ttl
            if ttl_none:
                info["ttl_none"] = ttl_none
            return info

        def cache_clear():
            nonlocal nhits, nmisses
            nhits = nmisses = 0
            cache.clear()

        wrapper_function.cache_info = cache_info
        wrapper_function.cache_clear = cache_clear
        return wrapper_function

    if decorator_invoked_without_args:
        return function_cache_decorator_registration(decorator_target_function)
    return function_cache_decorator_registration
