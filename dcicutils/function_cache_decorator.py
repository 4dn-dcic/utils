from collections import namedtuple, OrderedDict
from datetime import datetime, timedelta
import timeit
from typing import Optional, Union
import json
import sys


# Global list of all @function_decorator instances; for debugging/testing/troubleshooting.
_function_cache_list = []


def function_cache(*decorator_args, **decorator_kwargs):
    """
    Exactly analogous to the functools.lru_cache decorator, but also allows specifying
    that if the return value of the function is None then no caching is to be done;
    use the nocache_none decorator kwarg set to True to do this. And more generally, use
    the nocache decorator kwart set to anything to not cache any value which equals that.

    Also like @lru_cache, this supports the maxsize decorator argument, as well
    as the cache_info and cache_clear functions on the decorated function.
    The maxsize can be specified either as the first argument to the
    decorator or as a maxsize kwarg to the decorator.

    In addition we support a ttl (time to live) decorator kwarg which can be specified,
    as a timedelta type. If the cached value for the function is more than the specified
    ttl then it is considered stale and the function will be called to get a fresh value.
    And, there is a separate ttl_none supported which will do the same as ttl but will
    apply only if the cached value is None.

    There is also a serialize_key decorator kwarg which if specified as True, will
    serialize the arguments (args and kwargs) to the function call and use that value,
    converted to a string, as the key for caching the function result; this will allow
    caching for functions which take non-hashable structured types (i.e. dict or list)
    as arguments, which normally would not be possible, i.e. e.g. in which case this
    error would be generated: TypeError: unhashable type: 'dict'

    And a custom key decorator kwarg may be specified as a lambda/callable which
    computes the key by which the function results should be cached; this lambda
    is passed the exact same arguments as the function itself.

    Lastly, if the nokey kwarg is specified for the decorator then no key at all
    will used (or more precisely, and single/constant key will be used) by which
    to cache the function result.

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

    null_object = object()
    maxsize = sys.maxsize
    ttl = None
    ttl_none = None
    nocache = null_object
    key = None
    serialize_key = False
    nokey = False

    if decorator_args:
        maxsize_arg = decorator_args[0]
        if isinstance(maxsize_arg, int) and maxsize_arg > 0:
            maxsize = maxsize_arg
    if decorator_kwargs:
        maxsize_kwarg = decorator_kwargs.get("maxsize")
        if isinstance(maxsize_kwarg, int) and maxsize_kwarg > 0:
            maxsize = maxsize_kwarg
        ttl_kwarg = decorator_kwargs.get("ttl")
        if isinstance(ttl_kwarg, timedelta):
            ttl = ttl_kwarg
        ttl_none_kwarg = decorator_kwargs.get("ttl_none")
        if isinstance(ttl_none_kwarg, timedelta):
            ttl_none = ttl_none_kwarg
        nocache_kwarg = decorator_kwargs.get("nocache", null_object)
        if nocache_kwarg is not null_object:
            nocache = nocache_kwarg
        nocache_none_kwarg = decorator_kwargs.get("nocache_none")
        if isinstance(nocache_none_kwarg, bool) and nocache_none_kwarg:
            nocache = None
        key_kwarg = decorator_kwargs.get("key")
        if callable(key_kwarg):
            key = key_kwarg
        serialize_key_kwarg = decorator_kwargs.get("serialize_key")
        if isinstance(serialize_key_kwarg, bool):
            serialize_key = serialize_key_kwarg
        nokey_kwarg = decorator_kwargs.get("nokey")
        if isinstance(nokey_kwarg, bool):
            nokey = nokey_kwarg

    def function_cache_decorator(wrapped_function):

        def function_wrapper(*args, **kwargs):

            if nokey:
                cache_key = 0
            else:
                cache_key = key(*args, **kwargs) if key else args + tuple(sorted(kwargs.items()))
                if serialize_key:
                    cache_key = json.dumps(cache_key, default=str, separators=(",", ":"))
            cached = cache.get(cache_key, None)

            now = None  # Do not call datetime.now more than once

            if cached is not None:

                if ttl or ttl_none:
                    now = datetime.now()

                def is_stale():
                    """
                    Returns True iff the cached value for the @function_cache decorated
                    function is stale, according to any ttl related decorator kwargs.
                    Uses outer variables: ttl, ttl_none, cached, now
                    """
                    if ttl and now > cached["timestamp"] + ttl:
                        return True
                    if ttl_none and cached["value"] is None and now > cached["timestamp"] + ttl_none:
                        return True
                    return False

                if not is_stale():
                    nonlocal nhits
                    nhits += 1
                    cache.move_to_end(cache_key)
                    return cached["value"]

            nonlocal nmisses
            nmisses += 1
            start_time = timeit.default_timer()
            value = wrapped_function(*args, **kwargs)
            duration = timeit.default_timer() - start_time

            if nocache is null_object or nocache != value:
                if len(cache) >= maxsize:
                    cache.popitem(last=False)
                if not now:
                    now = datetime.now()
                cache[cache_key] = {"value": value, "timestamp": now, "duration": duration}

            return value

        def cache_info(as_dict: bool = False) -> Union[namedtuple, dict]:
            """
            Returns a named tuple with sundry info about the function cache
            for the @function_cache decorated function.
            """
            cache_info = namedtuple("cache_info",
                                    ["hits", "misses", "size", "maxsize", "ttl", "ttl_none",
                                     "nocache_none", "nocache_other",
                                     "key", "serialize_key", "updated", "duration", "name"])
            if len(cache) > 0:
                cached = next(iter(cache.items()))[1]
                # This is the timstamp of the most recent call to the wrapped function.
                updated = cached["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
                # This is the duration in milliseconds of the most recent call to the wrapped function.
                duration = cached["duration"] * 1000
            else:
                updated = None
                duration = None
            info = cache_info(nhits, nmisses, len(cache), maxsize, ttl, ttl_none,
                              nocache is None, nocache is not None and nocache is not null_object,
                              key, serialize_key, updated, duration, _get_function_name(wrapped_function))
            return dict(info._asdict()) if as_dict else info

        def cache_clear() -> None:
            """
            Clears the cache for the @function_cache decorated function.
            """
            nonlocal nhits, nmisses
            nhits = nmisses = 0
            cache.clear()

        function_wrapper.cache_info = cache_info
        function_wrapper.cache_clear = cache_clear
        _function_cache_list.append({"function_wrapper": function_wrapper, "wrapped_function": wrapped_function})

        return function_wrapper

    if decorator_invoked_without_args:
        return function_cache_decorator(decorator_target_function)
    return function_cache_decorator


def _get_function_name(wrapped_function: callable) -> str:
    """
    Returns a unique name for the given function/callable.
    """
    return f"{wrapped_function.__module__}.{wrapped_function.__qualname__}"


def function_cache_info() -> dict:
    """
    Returns a list of dictionaries representing all of the function_cache instances
    which exist; each dictonary containing detailed info about the function cache.
    Only for debugging/testing/troubleshooting.
    """
    info = []
    for function_cache in _function_cache_list:
        function_wrapper = function_cache["function_wrapper"]
        wrapped_function = function_cache["wrapped_function"]
        cache_info = function_wrapper.cache_info(as_dict=True)
        cache_info["name"] = _get_function_name(wrapped_function)
        info.append(cache_info)
    return sorted(info, key=lambda item: item["name"])


def function_cache_clear(function_name: Optional[str] = None) -> int:
    """
    Clears the cache for the given named function (as per function_cache_info), OR for
    ALL existing caches if no named function given. Returns the number of caches cleared.
    Only for debugging/testing/troubleshooting.
    """
    ncleared = 0
    for function_cache in _function_cache_list:
        wrapped_function = function_cache["wrapped_function"]
        if not function_name or _get_function_name(wrapped_function) == function_name:
            function_wrapper = function_cache["function_wrapper"]
            function_wrapper.cache_clear()
            ncleared += 1
            if function_name:
                break
    return ncleared
