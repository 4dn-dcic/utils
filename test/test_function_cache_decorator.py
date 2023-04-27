from datetime import timedelta
import time
from dcicutils.function_cache_decorator import function_cache


def test_function_cache_decorator():

    called = 0

    @function_cache
    def f(n):
        nonlocal called
        called += 1
        if n < 0:
            return None
        return n * n

    assert f(3) == 9   # miss
    assert f.cache_info()["size"] == 1
    assert f(4) == 16  # miss
    assert f.cache_info()["size"] == 2
    assert f(5) == 25  # miss
    assert f.cache_info()["size"] == 3
    assert f(3) == 9   # hit
    assert f.cache_info()["size"] == 3
    assert f(4) == 16  # hit
    assert f.cache_info()["size"] == 3
    assert f(5) == 25  # hit
    assert f.cache_info()["size"] == 3
    assert f(3) == 9   # hit
    assert f.cache_info()["size"] == 3
    assert f(4) == 16  # hit
    assert f.cache_info()["size"] == 3
    assert f(5) == 25  # hit
    assert f.cache_info()["size"] == 3
    assert called == 3
    assert f.cache_info()["hits"] == 6
    assert f.cache_info()["misses"] == 3
    assert f.cache_info()["size"] == 3

    assert f(-1) is None  # miss
    assert f.cache_info()["size"] == 4
    assert f(-1) is None  # hit
    assert f.cache_info()["size"] == 4
    assert f(-1) is None  # hit
    assert f.cache_info()["hits"] == 8
    assert f.cache_info()["misses"] == 4
    assert f.cache_info()["size"] == 4
    assert called == 4


def test_function_cache_decorator_with_do_not_cache_none():

    called = 0

    @function_cache(do_not_cache_none=True)
    def f(n):
        nonlocal called
        called += 1
        if n < 0:
            return None
        return n * n

    assert f.cache_info()["size"] == 0
    assert f(3) == 9   # miss
    assert f.cache_info()["size"] == 1
    assert f(4) == 16  # miss
    assert f.cache_info()["size"] == 2
    assert f(5) == 25  # miss
    assert f.cache_info()["size"] == 3
    assert f(3) == 9   # hit
    assert f.cache_info()["size"] == 3
    assert f(4) == 16  # hit
    assert f.cache_info()["size"] == 3
    assert f(5) == 25  # hit
    assert f.cache_info()["size"] == 3
    assert f(3) == 9   # hit
    assert f.cache_info()["size"] == 3
    assert f(4) == 16  # hit
    assert f.cache_info()["size"] == 3
    assert f(5) == 25  # hit
    assert f.cache_info()["size"] == 3
    assert called == 3
    assert f.cache_info()["hits"] == 6
    assert f.cache_info()["misses"] == 3

    called = 0
    f.cache_clear()
    assert f.cache_info()["size"] == 0
    assert f(-1) is None  # miss (because do_not_cache_none=True)
    assert f.cache_info()["size"] == 0
    assert f(-1) is None  # miss (because do_not_cache_none=True)
    assert f.cache_info()["size"] == 0
    assert f(-1) is None  # miss (because do_not_cache_none=True)
    assert f.cache_info()["size"] == 0
    assert called == 3
    assert f.cache_info()["hits"] == 0
    assert f.cache_info()["misses"] == 3


def test_function_cache_decorator_with_maxsize():

    called = 0

    @function_cache(maxsize=2)  # only caching 2 - keeps most recently used
    def f(n):
        nonlocal called
        called += 1
        if n < 0:
            return None
        return n * n

    assert f.cache_info()["maxsize"] == 2
    assert f.cache_info()["size"] == 0
    assert f(3) == 9   # miss
    assert f.cache_info()["size"] == 1
    assert f(4) == 16  # miss
    assert f.cache_info()["size"] == 2
    assert f(5) == 25  # miss
    assert f.cache_info()["size"] == 2
    assert f(4) == 16  # hit
    assert f(5) == 25  # hit
    assert f(4) == 16  # hit (one of 2 most recently used)
    assert f(5) == 25  # hit (one of 2 most recently used)
    assert f(3) == 9   # miss (not one of 2 most recently used)
    assert f(3) == 9   # hit
    assert f(5) == 25  # hit
    assert called == 4
    assert f.cache_info()["hits"] == 6
    assert f.cache_info()["misses"] == 4
    assert f.cache_info()["size"] == 2


def test_function_cache_decorator_with_ttl():

    called = 0

    @function_cache(ttl=timedelta(milliseconds=500))
    def f(n):
        nonlocal called
        called += 1
        if n < 0:
            return None
        return n * n

    assert f(3) == 9  # miss
    assert f(3) == 9  # hit
    assert f(3) == 9  # hit
    assert called == 1
    assert f.cache_info()["hits"] == 2
    assert f.cache_info()["misses"] == 1

    time.sleep(1)
    assert f(3) == 9  # miss (due to ttl)
    assert f(3) == 9  # hit
    assert called == 2
    assert f.cache_info()["hits"] == 3
    assert f.cache_info()["misses"] == 2


def test_function_cache_decorator_with_ttl_none():

    called = 0

    @function_cache(ttl_none=timedelta(milliseconds=500))
    def f(n):
        nonlocal called
        called += 1
        if n < 0:
            return None
        return n * n

    assert f(-1) is None  # miss
    assert f(-1) is None  # hit
    assert f(-1) is None  # hit
    assert called == 1
    assert f.cache_info()["hits"] == 2
    assert f.cache_info()["misses"] == 1

    time.sleep(1)
    assert f(-1) is None  # miss (due to ttl)
    assert f(-1) is None  # hit
    assert called == 2
    assert f.cache_info()["hits"] == 3
    assert f.cache_info()["misses"] == 2


def test_function_cache_decorator_structured_types():

    called = 0

    @function_cache(serialize_key=True)
    def f(d):
        nonlocal called
        called += 1
        return dict

    f({"abc":123})  # miss
    assert f.cache_info()["size"] == 1
    f({"abc":123})  # hit
    assert f.cache_info()["size"] == 1
    f({"abc":123})  # hit
    assert called == 1
    assert f.cache_info()["hits"] == 2
    assert f.cache_info()["misses"] == 1
    assert f.cache_info()["size"] == 1
