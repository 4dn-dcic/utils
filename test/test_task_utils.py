import pytest

from dcicutils.exceptions import MultiError
from dcicutils.misc_utils import print_error_message
from dcicutils.task_utils import Task, TaskManager, map_chunks, pmap, pmap_list


def _add1(x):
    return x + 1


def _adder(x, y):
    return x + y


try:
    _add1(1, 'a')  # noQA - this is going to fail. we want to see what error it raises
except Exception as e:
    _BadArgument = type(e)
    assert _BadArgument != MultiError
    # In other words...
    assert isinstance(e, _BadArgument) and not isinstance(e, MultiError)


def test_Task():

    manager = TaskManager()

    task = Task(position=0, function=_add1, arg1=7, manager=manager)

    assert task.position == 0
    assert task.function == _add1
    assert task.arg1 == 7
    assert task.more_args == []


def test_TaskManager():

    manager = TaskManager()  # Tests creation

    assert manager.fail_fast
    assert manager.raise_error


def test_map_chunks_simple():

    for chunk_size in [2, 3]:
        i = 1
        n_chunks = 0
        for chunk in map_chunks(_add1, [1, 2, 3, 4, 5, 6], chunk_size=chunk_size):
            n_chunks += 1
            assert isinstance(chunk, list)
            assert len(chunk) == chunk_size
            for elem in chunk:
                i += 1
                assert elem == i
        assert n_chunks == 6 / chunk_size


def test_pmap_simple():

    print()
    res = pmap(_add1, [1, 2, 3])
    print(f"res={res}")
    assert not isinstance(res, list)
    assert next(res) == 2
    assert next(res) == 3
    assert next(res) == 4
    assert next(res, None) is None


def test_pmap_list_simple():

    assert pmap_list(_add1, [1, 2, 3]) == [2, 3, 4]

    assert pmap_list(_adder, [1, 2, 3], [4, 5, 6]) == [5, 7, 9]


def test_pmap_list_raise():

    with pytest.raises(_BadArgument):
        pmap_list(_add1, [1, 'a'])

    with pytest.raises(_BadArgument):
        pmap_list(_add1, ['a', 'b'], fail_fast=True)

    with pytest.raises(MultiError) as exc:
        pmap_list(_add1, ['a', 'b'], fail_fast=False)
    e0, e1 = exc.value.errors
    assert isinstance(e0, _BadArgument)  # same error as before
    assert isinstance(e1, _BadArgument)  # another of same
    assert e0 != e1


def test_pmap_list_no_raise():

    with pytest.raises(Exception) as exc:
        pmap_list(_add1, [], raise_error=False, fail_fast=True)
    assert str(exc.value) == "raise_erorr cannot be false if fail_fast is true."

    r1, r2 = pmap_list(_add1, [1, 'a'], raise_error=False, fail_fast=False)
    assert r1 == 2
    assert isinstance(r2, _BadArgument)

    r1, r2 = pmap_list(_add1, ['a', 'b'], raise_error=False, fail_fast=False)
    assert isinstance(r1, _BadArgument)
    assert isinstance(r2, _BadArgument)


def test_pmap_list_no_raise_chunked():

    with pytest.raises(Exception) as exc:
        pmap_list(_add1, [], raise_error=False, fail_fast=True)
    assert str(exc.value) == "raise_erorr cannot be false if fail_fast is true."

    # These should behave the same for raise_error=False, fail_fast=False
    for chunk_size in [2, 3]:
        r1a, r1b, r2a, r2b, r3a, r3b, r4a = pmap_list(_add1, [1, 2, 3, 'a', 'b', 6, 'c'],
                                                      raise_error=False, fail_fast=False, chunk_size=chunk_size)
        assert r1a == 2
        assert r1b == 3
        assert r2a == 4
        assert isinstance(r2b, _BadArgument)
        assert isinstance(r3a, _BadArgument)
        assert r3b == 7
        assert isinstance(r4a, _BadArgument)


def test_pmap_list_raise_fast_chunked():

    # These should behave the same for raise_error=True, fail_fast=True
    for chunk_size in [2, 3]:
        with pytest.raises(Exception) as exc:
            pmap_list(_add1, [1, 2, 3, 'a', 'b', 6, 'c'],
                      raise_error=True, fail_fast=True, chunk_size=chunk_size)
        error_object = exc.value
        assert isinstance(error_object, _BadArgument)


def test_pmap_list_raise_slow_chunked():

    # The behavior for raise_error=True, fail_fast=False will be different for chunk_size=2 and chunk_size=3
    # For chunk_size=2, there will be one error for 'a', so it will be raised as a regular error.
    with pytest.raises(Exception) as exc:
        pmap_list(_add1, [1, 2, 3, 'a', 'b', 6, 'c'],
                  raise_error=True, fail_fast=False, chunk_size=2)
    error_object = exc.value
    assert isinstance(error_object, _BadArgument)
    # For chunk_size=3, there will be two errors for 'a' and 'b', so they will be raised as a MultiError
    with pytest.raises(Exception) as exc:
        pmap_list(_add1, [1, 2, 3, 'a', 'b', 6, 'c'],
                  raise_error=True, fail_fast=False, chunk_size=3)
    error_object = exc.value
    assert isinstance(error_object, MultiError)
    assert all(isinstance(e, _BadArgument) for e in error_object.errors)


def test_pmap_list_fail_fast():

    with pytest.raises(_BadArgument) as exc:
        pmap_list(_add1, ['a', 'b'], fail_fast=True)  # just reports the first error it finds
    e = exc.value
    print_error_message(e)
    assert isinstance(e, _BadArgument)  # Note that we did not wait for a MultiError