import pytest
import time

from dcicutils.exceptions import MultiError
from dcicutils.misc_utils import print_error_message
from dcicutils.qa_utils import Timer
from dcicutils.task_utils import Task, TaskManager, pmap_chunks, pmap, pmap_list


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

    some_call_id = 99999
    task = Task(position=0, function=_add1, arg1=7, manager=manager, call_id=some_call_id)
    print(task)

    assert task.call_id == some_call_id
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
        for chunk in pmap_chunks(_add1, [1, 2, 3, 4, 5, 6], chunk_size=chunk_size):
            n_chunks += 1
            assert isinstance(chunk, list)
            assert len(chunk) == chunk_size
            for elem in chunk:
                i += 1
                assert elem == i
        assert n_chunks == 6 / chunk_size


def test_pmap_simple():

    print()  # start on a fresh line
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

    print()  # start on a fresh line
    with pytest.raises(_BadArgument) as exc:
        pmap_list(_add1, ['a', 'b'], fail_fast=True)  # just reports the first error it finds
    e = exc.value
    print_error_message(e)
    assert isinstance(e, _BadArgument)  # Note that we did not wait for a MultiError


def test_pmap_parallelism():

    # For example, a sample test run with
    #    pytest -s -vv -k test_pmap_parallelism
    # showed:
    # Total seconds (serial): 1.180179, average 0.012 sec/call
    # Total seconds ( 2 at a time): 0.609771, expected range 0.531 < t < 0.885
    # Total seconds ( 5 at a time): 0.252411, expected range 0.212 < t < 0.354
    # Total seconds (10 at a time): 0.131777, expected range 0.106 < t < 0.177
    # Total seconds (20 at a time): 0.072549, expected range 0.053 < t < 0.089

    print()  # start on a fresh line

    slowness = 0.01  # With parallelism, hard to use ControlledTime so make sure slowness isn't VERY slow. :)

    def slow_add1(x):
        time.sleep(slowness)
        return x + 1

    n_tries = 100

    def the_input():
        return range(n_tries)

    def expected_output():
        return range(1, n_tries + 1)

    with Timer() as timer:
        assert list(map(slow_add1, the_input())) == list(expected_output())
    n_secs = timer.duration_seconds()
    measured_slowness = n_secs / n_tries
    print(f"Total seconds (serial): {n_secs}, average {measured_slowness:.3f} sec/call")
    assert 0.9 < n_secs  # allow for floating roundoff error, though really we expect n_secs > 1

    for chunk_size in [2, 5, 10, 20]:

        with Timer() as timer:
            assert list(pmap(slow_add1, the_input(), chunk_size=chunk_size)) == list(expected_output())
        n_secs = timer.duration_seconds()
        n_chunks = n_tries / chunk_size
        expected = measured_slowness * n_chunks
        # Allow for float round-off error on low side and additional computational overhead in the loop on high side
        expected_lo = expected * 0.9
        expected_hi = expected * 1.5
        print(f"Total seconds ({chunk_size:2d} at a time): {n_secs},"
              f" expected range {expected_lo:.3f} < t < {expected_hi:.3f}")
        assert expected_lo < n_secs < expected_hi
