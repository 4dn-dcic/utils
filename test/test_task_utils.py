import pytest

from dcicutils.exceptions import MultiError
from dcicutils.task_utils import (
    TaskManager, pmap,
    _Task,  # noQA - testing protected member
)


def _add1(x):
    return x + 1


def _adder(x, y):
    return x + y


def test_pmapper():

    manager = TaskManager()  # Tests creation

    assert manager.fail_fast
    assert manager.raise_error


def test_task():

    manager = TaskManager()

    task = _Task(position=0, function=_add1, arg1=7, manager=manager)

    assert task.position == 0
    assert task.function == _add1
    assert task.arg1 == 7
    assert task.more_args == []


def test_pmap():

    with pytest.raises(Exception):
        pmap(_add1)  # noQA - testing that the list of sequences is required.

    assert pmap(_add1, [1, 2, 3]) == [2, 3, 4]

    with pytest.raises(Exception):
        pmap(_adder, [1, 2, 3], [1, 2])  # noQA - testing arg length mismatch

    assert pmap(_adder, [1, 2, 3], [4, 5, 6]) == [5, 7, 9]


try:
    _add1(1, 'a')  # noQA - this is going to fail. we want to see what error it raises
except Exception as e:
    wrong_type_arg = type(e)
    assert wrong_type_arg != MultiError
    # In other words...
    assert isinstance(e, wrong_type_arg) and not isinstance(e, MultiError)


def test_pmap_raise():

    with pytest.raises(wrong_type_arg):
        pmap(_add1, [1, 'a'])

    with pytest.raises(wrong_type_arg):
        pmap(_add1, ['a', 'b'], fail_fast=True)

    with pytest.raises(MultiError) as exc:
        pmap(_add1, ['a', 'b'], fail_fast=False)
    e0, e1 = exc.value.errors
    assert isinstance(e0, wrong_type_arg)  # same error as before
    assert isinstance(e1, wrong_type_arg)  # another of same
    assert e0 != e1


def test_pmap_no_raise():

    with pytest.raises(Exception) as exc:
        pmap(_add1, [], raise_error=False, fail_fast=True)
    assert str(exc.value) == "raise_erorr cannot be false if fail_fast is true."

    r1, r2 = pmap(_add1, [1, 'a'], raise_error=False, fail_fast=False)
    assert r1 == 2
    assert isinstance(r2, wrong_type_arg)

    r1, r2 = pmap(_add1, ['a', 'b'], raise_error=False, fail_fast=False)
    assert isinstance(r1, wrong_type_arg)
    assert isinstance(r2, wrong_type_arg)


def test_pmap_fail_fast():

    with pytest.raises(wrong_type_arg):
        pmap(_add1, ['a', 'b'], fail_fast=True)  # just reports the first error it finds
