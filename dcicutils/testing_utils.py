from contextlib import contextmanager
from types import ModuleType
from typing import Any, Iterator, Optional
from unittest import mock


AN_UNLIKELY_RETURN_VALUE = "unlikely return value"


@contextmanager
def patch_context(
    to_patch: object,
    return_value: Any = AN_UNLIKELY_RETURN_VALUE,
    module: Optional[ModuleType] = None,
    **kwargs,
) -> Iterator[mock.MagicMock]:
    """Mock out the given object.

    Essentially mock.patch_object with some hacks to enable linting
    on the object to patch instead of providing as a string.

    Depending on import structure, adding the module to patch may be
    required.
    """
    if isinstance(to_patch, property):
        to_patch = to_patch.fget
        new_callable = mock.PropertyMock
    else:
        new_callable = mock.MagicMock
    if module is None:
        target = f"{to_patch.__module__}.{to_patch.__qualname__}"
    else:
        target = f"{module.__name__}.{to_patch.__qualname__}"
    with mock.patch(target, new_callable=new_callable, **kwargs) as mocked_item:
        if return_value != AN_UNLIKELY_RETURN_VALUE:
            mocked_item.return_value = return_value
        yield mocked_item
