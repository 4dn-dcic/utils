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
#
#
#
# @contextmanager
# def patch_context(
#     object_to_patch: object,
#     attribute_to_patch: str,
#     return_value: Optional[Any] = None,
#     **kwargs,
# ) -> Iterator[mock.MagicMock]:
#     with mock.patch.object(object_to_patch, attribute_to_patch, **kwargs) as mocked_item:
#         if return_value is not None:
#             mocked_item.return_value = return_value
#         yield mocked_item
