from contextlib import contextmanager
from typing import Any, Iterator, Optional
from unittest import mock


@contextmanager
def patch_context(
    object_to_patch: object,
    attribute_to_patch: str,
    return_value: Optional[Any] = None,
    **kwargs,
) -> Iterator[mock.MagicMock]:
    with mock.patch.object(object_to_patch, attribute_to_patch, **kwargs) as mocked_item:
        if return_value is not None:
            mocked_item.return_value = return_value
        yield mocked_item
