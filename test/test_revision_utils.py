import pytest
from dcicutils.revision_utils import JsonDiff


DUMMY_COLLECTION_TYPE = 'TestItem'


@pytest.fixture
def simple_patch_body():
    return {
        'x': 'hello',
        'y': 'world'
    }


@pytest.fixture
def differring_simple_patch_body():
    return {
        'x': 'hell0',
        'y': 'world'
    }


def test_jsondiff_basic(simple_patch_body, differring_simple_patch_body):
    """ Tests basic aspects of JsonDiff creation """
    diff1 = JsonDiff(simple_patch_body, DUMMY_COLLECTION_TYPE)
    assert diff1.body == simple_patch_body
    assert diff1.item_type == DUMMY_COLLECTION_TYPE
    diff2 = JsonDiff(differring_simple_patch_body, DUMMY_COLLECTION_TYPE)
    assert diff1 != diff2
    diff3 = JsonDiff(differring_simple_patch_body, 'bad_type')
    assert diff3 != diff1
    diff4 = JsonDiff(simple_patch_body, 'bad_type')
    assert diff1 != diff4

def test_jsondiff_serialize_deserialize(simple_patch_body, differring_simple_patch_body):
    """ Tests serializing/deserializing JsonDiff objects """
    diff1 = JsonDiff(simple_patch_body, DUMMY_COLLECTION_TYPE)
    serialized = diff1.serialize()
    diff2 = JsonDiff.deserialize(serialized)
    assert diff1 == diff2
