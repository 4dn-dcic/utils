import pytest
import re
from dcicutils.qa_utils import mock_not_called, local_attrs


def test_mock_not_called():
    name = "foo"
    mocked_foo = mock_not_called(name)
    try:
        mocked_foo(1, 2, three=3)
    except AssertionError as e:
        m = re.match("%s.*called" % re.escape(name), str(e))
        assert m, "Expected assertion text did not appear."
    else:
        raise AssertionError("An AssertionError was not raised.")


def test_dynamic_properties():

    NORMAL_ATTR0 = 16
    NORMAL_ATTR1 = 17
    NORMAL_ATTR2 = 'foo'
    NORMAL_ATTR3 = 'bar'

    OVERRIDDEN_ATTR0 = 61
    OVERRIDDEN_ATTR1 = 71
    OVERRIDDEN_ATTR2 = 'oof'
    OVERRIDDEN_ATTR3 = 'rab'

    def test_thing(test_obj):

        assert test_obj.attr0 == NORMAL_ATTR0
        assert test_obj.attr1 == NORMAL_ATTR1
        assert test_obj.attr2 == NORMAL_ATTR2
        assert test_obj.attr3 == NORMAL_ATTR3

        attrs = ['attr0', 'attr1', 'attr2', 'attr3']

        # If this were done wrong, we'd bind an inherited attribute
        # and then when we put things back it would become an instance
        # attribute, so we remember what things were originally
        # instance attributes so that we can check later.
        old_attr_dict = test_obj.__dict__.copy()

        # Test of the ordinary case.
        with local_attrs(test_obj, attr0=OVERRIDDEN_ATTR0, attr2=OVERRIDDEN_ATTR2):
            assert test_obj.attr0 == OVERRIDDEN_ATTR0
            assert test_obj.attr1 == NORMAL_ATTR1
            assert test_obj.attr2 == OVERRIDDEN_ATTR2
            assert test_obj.attr3 == NORMAL_ATTR3
        assert test_obj.attr0 == NORMAL_ATTR0
        assert test_obj.attr1 == NORMAL_ATTR1
        assert test_obj.attr2 == NORMAL_ATTR2
        assert test_obj.attr3 == NORMAL_ATTR3

        assert test_obj.__dict__ == old_attr_dict

        # Another test of the ordinary case.
        with local_attrs(test_obj, attr0=OVERRIDDEN_ATTR0, attr1=OVERRIDDEN_ATTR1,
                         attr2=OVERRIDDEN_ATTR2, attr3=OVERRIDDEN_ATTR3):
            assert test_obj.attr0 == OVERRIDDEN_ATTR0
            assert test_obj.attr1 == OVERRIDDEN_ATTR1
            assert test_obj.attr2 == OVERRIDDEN_ATTR2
            assert test_obj.attr3 == OVERRIDDEN_ATTR3
        assert test_obj.attr0 == NORMAL_ATTR0
        assert test_obj.attr1 == NORMAL_ATTR1
        assert test_obj.attr2 == NORMAL_ATTR2
        assert test_obj.attr3 == NORMAL_ATTR3

        # Test case of raising an error and assuring things are still set to normal
        try:
            with local_attrs(test_obj, attr0=OVERRIDDEN_ATTR0, attr2=OVERRIDDEN_ATTR2):
                assert test_obj.attr0 == NORMAL_ATTR0
                assert test_obj.attr1 == NORMAL_ATTR1
                assert test_obj.attr2 == NORMAL_ATTR2
                assert test_obj.attr3 == NORMAL_ATTR3
                raise Exception("This is expected to be caught.")
        except Exception:
            pass
        assert test_obj.attr0 == NORMAL_ATTR0
        assert test_obj.attr1 == NORMAL_ATTR1
        assert test_obj.attr2 == NORMAL_ATTR2
        assert test_obj.attr3 == NORMAL_ATTR3

        # Test case of no attributes set at all
        with local_attrs(object):
            assert test_obj.attr0 == NORMAL_ATTR0
            assert test_obj.attr1 == NORMAL_ATTR1
            assert test_obj.attr2 == NORMAL_ATTR2
            assert test_obj.attr3 == NORMAL_ATTR3
        assert test_obj.attr0 == NORMAL_ATTR0
        assert test_obj.attr1 == NORMAL_ATTR1
        assert test_obj.attr2 == NORMAL_ATTR2
        assert test_obj.attr3 == NORMAL_ATTR3

    class Foo:
        attr0 = NORMAL_ATTR0
        attr1 = NORMAL_ATTR1
        def __init__(self):
            self.attr2 = NORMAL_ATTR2
            self.attr3 = NORMAL_ATTR3

    with pytest.raises(ValueError):
        # Binding attr1 would affect other instances.
        test_thing(Foo())

    class Bar:
        def __init__(self):
            self.attr0 = NORMAL_ATTR0
            self.attr1 = NORMAL_ATTR1
            self.attr2 = NORMAL_ATTR2
            self.attr3 = NORMAL_ATTR3

    test_thing(Bar())

    class Baz:
        attr0 = NORMAL_ATTR0
        attr1 = NORMAL_ATTR1
        attr2 = NORMAL_ATTR2
        attr3 = NORMAL_ATTR3

    test_thing(Baz)

    with pytest.raises(ValueError):
        # Binding attr1 would affect other instances.
        test_thing(Baz())

    for thing in [3, "foo", None]:
        with local_attrs(thing):
            pass  # Just make sure no error occurs when no attributes given

