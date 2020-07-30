import pytest

from dcicutils.diff_utils import DiffManager


def test_diffmanager_unknown_style():

    err = DiffManager.UnknownStyle('foo')

    assert isinstance(err, Exception)
    assert isinstance(err, DiffManager.UnknownStyle)

    assert str(err) == "foo is not a known style."


def test_merge_label_key():

    jdm = DiffManager('javascript')

    assert jdm._merge_label_key(None, 'foo') == 'foo'
    assert jdm._merge_label_key('foo', 'bar') == 'foo.bar'

    pdm = DiffManager('python')

    assert pdm._merge_label_key(None, 'foo') == 'foo'
    assert pdm._merge_label_key('foo', 'bar') == 'foo["bar"]'

    ldm = DiffManager('list')

    assert ldm._merge_label_key(None, 'foo') == ('foo',)
    assert ldm._merge_label_key(('foo',), 'bar') == ('foo', 'bar')

    fdm = DiffManager('foo')

    # TODO: Probably better to raise an error here, but this is an internal function
    #       and this isn't likely to happen. This is here for code coverage to know what does happen.
    assert fdm._merge_label_key(None, 'bar') == 'bar'

    with pytest.raises(DiffManager.UnknownStyle):
        fdm._merge_label_key('foo', 'bar')


def test_unroll():

    dm = DiffManager(label="item")

    assert dm.unroll(3) == {
        "item": 3,
    }

    assert dm.unroll("foo") == {
        "item": "foo",
    }

    assert dm.unroll(None) == {
        "item": None,
    }

    assert dm.unroll(True) == {
        "item": True,
    }

    assert dm.unroll(False) == {
        "item": False,
    }

    assert dm.unroll({"a": 3, "b": 4}) == {
        "item.a": 3,
        "item.b": 4,
    }

    assert dm.unroll([100, 200, 300]) == {
        "item[0]": 100,
        "item[1]": 200,
        "item[2]": 300,
    }

    assert dm.unroll({
        "a": {
            "b": 1,
            "c": 2
        },
        "b": {
            "c": 3,
            "d": 4
        }
    }) == {
        "item.a.b": 1,
        "item.a.c": 2,
        "item.b.c": 3,
        "item.b.d": 4,
    }

    assert dm.unroll({
        "map": [["a", "alpha"], ["b", "beta"]]
    }) == {
        "item.map[0][0]": "a",
        "item.map[0][1]": "alpha",
        "item.map[1][0]": "b",
        "item.map[1][1]": "beta",
    }

    assert dm.unroll({
        "map": [{"a": "alpha"}, {"b": "beta"}]
    }) == {
        "item.map[0].a": "alpha",
        "item.map[1].b": "beta",
    }


def test_diffs():

    dm = DiffManager(label="item")

    assert dm.diffs(1, 1) == {"same": ["item"]}

    assert dm.diffs("foo", "bar") == {"changed": ["item"]}

    assert dm.diffs({"a": "foo", "b": "bar"}, {"a": "foo", "b": "baz"}) == {
        "changed": ["item.b"],
        "same": ["item.a"],
    }


def test_comparison():

    dm = DiffManager(label="item")

    assert dm.comparison(1, 1) == []

    assert dm.comparison("foo", "bar") == [
        'item : "foo" => "bar"'
    ]

    assert dm.comparison(
        {"a": "foo", "b": "bar"},
        {"a": "foo", "b": "baz"}
    ) == [
        'item.b : "bar" => "baz"'
    ]

    assert dm.comparison(
        {"a": "foo", "b": "bar"},
        {"a": "foo", "b": "baz"}
    ) == [
        'item.b : "bar" => "baz"'
    ]

    assert dm.comparison(
        {"a": "foo", "b": "bar"},
        {"a": "foo", "b": "baz"}
    ) == [
        'item.b : "bar" => "baz"'
    ]


def test_comparison_python():

    dm = DiffManager(style='python', label="item")

    assert dm.comparison(1, 1) == []

    assert dm.comparison("foo", "bar") == [
        'item : "foo" => "bar"'
    ]

    assert dm.comparison(
        {"a": "foo", "b": "bar"},
        {"a": "foo", "b": "baz"}
    ) == [
        'item["b"] : "bar" => "baz"'
    ]

    assert dm.comparison(
        {"a": "foo", "b": "bar"},
        {"a": "foo", "b": "baz"}
    ) == [
        'item["b"] : "bar" => "baz"'
    ]

    assert dm.comparison(
        {"a": "foo", "b": "bar"},
        {"a": "foo", "b": "baz"}
    ) == [
        'item["b"] : "bar" => "baz"'
    ]

    assert dm.comparison(
        {"a": "foo", "b": "bar"},
        {"a": "foo", "b": "baz"}
    ) == [
        'item["b"] : "bar" => "baz"'
    ]


def test_maybe_sorted():

    dm = DiffManager(sort_by_change_type=True)

    assert dm._maybe_sorted(['b', 'a'], for_change_type=True) == ['a', 'b']
    assert dm._maybe_sorted(['b', 'a'], for_change_type=False) == ['b', 'a']

    dm = DiffManager(sort_by_change_type=False)

    assert dm._maybe_sorted(['b', 'a'], for_change_type=True) == ['b', 'a']
    assert dm._maybe_sorted(['b', 'a'], for_change_type=False) == ['a', 'b']


# Not supporting this case for now.
#
# def test_patch_diffs():
#
#     dm = DiffManager(style='list')
#
#     assert dm.patch_diffs({
#         'a': {
#             'b': 3,
#             'c': ['a', 'b']
#         }
#     }) == [
#         ('a', 'b'),
#         ('a', 'c', 0),
#         ('a', 'c', 1),
#     ]

def test_patch_diffs_with_omitted_subscripts_list_style():

    dm = DiffManager(style='list')

    assert dm.patch_diffs({
        'a': {
            'b': 3,
            'c': [{'alpha': 11, 'beta': 22}, {'alpha': 33, 'gamma': 44}]
        }
    }) == [
        ('a', 'b'),
        ('a', 'c', 'alpha'),
        ('a', 'c', 'beta'),
        ('a', 'c', 'gamma'),
    ]


def test_patch_diffs_with_omitted_subscripts_javascript_style():

    dm = DiffManager(style='javascript')

    diffs = dm.patch_diffs({
        'a': {
            'b': 3,
            'c': [{'alpha': 11, 'beta': 22}, {'alpha': 33, 'gamma': 44}]
        }
    })
    assert diffs == [
        'a.b',
        'a.c.alpha',
        'a.c.beta',
        'a.c.gamma',
    ]


def test_patch_diffs_with_omitted_subscripts_python_style():

    dm = DiffManager(style='python')

    diffs = dm.patch_diffs({
        'a': {
            'b': 3,
            'c': [{'alpha': 11, 'beta': 22}, {'alpha': 33, 'gamma': 44}]
        }
    })
    assert diffs == [
        'a["b"]',
        'a["c"]["alpha"]',
        'a["c"]["beta"]',
        'a["c"]["gamma"]',
    ]
