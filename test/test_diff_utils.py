from dcicutils.diff_utils import DiffManager


def test_unroll():

    dm = DiffManager()

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

    dm = DiffManager()

    assert dm.diffs(1, 1) == {"same": ["item"]}

    assert dm.diffs("foo", "bar") == {"changed": ["item"]}

    assert dm.diffs({"a": "foo", "b": "bar"}, {"a": "foo", "b": "baz"}) == {
        "changed": ["item.b"],
        "same": ["item.a"],
    }

def test_comparison():

    dm = DiffManager()

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

    dm = DiffManager(style='python')

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
