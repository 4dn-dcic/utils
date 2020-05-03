from dcicutils.lang_utils import EnglishUtils, a_or_an, select_a_or_an, string_pluralize


def test_string_pluralize_case():
    # Check case
    assert EnglishUtils.string_pluralize("dog") == "dogs"
    assert EnglishUtils.string_pluralize("Dog") == "Dogs"
    assert EnglishUtils.string_pluralize("DOG") == "DOGS"


def test_string_pluralize():

    assert EnglishUtils.string_pluralize("child") == "children"

    assert EnglishUtils.string_pluralize("knife") == "knives"
    assert EnglishUtils.string_pluralize("shelf") == "shelves"
    assert EnglishUtils.string_pluralize("waif") == "waifs"
    assert EnglishUtils.string_pluralize("wife") == "wives"

    assert EnglishUtils.string_pluralize("beach") == "beaches"
    assert EnglishUtils.string_pluralize("wish") == "wishes"
    assert EnglishUtils.string_pluralize("sandwich") == "sandwiches"

    assert EnglishUtils.string_pluralize("quorum") == "quorums"  # but see CustomEnglishUtils below
    assert EnglishUtils.string_pluralize("vacuum") == "vacuums"

    assert EnglishUtils.string_pluralize("pan") == "pans"
    assert EnglishUtils.string_pluralize("man") == "men"
    assert EnglishUtils.string_pluralize("woman") == "women"
    assert EnglishUtils.string_pluralize("human") == "humans"
    assert EnglishUtils.string_pluralize("superhuman") == "superhumans"

    assert EnglishUtils.string_pluralize("box") == "boxes"

    assert EnglishUtils.string_pluralize("index") == "indexes"  # but see CustomEnglishUtils below

    assert EnglishUtils.string_pluralize("day") == "days"
    assert EnglishUtils.string_pluralize("pony") == "ponies"
    assert EnglishUtils.string_pluralize("turkey") == "turkeys"

    assert EnglishUtils.string_pluralize("waltz") == "waltzes"
    assert EnglishUtils.string_pluralize("whiz") == "whizzes"
    assert EnglishUtils.string_pluralize("fizz") == "fizzes"

    assert string_pluralize("box") == "boxes"
    assert string_pluralize("ox") == "oxen"


def test_n_of():
    assert EnglishUtils.n_of(-1, "day") == "-1 days"  # This could go either way, but it's easiest just to do this.
    assert EnglishUtils.n_of(0, "day") == "0 days"
    assert EnglishUtils.n_of(0.5, "day") == "0.5 days"
    assert EnglishUtils.n_of(1, "day") == "1 day"
    assert EnglishUtils.n_of(1.5, "day") == "1.5 days"
    assert EnglishUtils.n_of(2, "day") == "2 days"
    assert EnglishUtils.n_of(2.5, "day") == "2.5 days"


def test_relative_time_string():

    def test(seconds, long_string, short_string):
        assert EnglishUtils.relative_time_string(seconds) == long_string
        assert EnglishUtils.relative_time_string(seconds, detailed=False) == short_string

    (SECOND, MINUTE, HOUR, DAY, WEEK) = (
        EnglishUtils.SECOND, EnglishUtils.MINUTE, EnglishUtils.HOUR, EnglishUtils.DAY, EnglishUtils.WEEK)

    test(SECOND, "1 second", "1 second")
    test(MINUTE, "1 minute", "1 minute")
    test(HOUR, "1 hour", "1 hour")
    test(DAY, "1 day", "1 day")
    test(WEEK, "1 week", "1 week")

    test(SECOND + SECOND, "2 seconds", "2 seconds")
    test(MINUTE + SECOND, "1 minute, 1 second", "1 minute, 1 second")
    test(HOUR + SECOND, "1 hour, 1 second", "1 hour")
    test(DAY + SECOND, "1 day, 1 second", "1 day")
    test(WEEK + SECOND, "1 week, 1 second", "1 week")

    test(SECOND + MINUTE, "1 minute, 1 second", "1 minute, 1 second")
    test(MINUTE + MINUTE, "2 minutes", "2 minutes")
    test(HOUR + MINUTE, "1 hour, 1 minute", "1 hour, 1 minute")
    test(DAY + MINUTE, "1 day, 1 minute", "1 day")
    test(WEEK + MINUTE, "1 week, 1 minute", "1 week")

    test(5 * MINUTE, "5 minutes", "5 minutes")
    test(1 * HOUR + 5 * MINUTE, "1 hour, 5 minutes", "1 hour, 5 minutes")
    test(4 * DAY + 1 * HOUR + 5 * MINUTE, "4 days, 1 hour, 5 minutes", "4 days, 1 hour")
    test(4 * DAY + 5 * MINUTE, "4 days, 5 minutes", "4 days")
    test(2 * WEEK + 3 * HOUR + 2 * MINUTE, "2 weeks, 3 hours, 2 minutes", "2 weeks")
    test(2 * WEEK + 3 * HOUR + 2 * MINUTE + 3 * SECOND, "2 weeks, 3 hours, 2 minutes, 3 seconds", "2 weeks")
    test(4 * DAY + 1 * HOUR + 5.2 * SECOND, "4 days, 1 hour, 5.2 seconds", "4 days, 1 hour")
    test(5.2 * SECOND, "5.2 seconds", "5.2 seconds")


def test_time_count_formatter():

    x = "ignored"

    assert EnglishUtils._time_count_formatter(1234567890.11111111, x) == "1234567890.111111"
    assert EnglishUtils._time_count_formatter(1234567890.11111110, x) == "1234567890.111111"
    assert EnglishUtils._time_count_formatter(1234567890.11111000, x) == "1234567890.11111"
    assert EnglishUtils._time_count_formatter(1234567890.11110000, x) == "1234567890.1111"
    assert EnglishUtils._time_count_formatter(1234567890.11100000, x) == "1234567890.111"
    assert EnglishUtils._time_count_formatter(1234567890.11000000, x) == "1234567890.11"
    assert EnglishUtils._time_count_formatter(1234567890.10000000, x) == "1234567890.1"

    assert EnglishUtils._time_count_formatter(1234567890.77777777, x) == "1234567890.777778"
    assert EnglishUtils._time_count_formatter(1234567890.77777770, x) == "1234567890.777778"
    assert EnglishUtils._time_count_formatter(1234567890.77777000, x) == "1234567890.77777"
    assert EnglishUtils._time_count_formatter(1234567890.77770000, x) == "1234567890.7777"
    assert EnglishUtils._time_count_formatter(1234567890.77700000, x) == "1234567890.777"
    assert EnglishUtils._time_count_formatter(1234567890.77000000, x) == "1234567890.77"
    assert EnglishUtils._time_count_formatter(1234567890.70000000, x) == "1234567890.7"


def test_select_a_or_an():

    assert EnglishUtils.select_a_or_an("alpha") == "an"
    assert EnglishUtils.select_a_or_an("bravo") == "a"
    assert EnglishUtils.select_a_or_an("charlie") == "a"
    assert EnglishUtils.select_a_or_an("delta") == "a"
    assert EnglishUtils.select_a_or_an("echo") == "an"
    assert EnglishUtils.select_a_or_an("foxtrot") == "a"
    assert EnglishUtils.select_a_or_an("golf") == "a"
    assert EnglishUtils.select_a_or_an("hotel") == "a"
    assert EnglishUtils.select_a_or_an("india") == "an"
    assert EnglishUtils.select_a_or_an("juliet") == "a"
    assert EnglishUtils.select_a_or_an("kilo") == "a"
    assert EnglishUtils.select_a_or_an("lima") == "a"
    assert EnglishUtils.select_a_or_an("mike") == "a"
    assert EnglishUtils.select_a_or_an("november") == "a"
    assert EnglishUtils.select_a_or_an("oscar") == "an"
    assert EnglishUtils.select_a_or_an("papa") == "a"
    assert EnglishUtils.select_a_or_an("quebec") == "a"
    assert EnglishUtils.select_a_or_an("romeo") == "a"
    assert EnglishUtils.select_a_or_an("sierra") == "a"
    assert EnglishUtils.select_a_or_an("tango") == "a"
    assert EnglishUtils.select_a_or_an("unabridged") == "an"
    assert EnglishUtils.select_a_or_an("unbridled") == "an"
    assert EnglishUtils.select_a_or_an("unclear") == "an"
    assert EnglishUtils.select_a_or_an("undead") == "an"
    assert EnglishUtils.select_a_or_an("unease") == "an"
    assert EnglishUtils.select_a_or_an("unending") == "an"
    assert EnglishUtils.select_a_or_an("unfair") == "an"
    assert EnglishUtils.select_a_or_an("ungainly") == "an"
    assert EnglishUtils.select_a_or_an("unhappy") == "an"
    assert EnglishUtils.select_a_or_an("uniform") == "a"
    assert EnglishUtils.select_a_or_an("union") == "a"
    assert EnglishUtils.select_a_or_an("unimaginable") == "an"
    assert EnglishUtils.select_a_or_an("unimportant") == "an"
    assert EnglishUtils.select_a_or_an("unifying") == "a"
    assert EnglishUtils.select_a_or_an("unofficial") == "an"
    assert EnglishUtils.select_a_or_an("victor") == "a"
    assert EnglishUtils.select_a_or_an("whiskey") == "a"
    assert EnglishUtils.select_a_or_an("xray") == "an"
    assert EnglishUtils.select_a_or_an("xylophone") == "a"
    assert EnglishUtils.select_a_or_an("yankee") == "a"
    assert EnglishUtils.select_a_or_an("zulu") == "a"

    assert select_a_or_an("box") == "a"
    assert select_a_or_an("egg") == "an"


def test_a_or_an():

    assert EnglishUtils.a_or_an("box") == "a box"
    assert EnglishUtils.a_or_an("egg") == "an egg"

    assert a_or_an("box") == "a box"
    assert a_or_an("egg") == "an egg"


def test_custom_english_utils():

    class CustomEnglishUtils(EnglishUtils):

        MY_PLURALS = {
            'index': 'indices',
            'quorum': 'quora'
        }

        @classmethod
        def _special_case_plural(cls, word: str) -> str:
            return cls.MY_PLURALS.get(word) or super(CustomEnglishUtils, cls)._special_case_plural(word)

    assert CustomEnglishUtils.string_pluralize("index") == "indices"
    assert CustomEnglishUtils.string_pluralize("quorum") == "quora"

    # This tests that we're inheriting special case behavior from the parent
    assert CustomEnglishUtils.string_pluralize("sheep") == "sheep"

    # This dog & pony show tests that we're getting regular behavior from the parent
    assert CustomEnglishUtils.string_pluralize("dog") == "dogs"
    assert CustomEnglishUtils.string_pluralize("pony") == "ponies"

    # Tests that all the functions are integrating well
    assert CustomEnglishUtils.n_of(3, "index") == "3 indices"


def test_custom_a_or_an():

    def maybe_a_or_n(n, thing):
        if n == 0:
            return "no"
        elif n == 1:
            return EnglishUtils.select_a_or_an(thing)
        elif n < 5:
            return "some"
        else:
            return "many"

    assert EnglishUtils.n_of(0, "box", num_format=maybe_a_or_n) == "no boxes"
    assert EnglishUtils.n_of(1, "box", num_format=maybe_a_or_n) == "a box"
    assert EnglishUtils.n_of(3, "box", num_format=maybe_a_or_n) == "some boxes"
    assert EnglishUtils.n_of(9, "box", num_format=maybe_a_or_n) == "many boxes"

    assert EnglishUtils.n_of(0, "egg", num_format=maybe_a_or_n) == "no eggs"
    assert EnglishUtils.n_of(1, "egg", num_format=maybe_a_or_n) == "an egg"
    assert EnglishUtils.n_of(3, "egg", num_format=maybe_a_or_n) == "some eggs"

