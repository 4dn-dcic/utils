import datetime
import pytest
import re

from dcicutils.lang_utils import (
    EnglishUtils, a_or_an, select_a_or_an, string_pluralize, conjoined_list, disjoined_list,
    there_are, must_be_one_of, maybe_pluralize,
)


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

    assert string_pluralize("file to show") == "files to show"
    assert string_pluralize("bucket to delete") == "buckets to delete"
    assert string_pluralize("a book about a gene") == "books about genes"
    assert string_pluralize("a good book about the defective gene") == "good books about the defective gene"
    assert string_pluralize("a good book about a defective gene") == "good books about defective genes"

    assert string_pluralize("a book about a gene", allow_some=True) == "some books about genes"
    assert string_pluralize("a bucket of data", allow_some=True) == "some buckets of data"
    assert string_pluralize("a good book about a defective gene", allow_some=True) == (
        "some good books about defective genes")

    assert string_pluralize("son-in-law") == "sons-in-law"
    assert string_pluralize("son-of-a-b") == "sons-of-bs"
    assert string_pluralize("attorney-at-law") == "attorneys-at-law"

    assert string_pluralize("mother in law") == "mothers in law"

    assert string_pluralize("person of interest") == "persons of interest"

    assert string_pluralize("author of a document") == "authors of documents"
    assert string_pluralize("author of the document") == "authors of the document"

    assert string_pluralize("father of the author of a document") == "fathers of the author of a document"
    assert string_pluralize("a father of the author of a document") == "fathers of the author of a document"
    assert string_pluralize("father of an author of a document") == "fathers of authors of documents"
    assert string_pluralize("a father of an author of a document") == "fathers of authors of documents"

    assert string_pluralize("middle name of the applicant") == "middle names of the applicant"
    assert string_pluralize("middle name of an applicant") == "middle names of applicants"
    assert string_pluralize("son-in-law of an applicant") == "sons-in-law of applicants"
    assert string_pluralize("son-in-law of a brother-in-law") == "sons-in-law of brothers-in-law"

    assert string_pluralize("half-sister of a mother-in-law") == "half-sisters of mothers-in-law"

    assert string_pluralize("report naming a gene") == "reports naming genes"
    assert string_pluralize("gene named by a report") == "genes named by reports"

    assert string_pluralize("a variant referencing a gene") == "variants referencing genes"
    assert string_pluralize("a gene referenced by a variant") == "genes referenced by variants"

    assert string_pluralize("a box") == "boxes"
    assert string_pluralize("a box", allow_some=True) == "some boxes"
    assert string_pluralize("the box") == "the boxes"
    assert string_pluralize("an apple") == "apples"
    assert string_pluralize("an apple", allow_some=True) == "some apples"
    assert string_pluralize("the apple") == "the apples"

    assert string_pluralize("A box") == "Boxes"
    assert string_pluralize("A box", allow_some=True) == "Some boxes"
    assert string_pluralize("The box") == "The boxes"
    assert string_pluralize("An apple") == "Apples"
    assert string_pluralize("An apple", allow_some=True) == "Some apples"
    assert string_pluralize("The apple") == "The apples"

    assert string_pluralize("A BOX") == "BOXES"
    assert string_pluralize("A BOX", allow_some=True) == "SOME BOXES"
    assert string_pluralize("THE BOX") == "THE BOXES"
    assert string_pluralize("AN APPLE") == "APPLES"
    assert string_pluralize("AN APPLE", allow_some=True) == "SOME APPLES"
    assert string_pluralize("THE APPLE") == "THE APPLES"


def test_n_of():
    assert EnglishUtils.n_of(-1, "day") == "-1 days"  # This could go either way, but it's easiest just to do this.
    assert EnglishUtils.n_of(0, "day") == "0 days"
    assert EnglishUtils.n_of(0.5, "day") == "0.5 days"
    assert EnglishUtils.n_of(1, "day") == "1 day"
    assert EnglishUtils.n_of(1.5, "day") == "1.5 days"
    assert EnglishUtils.n_of(2, "day") == "2 days"
    assert EnglishUtils.n_of(2.5, "day") == "2.5 days"

    assert EnglishUtils.n_of(7, "variant") == '7 variants'
    assert EnglishUtils.n_of(1, "accession") == '1 accession'
    assert EnglishUtils.n_of(['alpha', 'beta', 'gamma'], 'Greek letter') == '3 Greek letters'


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

    relative_time = datetime.timedelta(hours=1, seconds=3)
    test(relative_time, "1 hour, 3 seconds", "1 hour")
    t1 = datetime.datetime.now()
    t2 = t1 + relative_time
    test(t2 - t1, "1 hour, 3 seconds", "1 hour")


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


def test_conjoined_list():

    with pytest.raises(ValueError):
        assert conjoined_list([])

    assert conjoined_list([], nothing='nothing') == 'nothing'
    assert conjoined_list(['a']) == 'a'
    assert conjoined_list(['a', 'b']) == 'a and b'
    assert conjoined_list(['a', 'b', 'c']) == 'a, b and c'
    assert conjoined_list(['a', 'b', 'c'], oxford_comma=True) == 'a, b, and c'
    assert conjoined_list(['a', 'b', 'c', 'd']) == 'a, b, c and d'

    assert conjoined_list(['a'], conjunction='or') == 'a'
    assert conjoined_list(['a', 'b'], conjunction='or') == 'a or b'
    assert conjoined_list(['a', 'b', 'c'], conjunction='or') == 'a, b or c'
    assert conjoined_list(['a', 'b', 'c', 'd'], conjunction='or') == 'a, b, c or d'
    assert conjoined_list(['a', 'b', 'c', 'd'], conjunction='or', oxford_comma=True) == 'a, b, c, or d'

    assert conjoined_list(['a'], conjunction='AND') == 'a'
    assert conjoined_list(['a', 'b'], conjunction='AND') == 'a AND b'
    assert conjoined_list(['a', 'b', 'c', 'd'], conjunction='AND') == 'a, b, c AND d'
    assert conjoined_list(['a', 'b', 'c', 'd'], conjunction='AND', oxford_comma=True) == 'a, b, c, AND d'

    assert conjoined_list(['a'], comma=';') == 'a'
    assert conjoined_list(['a', 'b'], comma=';') == 'a and b'
    assert conjoined_list(['a', 'b', 'c', 'd'], comma=';') == 'a; b; c and d'
    assert conjoined_list(['a', 'b', 'c', 'd'], comma=';', oxford_comma=True) == 'a; b; c; and d'

    assert conjoined_list(['a'], comma=False) == 'a'
    assert conjoined_list(['a', 'b'], comma=False) == 'a and b'
    assert conjoined_list(['a', 'b', 'c'], comma=False) == 'a and b and c'
    assert conjoined_list(['a', 'b', 'c', 'd'], comma=False) == 'a and b and c and d'
    # oxford_comma does nothing if comma is disabled.
    assert conjoined_list(['a', 'b', 'c', 'd'], comma=False, oxford_comma=True) == 'a and b and c and d'

    assert conjoined_list(['a'], comma=False, whitespace='_') == 'a'
    assert conjoined_list(['a', 'b'], comma=False, whitespace='_') == 'a_and_b'
    assert conjoined_list(['a', 'b', 'c'], comma=False, whitespace='_') == 'a_and_b_and_c'
    assert conjoined_list(['a', 'b', 'c', 'd'], comma=False, whitespace='_') == 'a_and_b_and_c_and_d'
    # oxford_comma does nothing if comma is disabled.
    assert conjoined_list(['a', 'b', 'c', 'd'], comma=False, whitespace='_', oxford_comma=True) == 'a_and_b_and_c_and_d'

    # Verify that the same function is a method
    assert EnglishUtils.conjoined_list([], nothing='nothing') == 'nothing'
    assert EnglishUtils.conjoined_list(['a']) == 'a'
    assert EnglishUtils.conjoined_list(['a', 'b']) == 'a and b'
    assert EnglishUtils.conjoined_list(['a', 'b', 'c']) == 'a, b and c'
    assert EnglishUtils.conjoined_list(['a', 'b', 'c', 'd']) == 'a, b, c and d'


def test_disjoined_list():

    assert disjoined_list(['a']) == 'a'
    assert disjoined_list(['a', 'b']) == 'a or b'
    assert disjoined_list(['a', 'b', 'c']) == 'a, b or c'
    assert disjoined_list(['a', 'b', 'c', 'd']) == 'a, b, c or d'

    assert disjoined_list(['a'], oxford_comma=True) == 'a'
    assert disjoined_list(['a', 'b'], oxford_comma=True) == 'a or b'
    assert disjoined_list(['a', 'b', 'c'], oxford_comma=True) == 'a, b, or c'
    assert disjoined_list(['a', 'b', 'c', 'd'], oxford_comma=True) == 'a, b, c, or d'


def test_there_are():

    assert there_are([]) == "There are no things."
    assert there_are([], zero=0) == "There are 0 things."

    assert there_are(['foo']) == "There is 1 thing: foo"
    assert there_are(['foo'], punctuate=True) == "There is 1 thing: foo."

    assert there_are(['box', 'bugle', 'bear']) == "There are 3 things: box, bugle, bear"
    assert there_are(['box', 'bugle', 'bear'], joiner=conjoined_list) == "There are 3 things: box, bugle and bear"
    assert there_are(['box', 'bugle', 'bear'],
                     joiner=conjoined_list, oxford_comma=True) == "There are 3 things: box, bugle, and bear"
    assert there_are(['box', 'bugle', 'bear'],
                     joiner=conjoined_list, oxford_comma=True, kind="option", conjunction="or"
                     ) == "There are 3 options: box, bugle, or bear"
    assert there_are(['apple', 'egg', 'steak'], use_article=True, punctuate=True,
                     joiner=disjoined_list, oxford_comma=True, kind="option", conjunction="or",
                     ) == "There are 3 options: an apple, an egg, or a steak."
    assert there_are(['apple', 'egg', 'steak'], use_article=True, joiner=conjoined_list, kind="option", punctuate=True,
                     ) == "There are 3 options: an apple, an egg and a steak."

    assert there_are([2, 3, 5, 7], kind="single-digit prime") == "There are 4 single-digit primes: 2, 3, 5, 7"
    assert there_are([2, 3, 5, 7], kind="single-digit prime", punctuate=True, joiner=conjoined_list,
                     ) == "There are 4 single-digit primes: 2, 3, 5 and 7."

    # From the doc strings

    assert there_are(['Joe', 'Sally'], kind="user") == "There are 2 users: Joe, Sally"
    assert there_are(['Joe', 'Sally'], kind="user", show=False) == "There are 2 users."
    assert there_are(['Joe', 'Sally'], kind="user", show=False, context="online") == "There are 2 users online."
    assert there_are(['Joe'], kind="user") == "There is 1 user: Joe"
    assert there_are([], kind="user") == "There are no users."
    assert there_are([], kind="user", context="online") == "There are no users online."

    assert there_are(['Joe', 'Sally'], kind="user", joiner=conjoined_list, punctuate=True
                     ) == "There are 2 users: Joe and Sally."
    assert there_are(['Joe'], kind="user", joiner=conjoined_list, punctuate=True) == "There is 1 user: Joe."
    assert there_are([], kind="user", joiner=conjoined_list, punctuate=True) == "There are no users."

    def check_tense(tense, if0, if1, if2):
        assert there_are([], tense=tense, show=False, kind="foo") == if0
        assert there_are(['x'], tense=tense, show=False, kind="foo") == if1
        assert there_are(['x', 'y'], tense=tense, show=False, kind="foo") == if2

    check_tense('past',
                if0="There were no foos.",
                if1="There was 1 foo.",
                if2="There were 2 foos.")

    check_tense('present',
                if0="There are no foos.",
                if1="There is 1 foo.",
                if2="There are 2 foos.")

    check_tense('will',
                if0="There will be no foos.",
                if1="There will be 1 foo.",
                if2="There will be 2 foos.")

    check_tense('would',
                if0="There would be no foos.",
                if1="There would be 1 foo.",
                if2="There would be 2 foos.")

    check_tense('past-perfect',
                if0="There have been no foos.",
                if1="There has been 1 foo.",
                if2="There have been 2 foos.")

    expected_error = ("The tense given, randomness, was"
                      " neither a supported tense (past, past-perfect or present)"
                      " nor a modal (can, could, may, might, must, shall, should, will or would).")
    with pytest.raises(ValueError, match=re.escape(expected_error)):
        there_are([], tense='randomness', show=False, kind="foo")


def test_must_be():

    assert must_be_one_of([], possible=False) == "There are no options."
    assert must_be_one_of(['foo'], possible=False) == "The only option is foo."
    assert must_be_one_of(['foo', 'bar'], possible=False) == "Options are foo and bar."
    assert must_be_one_of(['foo', 'bar', 'baz'], possible=False) == "Options are foo, bar and baz."

    assert must_be_one_of([]) == "There are no possible options."
    assert must_be_one_of(['foo']) == "The only possible option is foo."
    assert must_be_one_of(['foo', 'bar']) == "Possible options are foo and bar."
    assert must_be_one_of(['foo', 'bar', 'baz']) == "Possible options are foo, bar and baz."

    assert must_be_one_of([], quote=True) == "There are no possible options."
    assert must_be_one_of(['foo'], quote=True) == "The only possible option is 'foo'."
    assert must_be_one_of(['foo', 'bar'], quote=True) == "Possible options are 'foo' and 'bar'."
    assert must_be_one_of(['foo', 'bar', 'baz'], quote=True) == "Possible options are 'foo', 'bar' and 'baz'."

    assert must_be_one_of([], possible='valid', kind='argument') == "There are no valid arguments."
    assert must_be_one_of(['A'], possible='valid', kind='argument') == "The only valid argument is A."
    assert must_be_one_of(['A', 'B'], possible='valid', kind='argument') == "Valid arguments are A and B."
    assert must_be_one_of(['A', 'B', 'C'], possible='valid', kind='argument') == "Valid arguments are A, B and C."


def test_maybe_pluralize():

    assert maybe_pluralize(0, 'gene') == 'genes'
    assert maybe_pluralize(1, 'gene') == 'gene'
    assert maybe_pluralize(2, 'gene') == 'genes'
    assert maybe_pluralize(3, 'gene') == 'genes'

    assert maybe_pluralize([], 'gene') == 'genes'
    assert maybe_pluralize(['a'], 'gene') == 'gene'
    assert maybe_pluralize(['a', 'b'], 'gene') == 'genes'
    assert maybe_pluralize(['a', 'b', 'c'], 'gene') == 'genes'
