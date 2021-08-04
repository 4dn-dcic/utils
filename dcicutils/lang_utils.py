import datetime
import re

from .misc_utils import ignored, capitalize1
from typing import Union, Optional


class EnglishUtils:
    """
    In most cases you can get away without using this class, but it's here in case customization is needed.
    The usual interfaces are available as separate functions.
    """

    SECOND = 1
    MINUTE = 60 * SECOND
    HOUR = 60 * MINUTE
    DAY = 24 * HOUR
    WEEK = 7 * DAY

    _TIME_UNITS = ((WEEK, "week"), (DAY, "day"), (HOUR, "hour"), (MINUTE, "minute"))

    _SPECIAL_PLURALS = {
        "radius": "radii",
        "spectrum": "spectra",
        "ovum": "ova",
        "deer": "deer",
        "fish": "fish",
        "goose": "geese",
        "sheep": "sheep",
        "tooth": "teeth",
        "foot": "feet",
        "ox": "oxen",
    }

    @classmethod
    def _special_case_plural(cls, word: str) -> str:
        """Returns either a special case plural of its argument, or the empty string if it doesn't know."""
        return cls._SPECIAL_PLURALS.get(word, "")

    _ENDS_IN_FE = re.compile(r".*[aeiou]fe$", flags=re.IGNORECASE)
    _ENDS_IN_F = re.compile(r".*[aeoul]f$", flags=re.IGNORECASE)
    _ENDS_IN_MAN = re.compile(r".*man$", flags=re.IGNORECASE)
    _ENDS_IN_HUMAN = re.compile(r".*human$", flags=re.IGNORECASE)
    _ENDS_IN_CHILD = re.compile(r".*child$", flags=re.IGNORECASE)
    _ENDS_IN_VOWEL_Z = re.compile(r".*[aeiou]z$", flags=re.IGNORECASE)
    _ENDS_IN_XSZ_OR_SH_OR_CH = re.compile(r".*([xsz]|[cs]h)$", flags=re.IGNORECASE)
    _ENDS_IN_NONVOWEL_Y = re.compile(r".*[^aeiou]y$", flags=re.IGNORECASE)

    @classmethod
    def _adjust_ending(cls, word, strip_chars, add_suffix):
        return (word[:-strip_chars] if strip_chars else word) + add_suffix

    @classmethod
    def string_pluralize(cls, word: str) -> str:
        """
        Returns the probable plural of the given word.
        This is an ad hoc string pluralizer intended for situations where being mostly right is good enough.
        e.g., string_pluralize('sample') => 'sample'
              string_pluralize('community') => 'communities'
        """
        charn = word[-1]
        char1 = word[0]
        capitalize = char1.isupper()
        upcase = capitalize and not any(ch.islower() for ch in word)

        result = cls._special_case_plural(word)
        if result:
            return result

        if cls._ENDS_IN_FE.match(word):
            result = cls._adjust_ending(word, 2, "ves")
        elif cls._ENDS_IN_F.match(word):
            result = cls._adjust_ending(word, 1, "ves")
        elif cls._ENDS_IN_MAN.match(word) and not cls._ENDS_IN_HUMAN.match(word):
            result = cls._adjust_ending(word, 2, "e" + charn)
        elif cls._ENDS_IN_CHILD.match(word):
            result = cls._adjust_ending(word, 0, "ren")
        elif cls._ENDS_IN_VOWEL_Z.match(word):
            result = cls._adjust_ending(word, 0, "zes")
        elif cls._ENDS_IN_XSZ_OR_SH_OR_CH.match(word):
            result = cls._adjust_ending(word, 0, "es")
        elif cls._ENDS_IN_NONVOWEL_Y.match(word):
            result = cls._adjust_ending(word, 1, "ies")
        else:
            result = cls._adjust_ending(word, 0, "s")

        if upcase:
            return result.upper()
        elif capitalize:
            return result.capitalize()
        else:
            return result

    _USE_AN = {}

    _PREFIX_PATTERN_FOR_A = re.compile("^(%s)" % "|".join({
        "[^aeioux]",  # Consonants other than x need 'a' (bicycle, dog, etc.)
        "x[aeiouy]",  # x followed by any vowel makes it pronounceable like a consonant (xylophone), so needs 'a'
        "uni([^aeiuym]|[aeiuy][^aeiy])",  # things starting with with "uni" are pronounced like "yuni", so need "a"
    }), flags=re.IGNORECASE)

    @classmethod
    def select_a_or_an(cls, word):
        """
        Uses a heuristic to try to select the appropriate article ('a' or 'an') for a given English noun.
        select_a_or_an("gene") => 'a'
        select_a_or_an("accession") => 'an'
        """

        return "a" if cls._PREFIX_PATTERN_FOR_A.match(word) else "an"

    @classmethod
    def a_or_an(cls, word):
        """
        Heuristically attaches either "a" or "an" to a given English noun.
        a_or_an("gene") => "a gene"
        a_or_an("accession") => "an accession"
        """
        article = cls.select_a_or_an(word)
        return "%s %s" % (article, word)

    @classmethod
    def n_of(cls, n, thing, num_format=None):
        """
        Given a number and a noun, returns the name for that many of that noun.

        Examples:

            >>> n_of(7, "variant")
            '7 variants'
            >>> n_of(1, "accession")
            '1 accession'
            >>> n_of(['alpha', 'beta', 'gamma'], 'Greek letter')
            '3 Greek letters'
        """
        if isinstance(n, (list, tuple, set, dict)):
            n = len(n)
        display_n = n
        if num_format:
            res = num_format(n, thing)
            if res:
                display_n = res
        return "%s %s" % (display_n, thing if n == 1 else cls.string_pluralize(thing))

    @classmethod
    def must_be_one_of(cls, items, *, possible: Union[bool, str] = True, kind: str = "option", quote=False,
                       capitalize=True, joiner=None, **joiner_options):
        """
        Constructs a sentence that complains about a given quantity not being among a given set of options.

        This is intended to be useful in error messages to enumerate a set of values, usually but not necessarily
        strings, that had been expected but not received. For example:

        >>> must_be_one_of([])
        "There are no possible options."
        >>> must_be_one_of(['foo'])
        "The only possible option is foo."
        >>> must_be_one_of(['foo', 'bar'])
        "Possible options are foo and bar."
        >>> must_be_one_of(['foo', 'bar', 'baz'])
        "Possible options are foo, bar and baz."

        :param items: the items to enumerate
        :param possible: whether to use the word 'possible' before the given kind (default True), or an string to use
        :param kind: the kind of items being enumerated (default "option")
        :param quote: whether to put quotes around each option
        :param capitalize: whether to capitalize the first letter of the sentence (default True)
        :param joiner: the joining function to join the items (default if None is just a commas-separated list)
        :param joiner_options: additional (keyword) options to be used with a joiner function if one is supplied
        """

        n = len(items)
        maybe_adj = ""
        if possible:
            if possible is True:
                possible = "possible"
            maybe_adj = possible + " "
        if not joiner:
            joiner = cls.conjoined_list
        if quote:
            # First force to a string, so we don't call the item's repr, then use repr to add quotation marks.
            items = [repr(str(item)) for item in items]
        if n == 0:
            kinds = cls.string_pluralize(kind)
            result = f"there are no {maybe_adj}{kinds}."
        elif n == 1:
            [item] = items
            result = f"the only {maybe_adj}{kind} is {item}."
        else:
            kinds = cls.string_pluralize(kind)
            options = joiner(items, **joiner_options)
            result = f"{maybe_adj}{kinds} are {options}."
        if capitalize:
            result = capitalize1(result)
        return result

    @classmethod
    def there_are(cls, items, *, kind: str = "thing", count: Optional[int] = None, there: str = "there",
                  capitalize=True, joiner=None, zero: object = "no", punctuate=False, use_article=False,
                  **joiner_options) -> str:
        """
        Constructs a sentence that enumerates a set of things.

        :param items: the items to enumerate
        :param kind: the kind of items being enumerated (default "thing")
        :param count: the number of items (defaults to the result of 'len(items)')
        :param there: the demonstrative or noun phrase that starts the sentence (default "there")
        :param capitalize: whether to capitalize the first letter of the sentence (default True)
        :param joiner: the joining function to join the items (default if None is just a commas-separated list)
        :param zero: the value to print instead of a numeric zero (default "no")
        :param punctuate: in the case of one or more values (not zero), whether to end with a period (default False)
        :param use_article: whether to put 'a' or 'an' in front of each option (default False)
        :param joiner_options: additional (keyword) options to be used with a joiner function if one is supplied

        By far the most common uses are likely to be:

        >>> there_are(['Joe', 'Sally'], kind="user")
        "There are 2 users: Joe, Sally"
        >>> there_are(['Joe'], kind="user")
        "There is 1 user: Joe"
        >>> there_are([], kind="user")
        "There are no users."

        There are various control options. For example:

        >>> there_are(['Joe', 'Sally'], kind="user", joiner=conjoined_list, punctuate=True)
        "There are 2 users: Joe and Sally."
        >>> there_are(['Joe'], kind="user", joiner=conjoined_list, punctuate=True)
        "There is 1 user: Joe."
        >>> there_are([], kind="user", joiner=conjoined_list, punctuate=True)
        "There are no users."

        """

        there = capitalize1(there) if capitalize else there
        n = len(items) if count is None else count
        is_or_are = "is" if n == 1 else "are"
        part1 = f"{there} {is_or_are} {n_of(n, kind, num_format=lambda n, thing: zero if n == 0 else None)}"
        if n == 0:
            return part1 + "."
        else:
            if use_article:
                items = [a_or_an(str(item)) for item in items]
            else:
                items = [str(item) for item in items]
            if joiner is None:
                joined = ", ".join(items)
            else:
                joined = joiner(items, **joiner_options)
            punctuation = "." if punctuate else ""
            return f"{part1}: {joined}{punctuation}"

    @classmethod
    def _time_count_formatter(cls, n, unit):
        ignored(unit)
        if isinstance(n, float):
            return ("%.6f" % n).rstrip('0').rstrip('.')
        else:
            return n

    @classmethod
    def relative_time_string(cls, seconds, detailed=True):
        """
        Given a number of seconds, expresses that number of seconds in English.
        The seconds can be expressed either as a number or a datetime.timedelta.
        """
        result = []
        if isinstance(seconds, datetime.timedelta):
            seconds = seconds.total_seconds()
        remaining_seconds = seconds
        units_seen = False
        for unit_info in cls._TIME_UNITS:
            (unit_seconds, unit_name) = unit_info
            number_of_units = int(remaining_seconds // unit_seconds)
            remaining_seconds = remaining_seconds % unit_seconds
            if number_of_units != 0:
                units_seen = True
                result.append(cls.n_of(number_of_units, unit_name, num_format=cls._time_count_formatter))
            else:
                result.append(None)
        if not units_seen or remaining_seconds != 0:
            result.append(cls.n_of(remaining_seconds, "second", num_format=cls._time_count_formatter))
        if not detailed:
            abbreviated = []
            for item in result:
                if item or abbreviated:
                    stopping = bool(abbreviated)  # Stopping if this is the second item
                    abbreviated.append(item)
                    if stopping:
                        break
            result = abbreviated
        result = [item for item in result
                  if isinstance(item, str)]
        result = ", ".join(result)
        return result

    @classmethod
    def disjoined_list(cls, items, conjunction: str = 'or', comma: Union[bool, str] = ",",
                       oxford_comma: Union[bool, str] = False, whitespace: str = " ",
                       nothing: Optional[str] = None) -> str:
        """
        Given a list of items, returns an English string that describes the option of any of them,
        joined by commas, as needed, and with the conjunction 'or' before the last item if there's more than one.

        For example:

        >>> disjoined_list(['something'])
        'something'
        >>> disjoined_list(['P', 'NP'])
        'P or NP'
        >>> disjoined_list(['this', 'that', 'the other'])
        'this, that or the other'
        >>> disjoined_list(['this', 'that', 'the other'], oxford_comma=True)
        'this, that, or the other'
        >>> disjoined_list(['this', 'that', 'the other'], comma=False)
        'this or that or the other'

        :param items: a list of items
        :param conjunction: a string (default 'or') to be used before the last item if there's more than one
        :param comma: a string (default ',') to use as a comma. Semicolon (';') is the most obvious other choice,
                      or False to indicate that the conjunction should be used between all elements.
        :param oxford_comma: a boolean (default False) saying whether to use a so-called 'Oxford comma',
                             or a string to use as that comma.
        :param whitespace: what to use as separator whitespace (default ' ')
        :param nothing: a string to use if there are no items, to avoid an error being raised.
        """

        return cls.conjoined_list(items, conjunction=conjunction, comma=comma, oxford_comma=oxford_comma,
                                  whitespace=whitespace, nothing=nothing)

    @classmethod
    def conjoined_list(cls, items, conjunction: str = 'and', comma: Union[bool, str] = ",",
                       oxford_comma: Union[bool, str] = False, whitespace: str = " ",
                       nothing: Optional[str] = None) -> str:
        """
        Given a list of items, returns an English string that describes the collection of all of them,
        joined by commas, as needed, and with the conjunction 'and' before the last item if more than one item.

        For example:

        >>> conjoined_list(['something'])
        'something'
        >>> conjoined_list(['yin', 'yang'])
        'yin and yang'
        >>> conjoined_list(['up', 'down', 'all around'])
        'up, down and all around'
        >>> conjoined_list(['up', 'down', 'all around'], oxford_comma=True)
        'up, down, and all around'
        >>> conjoined_list(['up', 'down', 'all around'], comma=False)
        'up and down and all around'

        :param items: a list of items
        :param conjunction: a string (default 'and') to be used before the last item if there's more than one
        :param comma: a string (default ',') to use as a comma. Semicolon (';') is the most obvious other choice,
                      or False to indicate that the conjunction should be used between all elements.
        :param oxford_comma: a boolean (default False) saying whether to use an 'Oxford comma' (ask Google),
                             or a string to use as the Oxford comma.
        :param whitespace: what to use as separator whitespace (default ' ')
        :param nothing: a string to use if there are no items, to avoid an error being raised.
        """

        assert isinstance(conjunction, str), "The 'conjunction' argument must a string or boolean."
        conj = conjunction + whitespace

        if comma is False:
            sep = whitespace + conj
            oxford_comma = False  # It would be odd to
        elif comma is True:
            sep = "," + whitespace
        else:
            assert isinstance(comma, str), "The 'comma' argument must a string or boolean."
            sep = comma + whitespace

        if oxford_comma is False:
            final_sep = whitespace
        elif oxford_comma is True:
            final_sep = sep
        else:
            assert isinstance(oxford_comma, str), "The 'oxford_comma' argument must a string or boolean."
            final_sep = oxford_comma + whitespace

        n = len(items)

        if n == 0:
            if nothing:
                return nothing
            else:
                raise ValueError("Cannot construct a conjoined list with no elements.")
        elif n == 1:
            return str(items[0])
        elif n == 2:
            return f"{items[0]}{whitespace}{conj}{items[1]}"
        else:
            return sep.join(items[:-1]) + f"{final_sep}{conj}{items[-1]}"


# Export specific useful functions

a_or_an = EnglishUtils.a_or_an

n_of = EnglishUtils.n_of

conjoined_list = EnglishUtils.conjoined_list
disjoined_list = EnglishUtils.disjoined_list

relative_time_string = EnglishUtils.relative_time_string

select_a_or_an = EnglishUtils.select_a_or_an

string_pluralize = EnglishUtils.string_pluralize

there_are = EnglishUtils.there_are

must_be_one_of = EnglishUtils.must_be_one_of
