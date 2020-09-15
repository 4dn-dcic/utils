import datetime
import re

from .qa_utils import ignored


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


# Export specific useful functions

a_or_an = EnglishUtils.a_or_an

n_of = EnglishUtils.n_of

relative_time_string = EnglishUtils.relative_time_string

select_a_or_an = EnglishUtils.select_a_or_an

string_pluralize = EnglishUtils.string_pluralize
