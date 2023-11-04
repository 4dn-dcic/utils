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
        'datum': 'data',
        'metadatum': 'metadata',
        'sis': 'sisses',
        'stepsis': 'stepsisses',
    }

    @classmethod
    def _special_case_plural(cls, word: str) -> str:
        """Returns either a special case plural of its argument, or the empty string if it doesn't know."""
        return cls._SPECIAL_PLURALS.get(word, "")

    # There are some other rules in https://languagetool.org/insights/post/plural-nouns/
    # that we might want to consider at some point.
    _ENDS_IN_FE = re.compile(r".*[aeiou]fe$", flags=re.IGNORECASE)
    _ENDS_IN_F = re.compile(r".*[aeoul]f$", flags=re.IGNORECASE)
    _ENDS_IN_MAN = re.compile(r".*man$", flags=re.IGNORECASE)
    _ENDS_IN_HUMAN = re.compile(r".*human$", flags=re.IGNORECASE)
    _ENDS_IN_CHILD = re.compile(r".*child$", flags=re.IGNORECASE)
    _ENDS_IN_SIS = re.compile(r".*sis$", flags=re.IGNORECASE)
    _ENDS_IN_VOWEL_Z = re.compile(r".*[aeiou]z$", flags=re.IGNORECASE)
    _ENDS_IN_XSZ_OR_SH_OR_CH = re.compile(r".*([xsz]|[cs]h)$", flags=re.IGNORECASE)
    _ENDS_IN_NONVOWEL_Y = re.compile(r".*[^aeiou]y$", flags=re.IGNORECASE)

    @classmethod
    def _adjust_ending(cls, word, strip_chars, add_suffix):
        return (word[:-strip_chars] if strip_chars else word) + add_suffix

    _COMPOUND_PLURAL_SIMPLE_PREPOSITIONS = [
        'about', 'at', 'between', 'by', 'for', 'from', 'in', 'of', 'on', 'to', 'with'
    ]

    # Phrases like 'using', 'used by', 'used in', etc. function similarly to prepositions when doing pluralization
    # in that the plural of 'a variant referencing a gene' would be 'variants referencing genes', just as
    # the plural of 'a gene referenced by a variant' would be 'genes referenced by variants'.

    _COMPOUND_PLURAL_PSEUDO_CONNECTIVES = [
        'using', 'containing', 'including', 'referencing', 'naming',
        'used by', 'contained by', 'included by', 'referenced by', 'named by',
        'used in', 'contained in', 'included in', 'referenced in', 'named in',
    ]

    _COMPOUND_PLURAL_PREPOSITIONS = _COMPOUND_PLURAL_SIMPLE_PREPOSITIONS + _COMPOUND_PLURAL_PSEUDO_CONNECTIVES

    _NOUN_WITH_PREPOSITIONAL_ATTACHMENT = re.compile(
        # Note use of *? to get minimal match in group1, so that we'll find the first preposition, not a later one
        # Specifically, the use of "*?" makes us treat multiple prepositions like a-of-b-of-c by recursing as
        # (a)(-)(of)(-)(b-of-c). If we used "*" instead of "*?", we would get (a-of-b)(-)(of)(-)(c).
        # If we were only doing hyphenated items, it wouldn't matter a whole lot.
        # But in the words part, it matters a great deal because with spaces and hyphens interleaved, there is a
        # risk of parsing "a-of-b of c-of-d" as (a-of-b of c)(-)(of)(-)(d), whereas I think (a-of-b)( )(of)( )(c-of-d)
        # is more likely to get a better parse. -kmp 29-Aug-2021
        f"""^(?:
                 # Either these 5 will match (and the next 5 will not)
                 # to ONLY match a hyphenated compound word like son-in-law as (son)(-)(in)(-)(law)
                 ([a-z][a-z-]*?)  # shortest possible (i.e., first) block of hyphenated words preceding "-<prep>-"
                 ([-])            # pre-preposition-hyphen to make sure it's part of the same compound.
                 ({'|'.join(_COMPOUND_PLURAL_PREPOSITIONS).replace(' ', '[ ]')}) # matches the prep, as (about|at|...)
                 ([-])            # hyphen on the other side of prep
                 ([a-z-]*)        # we're less fussy about this. It could contain more preps, for example.
              |
                 # or these 5 will match (and the previous 5 will not)...
                 # to match shortest (i.e., first) words (included hyphenated words) leading to " <prep> "
                 # so 'the son-in-law of the proband' becomes (the son-in-law)( )(of)( )(the proband)
                 ([a-z]        # first token starts with an alphabetic,
                  [a-z- ]*?    # matches any number of words, which may be hyphenated,
                  [a-z])       # and ends in an alphabetic
                 ([ ]+)        # unlike with hyphenation, any number of pre-<prep> spaces is ok
                 ({'|'.join(_COMPOUND_PLURAL_PREPOSITIONS).replace(' ', '[ ]')}) # matches the prep, as (about|at|...)
                 ([ ]+)        # any amount of whitespace on the other side
                 (.*)          # anything else that follows first space-delimeted preposition
             )$""",

        re.IGNORECASE | re.VERBOSE)
    _INDEFINITE_NOUN_REF = re.compile(
        f"^an?[ -]+([^ -].*)$",
        re.IGNORECASE)

    _NOUN_WITH_CLAUSE_QUALIFIER = re.compile("^(.*[^,])(,|)[ ]+(that|which|while)[ ]+(.*)$", re.IGNORECASE)
    _IS_QUALIFIER = re.compile("^(is|was|has)[ ]+(.*)$", re.IGNORECASE)

    @classmethod
    def string_pluralize(cls, word: str, allow_some=False) -> str:
        """
        Returns the probable plural of the given word.
        This is an ad hoc string pluralizer intended for situations where being mostly right is good enough.
        e.g., string_pluralize('sample') => 'sample'
              string_pluralize('community') => 'communities'
        """

        qualifier_suffix = ""

        charn = word[-1]
        capitalize = word[0].isupper()
        upcase = word.isupper()  # capitalize and not any(ch.islower() for ch in word)

        qual_matched = cls._NOUN_WITH_CLAUSE_QUALIFIER.match(word)
        if qual_matched:
            qualified, comma, connective, qualifier = qual_matched.groups()
            word = qualified
            is_matched = cls._IS_QUALIFIER.match(qualifier)
            if is_matched:
                verb, qualifying_adj = is_matched.groups()
                orig_verb = verb
                verb = {'is': 'are', 'was': 'were', 'has': 'have'}.get(verb.lower(), verb)
                if orig_verb[0].isupper():
                    if orig_verb[-1].isupper():
                        verb = verb.upper()
                    else:
                        verb = verb.capitalize()
                qualifier = f"{verb} {qualifying_adj}"
            # Continue to other things after making a verb adjustment
            qualifier_suffix = f"{comma} {connective} {qualifier}"

        # Convert 'a foo' to just 'foo' prior to pluralization. It's pointless to return 'a apples' or 'an apples'.
        # Arguably, we _could_ return 'some apples'
        indef_matched = cls._INDEFINITE_NOUN_REF.match(word)
        if indef_matched:
            word = indef_matched.group(1)
            if allow_some:
                prefix = "SOME " if upcase else ("Some " if capitalize else "some ")
                return prefix + cls.string_pluralize(word) + qualifier_suffix

        prep_matched = cls._NOUN_WITH_PREPOSITIONAL_ATTACHMENT.match(word)
        if prep_matched:
            groups = prep_matched.groups()
            word, prep_spacing1, prep, prep_spacing2, prep_obj = groups[0:5] if groups[0] is not None else groups[5:10]
            indef_matched = cls._INDEFINITE_NOUN_REF.match(prep_obj)
            if indef_matched:
                # It's important to do this before calling ourselves recursively to avoid getting 'some' in prep phrases
                prep_obj = indef_matched.group(1)
                prep_obj = cls.string_pluralize(prep_obj)
            return cls.string_pluralize(word) + prep_spacing1 + prep + prep_spacing2 + prep_obj + qualifier_suffix

        result = cls._special_case_plural(word)
        if result:
            return result + qualifier_suffix

        if cls._ENDS_IN_FE.match(word):
            result = cls._adjust_ending(word, 2, "ves")
        elif cls._ENDS_IN_F.match(word):
            result = cls._adjust_ending(word, 1, "ves")
        elif cls._ENDS_IN_MAN.match(word) and not cls._ENDS_IN_HUMAN.match(word):
            result = cls._adjust_ending(word, 2, "e" + charn)
        elif cls._ENDS_IN_CHILD.match(word):
            result = cls._adjust_ending(word, 0, "ren")
        elif cls._ENDS_IN_SIS.match(word):
            result = cls._adjust_ending(word, 2, "es")
        elif cls._ENDS_IN_VOWEL_Z.match(word):
            result = cls._adjust_ending(word, 0, "zes")
        elif cls._ENDS_IN_XSZ_OR_SH_OR_CH.match(word):
            result = cls._adjust_ending(word, 0, "es")
        elif cls._ENDS_IN_NONVOWEL_Y.match(word):
            result = cls._adjust_ending(word, 1, "ies")
        else:
            result = cls._adjust_ending(word, 0, "s")

        if upcase:
            return result.upper() + qualifier_suffix
        elif capitalize:
            return result.capitalize() + qualifier_suffix
        else:
            return result + qualifier_suffix

    _USE_AN = {}

    _PREFIX_PATTERN_FOR_A = re.compile("^(%s)" % "|".join({
        "[^aeioux]",  # Consonants other than x need 'a' (bicycle, dog, etc.)
        "x[aeiouy]",  # x followed by any vowel makes it pronounceable like a consonant (xylophone), so needs 'a'
        "uni([^aeiuym]|[aeiuy][^aeiy])",  # things starting with "uni" are pronounced like "yuni", so need "a"
    }), flags=re.IGNORECASE)

    @classmethod
    def select_a_or_an(cls, word):
        """
        Uses a heuristic to try to select the appropriate article ("a" or "an") for a given English noun.
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
    def maybe_pluralize(cls, n, thing):
        """
        Given a number and a noun, returns the singular or plural of the noun as appropriate.

        The number may simply be a collection (list, tuple, etc.), in which case len is used.

        NOTE: The number itself does not appear in the return value.
              It is used only to determine if pluralization is needed.
        """

        if isinstance(n, (list, tuple, set, dict)):
            n = len(n)
        return thing if n == 1 else cls.string_pluralize(thing)

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
        :param possible: whether to use the word 'possible' before the given kind (default True), or a string to use
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

    _BE_TENSES = {
        'past': {'singular': 'was', 'plural': 'were'},
        'present': {'singular': 'is', 'plural': 'are'},
        'past-perfect': {'singular': 'has been', 'plural': 'have been'},
    }

    _MODALS = {'can', 'could', 'may', 'might', 'must', 'shall', 'should', 'will', 'would'}

    @classmethod
    def _conjugate_be(cls, count, tense):
        """
        Returns the conjugation of the verb 'to be' for a subject that has a given count and tense.

        For example:

        >>> EnglishUtils._conjugate_be(count=1, tense='present')
        'is'
        >>> EnglishUtils._conjugate_be(count=2, tense='present-perfect')
        'have been'
        >>> EnglishUtils._conjugate_be(count=2, tense='would')
        'would be'

        :param count: The number of items in the subject
        :param tense: The verb tense (past, past-perfect, or present)
                      or a modal (can, could, may, might, must, shall, should, will would).

        """
        is_or_are = cls._BE_TENSES.get(tense, {}).get('singular' if count == 1 else 'plural')
        if is_or_are is None:
            if tense in cls._MODALS:
                is_or_are = f"{tense} be"
            else:
                raise ValueError(f"The tense given, {tense}, was neither a supported tense"
                                 f" ({cls.disjoined_list(sorted(list(cls._BE_TENSES.keys())))})"
                                 f" nor a modal ({cls.disjoined_list(sorted(list(cls._MODALS)))}).")
        return is_or_are  # possibly in some other tense. :)

    @classmethod
    def there_are(cls, items, *, kind: str = "thing", count: Optional[int] = None, there: str = "there",
                  capitalize=True, joiner=None, zero: object = "no", punctuate=None, punctuate_none=None,
                  use_article=False, show=True, context=None, tense='present', punctuation_mark: str = ".",
                  just_are=False, **joiner_options) -> str:
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
        :param punctuate_none: in the case of no values or not showing values, whether to end with a period
               (default True if show is True, and otherwise is the same as the value of punctuate)
        :param punctuation_mark: if specified, something to use at the end if punctuating
        :param use_article: whether to put 'a' or 'an' in front of each option (default False)
        :param joiner_options: additional (keyword) options to be used with a joiner function if one is supplied
        :param show: whether to show the items if there are any (default True)
        :param context: an optional prepositional phrase indicating the context of the item(s) (default None)
        :param tense: one of 'past', 'present', 'future', 'conditional', or 'hypothetical' for the verbs used
        :param just_are: whether to stop at "There is" or "There are" without anything else.

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

        if punctuate is None:
            punctuate = False if show else True

        if punctuate_none is None:
            punctuate_none = True if show else punctuate

        there = capitalize1(there) if capitalize else there
        n = len(items) if count is None else count
        # If the items is not in the tenses table, it's assumed to be a modal like 'might', 'may', 'must', 'can' etc.
        is_or_are = cls._conjugate_be(count=n, tense=tense)
        part0 = f"{there} {is_or_are}"
        if just_are:
            return part0
        part1 = f"{part0} {n_of(n, kind, num_format=lambda n, thing: zero if n == 0 else None)}"
        if context:
            part1 += f" {context}"
        if n == 0 or not show:
            punctuation = punctuation_mark if punctuate_none else ""
            return part1 + punctuation
        else:
            if use_article:
                items = [a_or_an(str(item)) for item in items]
            else:
                items = [str(item) for item in items]
            if joiner is None:
                joined = ", ".join(items)
            else:
                joined = joiner(items, **joiner_options)
            punctuation = punctuation_mark if punctuate else ""
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
    def parse_relative_time_string(cls, s):
        parts = [x for x in s.split(' ') if x != '']
        if len(parts) % 2 != 0:
            raise ValueError(f"Relative time strings are an even number of tokens"
                             f" of the form '<n1> <unit1> <n2> <unit2>...': {s!r}")
        kwargs = {}
        for i in range(len(parts) // 2):
            # Canonicalize "1 week" or "1 weeks" to "weeks": 1.0 for inclusion as kwarg to timedelta
            # Uses specialized knowledge that all time units don't end in "s" but pluralize with "+s"
            value = float(parts[2 * i])
            units = parts[2 * i + 1].rstrip(',s') + "s"
            kwargs[units] = value
        try:
            return datetime.timedelta(**kwargs)
        except Exception:
            raise ValueError(f"Bad relative time string: {s!r}")

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
    def _item_strings(cls, items):
        return [str(x) for x in (sorted(items) if isinstance(items, set) else items)]

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

        :param items: a list of items.
            If a dictionary is given, a list of its keys will be used.
            If a set is used, it will be converted to a sorted list.
        :param conjunction: a string (default 'and') to be used before the last item if there's more than one
        :param comma: a string (default ',') to use as a comma. Semicolon (';') is the most obvious other choice,
                      or False to indicate that the conjunction should be used between all elements.
        :param oxford_comma: a boolean (default False) saying whether to use an 'Oxford comma' (ask Google),
                             or a string to use as the Oxford comma.
        :param whitespace: what to use as separator whitespace (default ' ')
        :param nothing: a string to use if there are no items, to avoid an error being raised.
        """

        items = cls._item_strings(items)
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

        if isinstance(items, dict):
            items = list(items.keys())
        elif isinstance(items, set):
            items = sorted(items)

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

maybe_pluralize = EnglishUtils.maybe_pluralize

n_of = EnglishUtils.n_of

conjoined_list = EnglishUtils.conjoined_list
disjoined_list = EnglishUtils.disjoined_list

relative_time_string = EnglishUtils.relative_time_string
parse_relative_time_string = EnglishUtils.parse_relative_time_string

select_a_or_an = EnglishUtils.select_a_or_an

string_pluralize = EnglishUtils.string_pluralize

there_are = EnglishUtils.there_are

must_be_one_of = EnglishUtils.must_be_one_of
