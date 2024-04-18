from datetime import datetime, timezone
from typing import Optional
from dcicutils.datetime_utils import (
    get_local_timezone_string, normalize_date_string, normalize_datetime_string,
)
from dcicutils.datetime_utils import (
    get_local_timezone, get_timezone, get_timezone_hours_minutes,
    get_utc_timezone, parse_datetime
)


def test_normalize_datetime_string():

    tz = get_local_timezone_string()

    value = "2024-02-08T10:37:51-05:00"
    assert normalize_datetime_string(value) == "2024-02-08T10:37:51" + tz

    value = " 2024-01-28  17:15:32"
    assert normalize_datetime_string(value) == "2024-01-28T17:15:32" + tz

    value = "2024-02-08"
    assert normalize_datetime_string(value) == "2024-02-08T00:00:00" + tz

    value = " 2024-01-28  17:15:32  + 03:34"
    assert normalize_datetime_string(value) == "2024-01-28T17:15:32+03:34"


def test_normalize_date_string():

    value = " 2024-01-28"
    assert normalize_date_string(value) == "2024-01-28"

    value = "2024-02-08T10:37:51-05:00"
    assert normalize_date_string(value) == "2024-02-08"

    value = " 2024-01-28  17:15:32  + 03:34"
    assert normalize_date_string(value) == "2024-01-28"


TZLOCAL = None
TZLOCAL_OFFSET_HOURS = None
TZLOCAL_OFFSET_MINUTES = None
TZLOCAL_SUFFIX = None
TZUTC = None
TZUTC_SUFFIX = None


def _setup_global_timezone_constants(tzlocal: Optional[timezone] = None) -> None:

    global TZLOCAL, TZLOCAL_OFFSET_HOURS, TZLOCAL_OFFSET_MINUTES, TZLOCAL_SUFFIX, TZUTC, TZUTC_SUFFIX

    TZLOCAL = tzlocal if isinstance(tzlocal, timezone) else get_local_timezone()

    if TZLOCAL != get_local_timezone():
        import dcicutils.datetime_utils
        dcicutils.datetime_utils.get_local_timezone = lambda: TZLOCAL

    TZLOCAL_OFFSET_HOURS, TZLOCAL_OFFSET_MINUTES = get_timezone_hours_minutes(TZLOCAL)
    TZLOCAL_SUFFIX = (f"{'-' if TZLOCAL_OFFSET_HOURS < 0 else '+'}"
                      f"{abs(TZLOCAL_OFFSET_HOURS):02}:{TZLOCAL_OFFSET_MINUTES:02}")
    TZUTC = get_utc_timezone()
    TZUTC_SUFFIX = f"+00:00"


def _assert_datetime_equals(value: datetime, year: int, month: int, day: int,
                            hour: int, minute: int, second: int, microsecond: int, tz: timezone = TZLOCAL):

    if not isinstance(tz, timezone):
        tz = TZLOCAL

    expected_value = datetime(year=year, month=month, day=day, hour=hour,
                              minute=minute, second=second, microsecond=microsecond, tzinfo=tz)
    assert value == expected_value

    tz_offset_hours, tz_offset_minutes = get_timezone_hours_minutes(tz)
    expected_value = datetime(year=year, month=month, day=day,
                              hour=(hour - tz_offset_hours),
                              minute=(minute - tz_offset_minutes),
                              second=second, microsecond=microsecond, tzinfo=TZUTC)
    assert value == expected_value


def _test_parse_datetime_a(ms: Optional[int] = None):

    ms_suffix = f".{ms}" if isinstance(ms, int) else ""
    ms = ms if isinstance(ms, int) else 0

    # --------------------------------------------------------------------------------------------------
    value = f"2024-04-17T15:04:16{ms_suffix}"
    parsed = parse_datetime(value)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15, 4, 16, ms, TZLOCAL)

    value = f"2024-04-17T15:04:16{ms_suffix}"
    parsed = parse_datetime(value, utc=True)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15, 4, 16, ms, TZUTC)

    value = f"2024-04-17T15:04:16{ms_suffix}"
    parsed = parse_datetime(value, tz=TZUTC)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15, 4, 16, ms, TZUTC)

    value = f"2024-04-17T15:04:16{ms_suffix}"
    parsed = parse_datetime(value, tz=TZLOCAL)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15, 4, 16, ms, TZLOCAL)

    # --------------------------------------------------------------------------------------------------
    value = f"2024-04-17T15:04:16{ms_suffix}{TZUTC_SUFFIX}"
    parsed = parse_datetime(value)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15, 4, 16, ms, TZUTC)

    value = f"2024-04-17T15:04:16{ms_suffix}{TZUTC_SUFFIX}"
    parsed = parse_datetime(value, utc=True)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15, 4, 16, ms, TZUTC)

    value = f"2024-04-17T15:04:16{ms_suffix}{TZUTC_SUFFIX}"
    parsed = parse_datetime(value, tz=TZUTC)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15, 4, 16, ms, TZUTC)

    value = f"2024-04-17T15:04:16{ms_suffix}{TZUTC_SUFFIX}"
    parsed = parse_datetime(value, tz=TZLOCAL)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15 + TZLOCAL_OFFSET_HOURS, 4, 16, ms, TZLOCAL)

    # --------------------------------------------------------------------------------------------------
    value = f"2024-04-17T15:04:16{ms_suffix}{TZLOCAL_SUFFIX}"
    parsed = parse_datetime(value)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15, 4, 16, ms, TZLOCAL)

    value = f"2024-04-17T15:04:16{ms_suffix}{TZLOCAL_SUFFIX}"
    parsed = parse_datetime(value, utc=True)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15 - TZLOCAL_OFFSET_HOURS, 4, 16, ms, TZUTC)

    value = f"2024-04-17T15:04:16{ms_suffix}{TZLOCAL_SUFFIX}"
    parsed = parse_datetime(value, tz=TZUTC)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15 - TZLOCAL_OFFSET_HOURS, 4, 16, ms, TZUTC)

    value = f"2024-04-17T15:04:16{ms_suffix}{TZLOCAL_SUFFIX}"
    parsed = parse_datetime(value, tz=TZLOCAL)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15, 4, 16, ms, TZLOCAL)


def test_parse_datetime_a(ms: Optional[int] = None):

    _setup_global_timezone_constants()
    _test_parse_datetime_a()
    _test_parse_datetime_a(ms=434698)

    _setup_global_timezone_constants(tzlocal=get_timezone(4))
    _test_parse_datetime_a()
    _test_parse_datetime_a(ms=434698)

    _setup_global_timezone_constants(tzlocal=get_timezone(5))
    _test_parse_datetime_a()
    _test_parse_datetime_a(ms=434698)

    _setup_global_timezone_constants(tzlocal=get_timezone(-4))
    _test_parse_datetime_a()
    _test_parse_datetime_a(ms=434698)
