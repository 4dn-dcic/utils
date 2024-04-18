from datetime import datetime, timezone, timedelta
from typing import Optional
from dcicutils.datetime_utils import (
    get_local_timezone_string, normalize_date_string, normalize_datetime_string,
)
from dcicutils.datetime_utils import (
    format_datetime, get_local_timezone, get_timezone,
    get_timezone_hours_minutes, get_utc_timezone, parse_datetime
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
TZLOCAL_NAME = None
TZUTC = None
TZUTC_SUFFIX = None


def _setup_global_timezone_constants(tzlocal: Optional[timezone] = None) -> None:

    global TZLOCAL, TZLOCAL_OFFSET_HOURS, TZLOCAL_OFFSET_MINUTES, TZLOCAL_SUFFIX, TZLOCAL_NAME, TZUTC, TZUTC_SUFFIX

    TZLOCAL = tzlocal if isinstance(tzlocal, timezone) else get_local_timezone()

    if TZLOCAL != get_local_timezone():
        import dcicutils.datetime_utils
        dcicutils.datetime_utils.get_local_timezone = lambda: TZLOCAL

    TZLOCAL_OFFSET_HOURS, TZLOCAL_OFFSET_MINUTES = get_timezone_hours_minutes(TZLOCAL)
    TZLOCAL_SUFFIX = (f"{'-' if TZLOCAL_OFFSET_HOURS < 0 else '+'}"
                      f"{abs(TZLOCAL_OFFSET_HOURS):02}:{TZLOCAL_OFFSET_MINUTES:02}")
    TZLOCAL_NAME = TZLOCAL.tzname(None)

    TZUTC = get_utc_timezone()
    TZUTC_SUFFIX = f"+00:00"
    TZUTC_SUFFIX = "Z"


def _assert_datetime_equals(value: datetime, year: int, month: int, day: int,
                            hour: int, minute: int, second: int, microsecond: Optional[int],
                            tz: timezone = TZLOCAL,
                            shift_hours: Optional[int] = None,
                            shift_minutes: Optional[int] = None):

    if not isinstance(tz, timezone):
        tz = TZLOCAL

    expected_value = datetime(year=year, month=month, day=day, hour=hour,
                              minute=minute, second=second, microsecond=microsecond or 0, tzinfo=tz)
    if isinstance(shift_hours, int):
        expected_value = expected_value + timedelta(hours=shift_hours)
    if isinstance(shift_minutes, int):
        expected_value = expected_value + timedelta(hours=shift_minutes)
    assert value == expected_value


def _test_parse_datetime_a(ms: Optional[int] = None):

    ms_suffix = f".{ms}" if isinstance(ms, int) else ""
    ms = ms if isinstance(ms, int) else None

    # --------------------------------------------------------------------------------------------------
    value = f"2024-04-17T15:04:16{ms_suffix}"
    parsed = parse_datetime(value)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15, 4, 16, ms, TZLOCAL)
    assert format_datetime(parsed, notz=True) == f"2024-04-17 15:04:16"
    assert format_datetime(parsed) == f"2024-04-17 15:04:16 {TZLOCAL_NAME}"
    assert format_datetime(parsed, ms=ms is not None) == f"2024-04-17 15:04:16{ms_suffix} {TZLOCAL_NAME}"
    assert format_datetime(parsed, noseconds=True) == f"2024-04-17 15:04 {TZLOCAL_NAME}"
    assert format_datetime(parsed, iso=True) == f"2024-04-17T15:04:16{TZLOCAL_SUFFIX}"
    assert format_datetime(parsed, iso=True, notz=True) == f"2024-04-17T15:04:16"
    assert format_datetime(parsed, iso=True, ms=ms is not None) == f"2024-04-17T15:04:16{ms_suffix}{TZLOCAL_SUFFIX}"
    assert (format_datetime(parsed, verbose=True, ms=ms is not None) ==
            f"Wednesday, April 17, 2024 | 3:04:16{ms_suffix} PM {TZLOCAL_NAME}")
    assert (format_datetime(parsed, ms=ms is not None, verbose=True, noseparator=True) ==
            f"Wednesday, April 17, 2024 3:04:16{ms_suffix} PM {TZLOCAL_NAME}")
    assert (format_datetime(parsed, ms=ms is not None, verbose=True, noseparator=True, noday=True) ==
            f"April 17, 2024 3:04:16{ms_suffix} PM {TZLOCAL_NAME}")

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
    _assert_datetime_equals(parsed, 2024, 4, 17, 15, 4, 16, ms, TZLOCAL,
                            shift_hours=TZLOCAL_OFFSET_HOURS, shift_minutes=TZLOCAL_OFFSET_MINUTES)

    # --------------------------------------------------------------------------------------------------
    value = f"2024-04-17T15:04:16{ms_suffix}{TZLOCAL_SUFFIX}"
    parsed = parse_datetime(value)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15, 4, 16, ms, TZLOCAL)

    value = f"2024-04-17T15:04:16{ms_suffix}{TZLOCAL_SUFFIX}"
    parsed = parse_datetime(value, utc=True)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15, 4, 16, ms, TZUTC,
                            shift_hours=-TZLOCAL_OFFSET_HOURS, shift_minutes=TZLOCAL_OFFSET_MINUTES)

    value = f"2024-04-17T15:04:16{ms_suffix}{TZLOCAL_SUFFIX}"
    parsed = parse_datetime(value, tz=TZUTC)
    _assert_datetime_equals(parsed, 2024, 4, 17, 15, 4, 16, ms, TZUTC,
                            shift_hours=-TZLOCAL_OFFSET_HOURS, shift_minutes=TZLOCAL_OFFSET_MINUTES)

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

    _setup_global_timezone_constants(tzlocal=get_timezone(7))
    _test_parse_datetime_a()
    _test_parse_datetime_a(ms=434698)

    _setup_global_timezone_constants(tzlocal=get_timezone(9))
    _test_parse_datetime_a()

    for zone in range(-24, 24 + 1):
        _setup_global_timezone_constants(tzlocal=get_timezone(zone))
        _test_parse_datetime_a()
        _test_parse_datetime_a(ms=434698)
