from datetime import datetime, timezone
from dcicutils.datetime_utils import (
    get_local_timezone_string, normalize_date_string, normalize_datetime_string,
)
from dcicutils.datetime_utils import (
    get_local_timezone, get_local_timezone_hours_minutes,
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


_tzlocal = get_local_timezone()
_tzlocal_offset_hours, _tzlocal_offset_minutes = get_local_timezone_hours_minutes()
_tzutc = get_utc_timezone()


def _assert_datetime_equals(value: datetime, year: int, month: int, day: int,
                            hour: int, minute: int, second: int, microsecond: int, tz: timezone = _tzlocal):

    if not isinstance(tz, timezone):
        tz = _tzlocal

    expected_value = datetime(year=year, month=month, day=day, hour=hour,
                              minute=minute, second=second, microsecond=microsecond, tzinfo=tz)
    assert value == expected_value

    tz_offset_hours, tz_offset_minutes = get_timezone_hours_minutes(tz)
    expected_value = datetime(year=year, month=month, day=day,
                              hour=(hour - tz_offset_hours),
                              minute=(minute - tz_offset_minutes),
                              second=second, microsecond=microsecond, tzinfo=_tzutc)
    assert value == expected_value


def test_parse_datetime_a():

    value = "2024-04-17T15:04:16.434698+00:00"
    _assert_datetime_equals(parse_datetime(value), 2024, 4, 17, 15, 4, 16, 434698, tz=_tzutc)
    _assert_datetime_equals(parse_datetime(value), 2024, 4, 17,
                            15 + _tzlocal_offset_hours, 4 + _tzlocal_offset_minutes, 16, 434698, tz=_tzlocal)

    # value = "2024-04-17T15:04:16.434698-04:00"
    # _assert_datetime_equals(parse_datetime(value), 2024, 4, 17, 15, 4, 16, 434698)
