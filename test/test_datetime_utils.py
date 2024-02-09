from dcicutils.datetime_utils import get_local_timezone_string, normalize_date_string, normalize_datetime_string


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
