from dcicutils.misc_utils import normalize_spaces
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple


def parse_datetime_string(value: str) -> Optional[datetime]:
    """
    Parses the given string into a datetime object and returns it, or if ill-formated then returns None.
    The given string is assumed to be in the format "YYYY-MM-DD hh:mm:ss" and with an optional timezone
    suffix in format "+hh:mm" or "+hh". Also allowed is just a date of the format "YYYY-MM-DD" in which
    case a time of "00:00:00" is assumed. If no timezone is specified then the local timezone is assumed.
    """
    if not isinstance(value, str) or not (value := normalize_spaces(value)):
        return None
    tz_hours = -1
    tz_minutes = -1
    if value.rfind("T") > 0:
        value = value.replace("T", " ")
    if (space := value.find(" ")) > 0 and (value_suffix := value[space + 1:]):
        if (plus := value_suffix.rfind("+")) > 0 or (minus := value_suffix.rfind("-")) > 0:
            value = normalize_spaces(value[:space] + " " + value_suffix[:(plus if plus > 0 else minus)])
            if value_tz := normalize_spaces(value_suffix[(plus if plus > 0 else minus) + 1:]):
                if len(value_tz := value_tz.split(":")) == 2:
                    value_tz_hours = value_tz[0].strip()
                    value_tz_minutes = value_tz[1].strip()
                else:
                    value_tz_hours = value_tz[0].strip()
                    value_tz_minutes = "0"
                if value_tz_hours.isdigit() and value_tz_minutes.isdigit():
                    tz_hours = int(value_tz_hours)
                    tz_minutes = int(value_tz_minutes)
                    if not (plus > 0):
                        tz_hours = -tz_hours
    else:
        value = value + " 00:00:00"
    if tz_hours < 0 or tz_minutes < 0:
        tz_hours, tz_minutes = get_local_timezone_hours_minutes()
    try:
        dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        tz = timezone(timedelta(hours=tz_hours, minutes=tz_minutes))
        return dt.replace(tzinfo=tz)
    except Exception:
        return None


def parse_date_string(value: str) -> Optional[datetime]:
    """
    Parses the given string into a datetime object representing only a date and
    returns it, or if ill-formated then returns None. The given string is assumed
    to be in the format "YYYY-MM-DD"; if a given string of this format is suffixed
    with a space or a "T" and ANYTHING else, then that trailing portion is ignored.
    """
    if isinstance(value, str) and (value := normalize_spaces(value)):
        if (separator := value.find(" ")) > 0 or (separator := value.find("T")) > 0:
            value = value[:separator]
        try:
            return datetime.strptime(value, "%Y-%m-%d")
        except Exception:
            pass


def normalize_datetime_string(value: str) -> Optional[str]:
    """
    Parses the given string into a datetime object and returns a string for that datetime in ISO-8601 format,
    or if ill-formated then returns None. The given string is assumed to be in the format "YYYY-MM-DD hh:mm:ss"
    and with an optional timezone suffix in format "+hh:mm" or "+hh". Also allowed is just a date of the
    format "YYYY-MM-DD" in which case a time of "00:00:00" is assumed. If no timezone is specified then
    the local timezone is assumed. The returned format looks like this: "2024-02-08T10:37:51-05:00"
    """
    dt = parse_datetime_string(value)
    return dt.isoformat() if dt else None


def normalize_date_string(value: str) -> Optional[str]:
    """
    Parses the given string into a datetime object representing only a date and returns a string for that
    date in ISO-8601 format, or if ill-formated then returns None. The given string is assumed to be in
    the format "YYYY-MM-DD"; but if a given string of this format is suffixed with a space followed by
    ANYTHING else, then that trailing portion is ignored. The returned format looks like this: "2024-02-08"
    """
    d = parse_date_string(value)
    return d.strftime("%Y-%m-%d") if d else None


def get_local_timezone_string() -> str:
    """
    Returns current/local timezone in format like: "-05:00".
    """
    tz_hours, tz_minutes = get_local_timezone_hours_minutes()
    return f"{tz_hours:+03d}:{tz_minutes:02d}"


def get_local_timezone_hours_minutes() -> Tuple[int, int]:
    """
    Returns a tuple with the integer hours and minutes offset for the current/local timezone.
    """
    tz_minutes = datetime.now(timezone.utc).astimezone().utcoffset().total_seconds() / 60
    return int(tz_minutes // 60), int(abs(tz_minutes % 60))
