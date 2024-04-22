from dcicutils.misc_utils import normalize_spaces
from datetime import datetime, timedelta, timezone
from dateutil import parser as datetime_parser
from typing import Optional, Tuple, Union

TIMEZONE_LOCAL = datetime.now().astimezone().tzinfo  # type: datetime.timezone
TIMEZONE_LOCAL_NAME = TIMEZONE_LOCAL.tzname(None)  # type: str
TIMEZONE_LOCAL_OFFSET = TIMEZONE_LOCAL.utcoffset(None)  # type: datetime.timedelta
TIMEZONE_LOCAL_OFFSET_TOTAL_MINUTES = int(TIMEZONE_LOCAL_OFFSET.total_seconds()) // 60  # type: int
TIMEZONE_LOCAL_OFFSET_HOURS = TIMEZONE_LOCAL_OFFSET_TOTAL_MINUTES // 60  # type: int
TIMEZONE_LOCAL_OFFSET_MINUTES = TIMEZONE_LOCAL_OFFSET_TOTAL_MINUTES % 60  # type: int
TIMEZONE_LOCAL_SUFFIX = f"{TIMEZONE_LOCAL_OFFSET_HOURS:+03d}:{TIMEZONE_LOCAL_OFFSET_MINUTES:02d}"  # type: str

TIMEZONE_UTC = timezone.utc  # type: datetime.timezone
TIMEZONE_UTC_NAME = TIMEZONE_UTC.tzname(None)  # type: str
TIMEZONE_UTC_OFFSET = timedelta(0)  # type: datetime.timedelta
TIMEZONE_UTC_OFFSET_TOTAL_MINUTES = 0  # type: int
TIMEZONE_UTC_OFFSET_HOURS = 0  # type: int
TIMEZONE_UTC_OFFSET_MINUTES = 0  # type: int
TIMEZONE_UTC_SUFFIX = "Z"  # type: str


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


def get_timezone(hours_or_timedelta: Union[int, timedelta], minutes: Optional[int] = None) -> timezone:
    try:
        if isinstance(hours_or_timedelta, timedelta):
            return timezone(hours_or_timedelta)
        return timezone(timedelta(hours=hours_or_timedelta, minutes=minutes or 0))
    except Exception:
        return TIMEZONE_LOCAL


def get_timezone_offset(tz: timezone) -> timedelta:
    try:
        return tz.utcoffset(None)
    except Exception:
        return TIMEZONE_LOCAL_OFFSET


def get_timezone_hours_minutes(tz: timezone) -> Tuple[int, int]:
    """
    Returns a tuple with the integer hours and minutes offset for the given timezone.
    If negative then only the hours is negative; the mintutes is always positive;
    this is okay because there are no timezones less than one hour from UTC.
    """
    tz_offset = get_timezone_offset(tz)
    tz_offset_total_minutes = int(tz_offset.total_seconds()) // 60
    tz_offset_hours = tz_offset_total_minutes // 60
    tz_offset_minutes = abs(tz_offset_total_minutes % 60)
    return tz_offset_hours, tz_offset_minutes


def get_utc_timezone() -> timezone:
    return TIMEZONE_UTC


def get_local_timezone() -> timezone:
    """
    Returns current/local timezone as a datetime.timezone object.
    """
    return TIMEZONE_LOCAL


def get_local_timezone_string() -> str:
    """
    Returns current/local timezone in format like: "-05:00".
    """
    return TIMEZONE_LOCAL_SUFFIX


def get_local_timezone_hours_minutes() -> Tuple[int, int]:
    """
    Returns a tuple with the integer hours and minutes offset for the current/local timezone.
    If negative then only the hours is negative; the mintutes is always positive;
    this is okay because there are no timezones less than one hour from UTC.
    """
    return TIMEZONE_LOCAL_OFFSET_HOURS, TIMEZONE_LOCAL_OFFSET_MINUTES


def parse_datetime(value: str, utc: bool = False, tz: Optional[timezone] = None) -> Optional[datetime]:
    """
    Parses the given string into a datetime, if possible, and returns that value,
    or None if not able to parse. The timezone of the returned datetime will be the
    local timezone; or if the given utc argument is True then it will be UTC; or if the
    given tz argument is a datetime.timezone then return datetime will be in that timezone.
    """
    if isinstance(value, datetime):
        return value
    elif not isinstance(value, str):
        return None
    try:
        # This dateutil.parser handles quite a wide variety of formats and suits our needs.
        value = datetime_parser.parse(value)
        if utc is True:
            # If the given utc argument is True then it trumps any tz argument if given.
            tz = timezone.utc
        if value.tzinfo is not None:
            # The given value had an explicit timezone specified.
            if isinstance(tz, timezone):
                return value.astimezone(tz)
            return value
        return value.replace(tzinfo=tz if isinstance(tz, timezone) else get_local_timezone())
    except Exception:
        return None


def format_datetime(value: datetime,
                    utc: bool = False,
                    tz: Optional[Union[timezone, bool]] = None,
                    iso: bool = False,
                    notz: bool = False,
                    noseconds: bool = False,
                    ms: bool = False,
                    verbose: bool = False,
                    noseparator: bool = False,
                    noday: bool = False,
                    nodate: bool = False,
                    notime: bool = False) -> str:
    """
    Returns the given datetime as a string in "YYYY:MM:DD hh:mm:ss tz" format, for
    example "2024-04-17 15:42:26 EDT". If the given notz argument is True then omits
    the timezone; if the noseconds argument is given the omits the seconds. If the given
    verbose argument is True then returns a really verbose version of the datetime, for
    example "Wednesday, April 17, 2024 | 15:42:26 EDT"; if the noseparator argument is
    True then omits the "|" separator; if the noday argument is True then omits the day
    of week part. The timezone of the returned datetime string will default to the local
    one; if the given utc argument is True then it will be UTC; or if the given tz
    argument is a datetime.timezone it will be in that timezone.
    """
    if nodate is True and notime is True:
        return ""
    if not isinstance(value, datetime):
        if not isinstance(value, str) or not (value := parse_datetime(value)):
            return ""
    try:
        if utc is True:
            tz = timezone.utc
        elif not isinstance(tz, timezone):
            tz = get_local_timezone()
            if tz is True:
                notz = False
            elif tz is False:
                notz = True
        if noseconds is True:
            ms = False
        value = value.astimezone(tz)
        if iso:
            if notz is True:
                value = value.replace(tzinfo=None)
            if not (ms is True):
                value = value.replace(microsecond=0)
            if noseconds is True:
                if notz is True:
                    if nodate is True:
                        return value.strftime(f"%H:%M")
                    elif notime is True:
                        return value.strftime(f"%Y-%m-%d")
                    else:
                        return value.strftime(f"%Y-%m-%dT%H:%M")
                if len(tz := value.strftime("%z")) > 3:
                    tz = tz[:3] + ":" + tz[3:]
                if nodate is True:
                    return value.strftime(f"%H:%M") + tz
                elif notime is True:
                    return value.strftime(f"%Y-%m-%d") + tz
                else:
                    return value.strftime(f"%Y-%m-%dT%H:%M") + tz
            if nodate is True:
                if (not (notz is True)) and len(tz := value.strftime("%z")) > 3:
                    tz = tz[:3] + ":" + tz[3:]
                else:
                    tz = ""
                return value.strftime(f"%H:%M:%S{f'.%f' if ms is True else ''}") + tz
            elif notime is True:
                return value.strftime(f"%Y-%m-%d")
            else:
                return value.isoformat()
        if verbose:
            if nodate is True:
                return value.strftime(
                    f"%-I:%M{'' if noseconds is True else ':%S'}"
                    f"{f'.%f' if ms is True else ''} %p{'' if notz is True else ' %Z'}")
            elif notime is True:
                return value.strftime(f"{'' if noday is True else '%A, '}%B %-d, %Y")
            else:
                return value.strftime(
                    f"{'' if noday is True else '%A, '}%B %-d, %Y{'' if noseparator is True else ' |'}"
                    f" %-I:%M{'' if noseconds is True else ':%S'}"
                    f"{f'.%f' if ms is True else ''} %p{'' if notz is True else ' %Z'}")
        else:
            if nodate is True:
                return value.strftime(
                    f"%H:%M{'' if noseconds is True else ':%S'}"
                    f"{f'.%f' if ms is True else ''}{'' if notz is True else ' %Z'}")
            elif notime is True:
                return value.strftime(f"%Y-%m-%d")
            else:
                return value.strftime(
                    f"%Y-%m-%d %H:%M{'' if noseconds is True else ':%S'}"
                    f"{f'.%f' if ms is True else ''}{'' if notz is True else ' %Z'}")
    except Exception:
        return None


def format_date(value: datetime,
                utc: bool = False,
                tz: Optional[Union[timezone, bool]] = None,
                verbose: bool = False,
                noday: bool = False) -> str:
    return format_datetime(value, utc=utc, tz=tz, verbose=verbose, noday=noday, notime=True)


def format_time(value: datetime,
                utc: bool = False,
                iso: bool = False,
                tz: Optional[Union[timezone, bool]] = None,
                ms: bool = False,
                notz: bool = False,
                noseconds: bool = False,
                verbose: bool = False,
                noday: bool = False) -> str:
    return format_datetime(value, utc=utc, tz=tz, iso=iso, ms=ms, notz=notz,
                           noseconds=noseconds, verbose=verbose, nodate=True)
