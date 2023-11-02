import calendar
import pytz
from datetime import datetime, timezone
from dateutil.parser import *
from dateutil import tz


def get_datetime_obj(datetime_string, timezone=None):
    if datetime_string is None:
        return datetime_string
    datetime_string = str(datetime_string)
    return parse(datetime_string) if timezone is None else parse(datetime_string).replace(tzinfo=timezone)


def convert_utc_to_local(datetime_utc, timezone):
    if datetime_utc is None:
        return datetime_utc
    to_zone = tz.gettz(timezone)
    from_zone = tz.gettz('UTC')
    datetime_utc = datetime_utc.replace(tzinfo=from_zone)
    localized = datetime_utc.astimezone(to_zone)
    return localized

def convert_local_to_utc(datetime_local, origin_timezone):
    if datetime_local is None:
        return datetime_local
    from_zone = tz.gettz(origin_timezone)
    to_zone = tz.gettz('UTC')
    datetime_utc = datetime_local.replace(tzinfo=from_zone)
    localized = datetime_utc.astimezone(to_zone)
    return localized


def format_datetime(datetime_obj):
    formatted = datetime.strftime(datetime_obj, '%d %b, %H:%M')
    return formatted


def get_epoch(datetime_obj, local_timezone=None):
    if local_timezone is None:
        epoch_time = int(datetime.strftime(datetime_obj, '%s'))
    else:
        to_zone = tz.gettz(local_timezone)
        epoch_time = int((datetime_obj - datetime(1970, 1, 1, tzinfo=to_zone)).total_seconds())
    return epoch_time


def get_eom(convert_date):
    days_in_month = calendar.monthrange(convert_date.year, convert_date.month)[1]
    end_dt = datetime(convert_date.year, convert_date.month, days_in_month, 0, 0, 000000, tzinfo=pytz.UTC)
    return end_dt


def get_prev_month(convert_date):
    if convert_date.month == 1:
        value_converted = convert_date.replace(month=12)
        value_converted = value_converted.replace(year=convert_date.year - 1)
    else:
        value_converted = convert_date.replace(month=convert_date.month - 1)
    return value_converted.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f %z')


def get_semantic_datetime(convert_date):
    # this is brittle on purpose: https://stackoverflow.com/questions/28154066/how-to-convert-datetime-to-integer-in-python
    date_replaced = f'{convert_date}'.replace("-", "")
    return date_replaced


def trim_date_string(date_string, digits=10):
    return f'{date_string}'[:digits]