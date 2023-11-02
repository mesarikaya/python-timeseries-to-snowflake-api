import os
from snowflake_wrapper.base import SnowflakeWrapper
from datetime import timedelta, datetime
import concurrent.futures
from .. import helpers
import logging
from decimal import Decimal, DecimalException

log = logging.getLogger(__name__)


class SnowflakeMethods:
    wrapper = None

    def __init__(self):
        if os.getenv('USE_REAL_SNOWFLAKE') == 'false':
            # If we're local and not trying to integration test, let's not try to instantiate the wrapper integration.
            # Note that this is not an env variable that Kubernetes knows about so the default must always be to instantiate the wrapper.
            self.wrapper = None
        else:
            self.wrapper = SnowflakeWrapper()

    def convert_boolean_value(self, value):
        if value is not None:
            return int(value) == 1
        return value

    def convert_to_decimal(self, value, places=4):
        if value is not None:
            return Decimal(round(Decimal(value), places))
        return None

    def handle_nulls(self, value):
        if value is None:
            return ''
        return value

    # Refer to PBI 170401 for context.
    def normalize_time_series(self, time_series_results, date_time_start, date_time_end, time_index=0):
        if len(time_series_results) > 0:
            result_start_stripped = str(time_series_results[0][time_index]).split(" ")[0]
            result_end_stripped = str(time_series_results[-1][time_index]).split(" ")[0]
            if " " in str(date_time_start):
                date_time_start = str(date_time_start).split(" ")[0]
            if "+" in str(date_time_start):
                date_time_start = str(date_time_start).split("+")[0]
            if " " in str(date_time_end):
                date_time_end = str(date_time_end).split(" ")[0]
            if "+" in str(date_time_end):
                date_time_end = str(date_time_end).split("+")[0]
            if helpers.get_datetime_obj(result_start_stripped).date() > helpers.get_datetime_obj(
                    f"{date_time_start}").date():
                time_series_results.insert(0, (None, None, f'{date_time_start}', f'{date_time_start}', 0))
        return time_series_results

def date_range(start_date, end_date):
    current_date = start_date
    next_date = current_date + timedelta(days=3)
    while current_date <= end_date:
        if next_date >= end_date:
            yield current_date, end_date
            current_date = next_date + timedelta(days=1)
        else:
            yield current_date, next_date
            current_date += timedelta(days=3)
            next_date = current_date + timedelta(days=3)
