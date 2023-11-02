import os
from io import StringIO
import logging
from typing import Optional

import pandas as pd
import requests

from django.conf import settings
from .models import MonthlyFinancialFormMetricValue
from dateutil.relativedelta import *
from datetime import *
from . import helpers
from dateutil import tz

log = logging.getLogger(__name__)


def do_update():
    # Get both the past month and current dates
    date_vals = get_date_vals()
    last_month_date = date_vals[0]
    current_month_date = date_vals[1]
    existing_vals = MonthlyFinancialFormMetricValue.objects.filter(value_timestamp_utc__gte=last_month_date)
    for val in existing_vals:
        
        utc_date = get_utc_date(current_month_date, val.plant_technology.plant.timezone)

        # Update/create values for the current month from the previous month
        # This will automatically propagate values from the prev month to the current one

        MonthlyFinancialFormMetricValue.objects.update_or_create(
            plant_technology_path=val.plant_technology_path,
            value_timestamp_utc=utc_date,
            value_timestamp_local=current_month_date, 
            plant_technology=val.plant_technology, 
            monthly_financial_form_metric=val.monthly_financial_form_metric,
            defaults={
            'metric_value':val.metric_value, 
            'metric_value_string':val.metric_value_string,
            'user_created': os.getenv("DB_USERNAME"),
            'user_updated': os.getenv("DB_USERNAME")
            },
        )      
    
    return f"Monthly financial data processed for: {current_month_date} Vals processed: {len(existing_vals)}"

def get_date_vals():
    today = date.today()
    current_month = datetime(today.year, today.month, 1, 12)
    last_month = current_month+relativedelta(months=-1)
    return [last_month, current_month]

def get_utc_date(local_date, timezone):
    timezone = tz.gettz(timezone)
    current_local = datetime(local_date.year, local_date.month, local_date.day, local_date.hour).replace(tzinfo=timezone)
    utc_zone = tz.gettz('UTC')
    current_utc = current_local.astimezone(utc_zone)
    return current_utc
