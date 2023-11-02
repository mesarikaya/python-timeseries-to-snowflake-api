from __future__ import absolute_import, unicode_literals

from celery import shared_task
import logging
from snowflake_drf import task_data_import, task_monthly_financial_data

log = logging.getLogger(__name__)

@shared_task(bind=True)
def checks_for_updates_on_element_unify(self):
    msg = "Checking for updates on Elementy Unify - Each 15 minutes"
    log.info(msg)
    msg = task_data_import.process_element_data()
    return msg

@shared_task(bind=True)
def refresh_financial_data(self):
    msg = "Updating financial data"
    log.info(msg)
    msg = task_monthly_financial_data.do_update()
    return msg
