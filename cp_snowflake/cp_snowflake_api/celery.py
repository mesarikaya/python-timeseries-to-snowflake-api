from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from django.conf import settings
from celery.schedules import crontab
from ddtrace import patch

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'cp_snowflake_api.settings')

patch(celery=True)
app = Celery('cp_snowflake_api')

broker_url = settings.REDIS_LOCAL_URL if settings.REDIS_PRIMARY_ENDPOINT is None else settings.REDIS_URL

# print(f"broker_url: {broker_url}")

app.conf.update(
    enable_utc=True,
    broker_url=broker_url,
    accept_content=['application/json'],
    result_serializer='json',
    task_serializer='json',
    result_backend='django-db',
    result_extended=True,
    beat_scheduler='django_celery_beat.schedulers:DatabaseScheduler',
    worker_hijack_root_logger=False
)

app.conf.beat_schedule = {
   'checking_element_unify': {
       'task': 'snowflake_drf.tasks.checks_for_updates_on_element_unify',
       'schedule': crontab(hour=8, minute=00),
   },
   'monthly_financial_data': {
       'task': 'snowflake_drf.tasks.refresh_financial_data',
       'schedule': crontab(minute=0, hour=0, day_of_month='1'),
   }
}

app.autodiscover_tasks()


@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
