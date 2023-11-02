import os
from django.conf import settings
from django.test import TestCase
from dateutil.relativedelta import *
from snowflake_drf.models import MonthlyFinancialFormMetricValue
from datetime import *
import pandas as pd
from dateutil import tz

from snowflake_drf import task_data_import, task_monthly_financial_data


BASE_DIR = settings.BASE_DIR
FIXTURE_DIR = BASE_DIR / 'snowflake_drf' / 'fixtures'


class TaskTestCase(TestCase):

    fixtures = ['regions', 'technologies', 'plants', 'planttechnologies', 'monthlyfinancialformmetrics', 'monthlyfinancialformmetricvalues_realdata']

    def setUp(self):
        os.environ.setdefault('DB_USERNAME', '')

    def test_load_monthly_financial_data(self):
        dates = self.get_opportunity_financial_datetimes()
        # update fixture data
        monthly_val = MonthlyFinancialFormMetricValue.objects.get(pk=19)
        monthly_val.value_timestamp_utc = dates[3]
        monthly_val.save()
        monthly_val = MonthlyFinancialFormMetricValue.objects.get(pk=20)
        monthly_val.value_timestamp_utc = dates[3]
        monthly_val.save()

        # define expected string
        expected = f"Monthly financial data processed for: {dates[1]} Vals processed: 2"

        actual = task_monthly_financial_data.do_update()
        self.assertEqual(expected,actual)

    def test_load_monthly_financial_data_none_to_update(self):

        dates = self.get_opportunity_financial_datetimes()

        # update fixture data
        MonthlyFinancialFormMetricValue.objects.all().update(datetime_updated=datetime(2021, 12, 1))

        # define expected string
        expected = f"Monthly financial data processed for: {dates[1]} Vals processed: 0"

        actual = task_monthly_financial_data.do_update()
        self.assertEqual(expected,actual)

    def test_utc_convert_na(self):
        local_date = datetime(2023, 5, 1, 6)
        actual_date = task_monthly_financial_data.get_utc_date(local_date, "America/Chicago")
        timezone = tz.gettz("UTC")
        self.assertEqual(datetime(2023, 5, 1, 11).replace(tzinfo=timezone), actual_date)

    def test_utc_convert_emea(self):
        local_date = datetime(2023, 6, 1, 15)
        actual_date = task_monthly_financial_data.get_utc_date(local_date, "Europe/Amsterdam")
        timezone = tz.gettz("UTC")
        self.assertEqual(datetime(2023, 6, 1, 13).replace(tzinfo=timezone), actual_date)

    def test_process_lead_indicators(self):
        from snowflake_drf.models import ConfigurationFormLeadingIndicator
        df = pd.read_csv(FIXTURE_DIR / 'el_plant.csv')
        task_data_import.process_plant(df)
        df = pd.read_csv(FIXTURE_DIR / 'el_technology.csv')
        task_data_import.process_technology(df)
        df = pd.read_csv(FIXTURE_DIR / 'el_plant_technology.csv')
        task_data_import.process_plant_technology(df)
        df = pd.read_csv(FIXTURE_DIR / 'el_mtpm.csv')
        task_data_import.process_mtpm(df)
        df = pd.read_csv(FIXTURE_DIR / 'el_lead_indicators.csv')
        task_data_import.process_lead_indicators(df)
        self.assertGreater(len(ConfigurationFormLeadingIndicator.objects.all()), 0)

    def test_process_mtpm(self):
        from snowflake_drf.models import ConfigurationFormMTPM
        df = pd.read_csv(FIXTURE_DIR / 'el_plant.csv')
        task_data_import.process_plant(df)
        df = pd.read_csv(FIXTURE_DIR / 'el_technology.csv')
        task_data_import.process_technology(df)
        df = pd.read_csv(FIXTURE_DIR / 'el_plant_technology.csv')
        task_data_import.process_plant_technology(df)
        df = pd.read_csv(FIXTURE_DIR / 'el_mtpm.csv')
        task_data_import.process_mtpm(df)
        self.assertGreater(len(ConfigurationFormMTPM.objects.all()), 0)

    def test_process_plant_technologies(self):
        from snowflake_drf.models import PlantTechnology
        df = pd.read_csv(FIXTURE_DIR / 'el_plant.csv')
        task_data_import.process_plant(df)
        df = pd.read_csv(FIXTURE_DIR / 'el_technology.csv')
        task_data_import.process_technology(df)
        df = pd.read_csv(FIXTURE_DIR / 'el_plant_technology.csv')
        task_data_import.process_plant_technology(df)
        self.assertGreater(len(PlantTechnology.objects.all()), 0)

    def get_opportunity_financial_datetimes(self):
        # date logic is in the test class rather than encapsulated in utils because if the logic for the task
        # changes (deliberately or as a regression) we should also update the tests; it's a core funcitonality
        # for the task.
        today = date.today()
        current_month = datetime(today.year, today.month, 1, 12)
        last_month = current_month+relativedelta(months=-1)
        next_month = current_month+relativedelta(months=+1)
        third_month = current_month+relativedelta(months=+2)
        return [last_month, current_month, next_month, third_month]