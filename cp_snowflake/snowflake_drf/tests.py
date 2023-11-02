import concurrent.futures
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from decimal import Decimal

from django.test import TestCase, tag
import json
import logging
import os

from . import helpers
from .queries.methods import date_range, SnowflakeMethods
from .queries.equipment_tags import EquipmentTags
from .queries.leading_indicators import LeadingIndicators
from .queries.mtpms import Mtpms
from .queries.plant_data import PlantData
from .token_utils import generate_non_sso_token, generate_client_credentials_token
from .test_utils import set_auth_header, set_sso_client_credential_token, set_sso_user_token

from snowflake_drf.task_data_import import process_element_data, process_region, process_technology, process_plant, \
    process_plant_technology

from rest_framework.test import APIClient
from snowflake_drf import tasks, views
from unittest.mock import MagicMock, patch

from snowflake_drf.models import Region, Technology, PlantTechnology, Plant
import pandas as pd
from .customPermissions import set_group_memberships

log = logging.getLogger(__name__)

class SnowflakeTestCase(TestCase):
    token = None
    client = APIClient()
    fixtures = ['regions', 'plants', 'technologies', 'planttechnologies', 'planthistoricaldatas',
                'monthlyfinancialformmetrics', 'monthlyfinancialformmetricvalues', 'configurationformmtpms',
                'configurationformmtpmvalues', 'configurationformleadingindicators',
                'configurationformleadingindicatorvalues', 'annualtargetformmetricvalues']

    read_only_access_token = None
    write_access_token = None
    admin_access_token = None
    client_credentials_access_token = None

    def setUp(self):
        os.environ.setdefault('TEST_WITH_REAL_SSO', 'true')

        self.patcher1 = patch('concurrent.futures.ThreadPoolExecutor')
        self.patcher2 = patch('concurrent.futures.as_completed')
        self.mock_ThreadPoolExecutor = self.patcher1.start()
        self.mock_as_completed = self.patcher2.start()
        self.executor = MagicMock()
        self.wrapper = MagicMock()
        self.wrapper.validate_and_execute = MagicMock()
        self.convert_boolean_value = MagicMock()
        self.get_snowflake_leading_indicator_time_series = MagicMock()
        self.get_snowflake_equipment_tag_time_series = MagicMock()
        self.snowflake_build_mtpm_dim_object = MagicMock()
        self.snowflake_build_mtpm_dim_object_preferred_uom = MagicMock()
        self.get_snowflake_mtpm_time_series = MagicMock()
        self.handle_nulls = MagicMock()
        self.mock_ThreadPoolExecutor.return_value = self.executor

    @classmethod
    def setUpTestData(cls):
        os.environ['CLIENT_ID'] = '374b1465-dd0b-483b-b07e-f623e7619a12'
        os.environ['ROLE_PREFIX'] = 'DEV_CSST_MDP_Cockpit_'
        is_test_with_real_sso = os.environ.get('TEST_WITH_REAL_SSO', 'true')

        """SET TEST_WITH_REAL_SSO env variable to true in .env to be able to test everything without real sso"""
        if is_test_with_real_sso == 'false':
            cls.read_only_access_token = generate_non_sso_token(["DEV_CSST_MDP_Cockpit_Snowflake_User_Read"])
            cls.write_access_token = generate_non_sso_token(["DEV_CSST_MDP_Cockpit_User_Write"])
            cls.admin_access_token = generate_non_sso_token(["DEV_CSST_MDP_Cockpit_User_Admin"])
            cls.client_credentials_access_token = generate_client_credentials_token()
        else:
            cls.read_only_access_token = set_sso_user_token("READ")
            cls.write_access_token = set_sso_user_token("WRITE")
            cls.admin_access_token = set_sso_user_token("ADMIN")
            cls.client_credentials_access_token = set_sso_client_credential_token()

    def tearDown(self):
        self.patcher1.stop()
        self.patcher2.stop()

    def test_initial_enablement(self):
        self.assert_(True)

    def test_health_check(self):
        payload = {
            "message": "App is running"
        }

        response = self.client.get("/healthcheck/", payload, format='json')
        data = response.data
        assert data["message"] == payload["message"]
        assert response.status_code == 200

    def test_get_regions(self):
        authorization = set_auth_header(self.write_access_token)
        response = self.client.get("/forms/regions/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 2
        assert results[0]["region_name"] == "NA"

        response = self.client.get("/forms/regions/", {"region_id": "1"}, format="json", **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["region_name"] == "NA"

    def test_get_plants(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get("/forms/plants/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 3
        assert results[0]["plant_name"] == "PLANT ONE"

        response = self.client.get("/forms/plants/", {"region_id": "1"}, format='json', **authorization)
        results = self.get_results(response.content)
        sorted_results = sorted(results, key=lambda d: d['plant_name'])
        assert response.status_code == 200
        assert len(sorted_results) == 2
        assert sorted_results[1]["plant_name"] == "PLANT TWO"

    def test_get_technologies(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get("/forms/technologies/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 5
        assert results[3]["technology_name"] == "Ruby"

        response = self.client.get("/forms/technologies/", {"technology_id": "5"}, format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["technology_name"] == "GPT2"

        response = self.client.get("/forms/technologies/", {"plant_id": "PLANT3"}, format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["technology_name"] == "GPT2"

    def test_get_plant_technologies(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get("/forms/plant-technologies/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 5
        assert results[1]["plant_technology_id"] == "PLANT2DJANGO"

    def test_get_plant_historical_data(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get("/forms/plant-historical-data/", format='json', **authorization)

        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 4
        assert results[0]["plant_id"] == "PLANT1"

    def test_get_monthly_financial_form_metric(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get("/forms/monthly-financial-form-metric/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 3
        assert results[0]["uom_imperial"] == "IN"

        response = self.client.get("/forms/monthly-financial-form-metric/", {"technology_id": "1"}, format='json',
                                   **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["uom_imperial"] == "Z"

        response = self.client.get("/forms/monthly-financial-form-metric/", {"technology_id": "5"}, format='json',
                                   **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["is_numeric"] == False

    def test_get_monthly_financial_form_metric_ingestion(self):
        authorization = set_auth_header(self.client_credentials_access_token)

        response = self.client.get("/forms/ingestion/metric/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 3
        assert results[0]["uom_imperial"] == "IN"

    def test_get_monthly_financial_form_metric_value(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get("/forms/monthly-financial-form-metric-value/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 4
        assert results[0]["metric_value"] == "1.2"

        response = self.client.get("/forms/monthly-financial-form-metric-value/?monthly_financial_form_metric_id=1",
                                   format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["metric_value"] == "1.2"

        response = self.client.get("/forms/monthly-financial-form-metric-value/?monthly_financial_form_metric_id=3",
                                   format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 2
        assert results[0]["metric_value"] == "Test Material"   

    def test_get_monthly_financial_form_metric_value_by_date(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get(
            "/forms/monthly-financial-form-metric-value/?value_timestamp_local=2022-12-07 20:47:42",
            format='json',
            **authorization)
        results = self.get_results(response.content)
        assert len(results) == 3
        assert results[0]["metric_value"] == "1.2"
        assert response.status_code == 200

        response = self.client.get(
            "/forms/monthly-financial-form-metric-value/?value_timestamp_local=2022-05-08 20:47:43",
            format='json',
            **authorization)
        results = self.get_results(response.content)
        assert len(results) == 0

        response = self.client.get(
            "/forms/monthly-financial-form-metric-value/?monthly_financial_form_metric_id=3&value_timestamp_local=2022-12-07 20:47:42",
            format='json', **authorization)
        results = self.get_results(response.content)
        sorted_results = sorted(results, key=lambda d: d['monthly_financial_form_metric_value_id'])
        assert response.status_code == 200
        assert len(results) == 2
        assert sorted_results[1]["metric_value"] == "Test Material 2"

        response = self.client.get("/forms/monthly-financial-form-metric-value/?plant_technology_id=PLANT2DJANGO",
                                   format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["metric_value"] == "1.2"

        # testing filter logic for form
        response = self.client.get(
            "/forms/monthly-financial-form-metric-value/?plant_technology_id=PLANT1PYTHON&value_timestamp_utc=2022-12-08 20:47:43",
            format='json',
            **authorization)
        results = self.get_results(response.content)
        assert len(results) == 1

        response = self.client.get(
            "/forms/monthly-financial-form-metric-value/?plant_technology_id=PLANT1PYTHON&value_timestamp_utc=2023-01-09 20:47:43",
            format='json',
            **authorization)
        results = self.get_results(response.content)
        assert len(results) == 1

        response = self.client.get(
            "/forms/monthly-financial-form-metric-value/?value_timestamp_utc=2022-11-15 20:47:43",
            format='json',
            **authorization)
        results = self.get_results(response.content)
        assert len(results) == 0

    def test_post_financial_data_prev_month(self):
        prev_month_date = date.today() + relativedelta(months=-2)
        authorization = set_auth_header(self.write_access_token)
        response = self.client.post("/forms/monthly-financial-form-metric-value/", {
            "monthly_financial_form_metric_id": "2",
            "plant_technology_path": "PATHPATH",
            "value_timestamp_utc": prev_month_date,
            "value_timestamp_local": prev_month_date,
            "metric_value": "3.8",
            "datetime_created": "2023-10-30 13:28:27",
            "datetime_updated": "2023-10-30 13:28:27",
            "user_created": "postgres",
            "user_updated": "postgres",
            "plant_technology_id": "PLANT1PYTHON",
            "is_numeric": "true"
        }, format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 201 
        assert results is not None

    def test_post_financial_data_last_month(self):
        last_month_date = date.today() + relativedelta(months=-1)
        authorization = set_auth_header(self.write_access_token)
        response = self.client.post("/forms/monthly-financial-form-metric-value/", {
            "monthly_financial_form_metric_id": "2",
            "plant_technology_path": "PATHPATH",
            "value_timestamp_utc": last_month_date,
            "value_timestamp_local": last_month_date,
            "metric_value": "3.8",
            "datetime_created": "2023-10-30 13:28:27",
            "datetime_updated": "2023-10-30 13:28:27",
            "user_created": "postgres",
            "user_updated": "postgres",
            "plant_technology_id": "PLANT1PYTHON",
            "is_numeric": "true"
        }, format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 201 
        assert results is not None

    def test_post_financial_data_current_month(self):
        current_date = date.today()
        authorization = set_auth_header(self.write_access_token)
        response = self.client.post("/forms/monthly-financial-form-metric-value/", {
            "monthly_financial_form_metric_id": "2",
            "plant_technology_path": "PATHPATH",
            "value_timestamp_utc": current_date,
            "value_timestamp_local": current_date,
            "metric_value": "3.8",
            "datetime_created": "2023-10-30 13:28:27",
            "datetime_updated": "2023-10-30 13:28:27",
            "user_created": "postgres",
            "user_updated": "postgres",
            "plant_technology_id": "PLANT1PYTHON",
            "is_numeric": "true"
        }, format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 201 
        assert results is not None

    def test_patch_financial_data_prev_month(self):
        prev_month_date = date.today() + relativedelta(months=-2)
        authorization = set_auth_header(self.write_access_token)
        response = self.client.patch("/forms/monthly-financial-form-metric-value/2/", {
            "metric_value": "3.8",
            "value_timestamp_utc": prev_month_date,
            "datetime_updated": "2023-10-30 13:28:27",
            "user_updated": "luis",
        }, format='json', content_type='application/json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200 
        assert results is not None

    def test_patch_financial_data_last_month(self):
        prev_month_date = date.today() + relativedelta(months=-1)
        authorization = set_auth_header(self.write_access_token)
        response = self.client.patch("/forms/monthly-financial-form-metric-value/2/", {
            "metric_value": "3.8",
            "value_timestamp_utc": prev_month_date,
            "datetime_updated": "2023-10-30 13:28:27",
            "user_updated": "luis",
        }, format='json', content_type='application/json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200 
        assert results is not None

    def test_patch_financial_data_current_month(self):
        current_month_date = date.today()
        authorization = set_auth_header(self.write_access_token)
        response = self.client.patch("/forms/monthly-financial-form-metric-value/2/", {
            "metric_value": "3.8",
            "value_timestamp_utc": current_month_date,
            "datetime_updated": "2023-10-30 13:28:27",
            "user_updated": "luis",
        }, format='json', content_type='application/json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200 
        assert results is not None

    def test_get_annual_target_form_with_valid_date(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get(
            "/forms/annual-target-form-metric-value/?value_timestamp_utc=2022-12-07 20:37:59.745833 %2B06:00",
            format='json',
            **authorization)
        results = self.get_results(response.content)
        assert len(results) == 1
        assert response.status_code == 200

    def test_get_annual_target_form_with_naive_date(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get(
            "/forms/annual-target-form-metric-value/?value_timestamp_utc=2022-12-07",
            format='json',
            **authorization)
        results = self.get_results(response.content)
        assert len(results) == 1
        assert response.status_code == 200

    def test_get_annual_target_form_with_invalid_date(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get(
            "/forms/annual-target-form-metric-value/?value_timestamp_utc=2022-11-10 20:38:59.745833 %2B06:00",
            format='json',
            **authorization)
        results = self.get_results(response.content)
        assert len(results) == 0
        assert response.status_code == 200

    def test_get_annual_target_form_with_date_and_fallback(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get(
            "/forms/annual-target-form-metric-value/?value_timestamp_utc=2023-01-01 20:39:59.745833 %2B06:00",
            format='json',
            **authorization)
        results = self.get_results(response.content)
        log.debug("results %s", results)
        assert len(results) == 1
        assert response.status_code == 200

    def test_get_monthly_financial_form_metric_value_by_plant_technology(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get("/forms/monthly-financial-form-metric-value/?plant_technology_id=PLANT2DJANGO",
                                   format='json',
                                   **authorization)
        results = self.get_results(response.content)
        assert len(results) == 1
        assert results[0]["metric_value"] == "1.2"
        assert response.status_code == 200

        response = self.client.get("/forms/monthly-financial-form-metric-value/?plant_technology_id=PLANT1PYTHON",
                                   format='json',
                                   **authorization)
        results = self.get_results(response.content)
        assert len(results) == 1

    def test_get_configuration_form_mtpm(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get("/forms/configuration-form-mtpm/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["mtpm_name"] == "MTPM_NAME"

        response = self.client.get("/forms/configuration-form-mtpm/", {"plant_technology_id": "PLANT1PYTHON"},
                                   format='json',
                                   **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["mtpm_name"] == "MTPM_NAME"

        response = self.client.get("/forms/configuration-form-mtpm-value/?plant_technology_id=PLANT1PYTHON",
                                   format='json',
                                   **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["display_high"] == 999.99

    def test_get_configuration_form_mtpm_value(self):
        authorization = set_auth_header(self.write_access_token)
        response = self.client.get("/forms/configuration-form-mtpm-value/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["display_high"] == 999.99

        response = self.client.get("/forms/configuration-form-mtpm-value/?configuration_form_mtpm_id=1",
                                   format='json',
                                   **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["display_high"] == 999.99

    def test_get_configuration_form_leading_indicator(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get("/forms/configuration-form-leading-indicator/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["uom_metric"] == "CMPM"

        response = self.client.get("/forms/configuration-form-leading-indicator/", {"configuration_form_mtpm_id": "1"},
                                   format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["uom_metric"] == "CMPM"

        response = self.client.get("/forms/configuration-form-leading-indicator/", {"plant_technology_id": "PLANT1PYTHON"},
                                   format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["uom_metric"] == "CMPM"

    def test_get_configuration_form_leading_indicator_value(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get("/forms/configuration-form-leading-indicator-value/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["corrective_action"] == "decontaminate"

        response = self.client.get(
            "/forms/configuration-form-leading-indicator-value/?configuration_form_leading_indicator_id=1",
            format='json',
            **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["corrective_action"] == "decontaminate"

        response = self.client.get("/forms/configuration-form-leading-indicator-value/?plant_technology_id=PLANT2RUBY",
                                   format='json',
                                   **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["corrective_action"] == "decontaminate"

    def test_get_annual_target_form_metric_value(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get(
            "/forms/annual-target-form-metric-value/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 2

        response = self.client.get(
            "/forms/annual-target-form-metric-value/?plant_technology_id=PLANT2RUBY", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 2

        response = self.client.get(
            "/forms/annual-target-form-metric-value/?configuration_form_mtpm_id=1", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 2

        response = self.client.get(
            "/forms/annual-target-form-metric-value/?annual_target_form_metrics_id=1", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 2
        assert results[0]["plant_technology_id"] == "PLANT2RUBY"

        response = self.client.get(
            "/forms/annual-target-form-metric-value/?value_timestamp_local=2022-12-07 20:36:59", format='json',
            **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert len(results) == 1
        assert results[0]["plant_technology_id"] == "PLANT2RUBY"

    def test_snowflake_regions(self):
        authorization = set_auth_header(self.write_access_token)

        views.plant_data = MagicMock()
        views.plant_data.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.get("/snowflake/regions/", format='json', **authorization)
        results = self.get_results(response.content)

        assert response.status_code == 200
        assert results is not None

    def test_snowflake_regions_parameter(self):
        authorization = set_auth_header(self.write_access_token)

        views.plant_data = MagicMock()
        views.plant_data.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.get("/snowflake/regions/", {"region_id": "EMEA"}, format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_plants(self):
        authorization = set_auth_header(self.write_access_token)

        views.plant_data = MagicMock()
        views.plant_data.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.get("/snowflake/plants/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_plants_parameter(self):
        authorization = set_auth_header(self.write_access_token)

        views.plant_data = MagicMock()
        views.plant_data.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.get("/snowflake/plants/", {"region_id": "NA"}, format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_plants_optional_parameter(self):
        authorization = set_auth_header(self.write_access_token)

        views.plant_data = MagicMock()
        views.plant_data.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.get("/snowflake/plants/",
                                   {"region_id": "EMEA", "plant_id": "0171cf35-6bf7-11ed-b70c-1cc10cb4aa68"},
                                   format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_plant_technologies(self):
        authorization = set_auth_header(self.write_access_token)

        views.plant_data = MagicMock()
        views.plant_data.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.get("/snowflake/plants/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_plant_technologies_parameter(self):
        authorization = set_auth_header(self.write_access_token)

        views.plant_data = MagicMock()
        views.plant_data.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.get("/snowflake/plants/", {"plant_id": "9d83daf8-6b78-11ed-b70c-1cc10cb4aa68"},
                                   format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_plant_technologies_optional_parameter(self):
        authorization = set_auth_header(self.write_access_token)

        views.plant_data = MagicMock()
        views.plant_data.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.get("/snowflake/plants/",
                                   {"plant_technology_id": "9d83db04-6b78-11ed-b70c-1cc10cb4aa68"}, format='json',
                                   **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_mtpm_list(self):
        authorization = set_auth_header(self.write_access_token)

        views.mtpms = MagicMock()
        views.mtpms.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.get("/snowflake/mtpm/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_mtpm_list_parameter(self):
        authorization = set_auth_header(self.write_access_token)

        views.mtpms = MagicMock()
        views.mtpms.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.get("/snowflake/mtpm/", {"plant_technology_id": "0171c2b7-6bf7-11ed-b70c-1cc10cb4aa68"},
                                   format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_mtpm_id_opportunity_post(self):
        authorization = set_auth_header(self.client_credentials_access_token)

        views.mtpms = MagicMock()
        views.mtpms.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.post("/snowflake/mtpm/metrics/opportunity/", {'mtpm': ['ID1R1P1T1MTPM1', 'ID1R1P1T1MTPM2', 'ID1R1P1T1MTPM3', '4ff9067b-4f2a-11ed-b701-1cc10cb4aa68'],
                                                         "datetimestart": "2023-01-01", "datetimeend": "2023-01-12",
                                                         "plant_technology_id": "5b5fbd07-4b28-11ed-b700-1cc10cb4aa68"},
                                    format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_mtpm_id_post(self):
        authorization = set_auth_header(self.client_credentials_access_token)

        views.mtpms = MagicMock()
        views.mtpms.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.post("/snowflake/mtpm/metrics/", {'mtpm': ['ID1R1P1T1MTPM1', 'ID1R1P1T1MTPM2', 'ID1R1P1T1MTPM3', '4ff9067b-4f2a-11ed-b701-1cc10cb4aa68'],
                                                         "datetimestart": "2023-01-01", "datetimeend": "2023-01-12",
                                                         "plant_technology_id": "5b5fbd07-4b28-11ed-b700-1cc10cb4aa68"},
                                    format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_mtpm_ui_level_post(self):
        authorization = set_auth_header(self.client_credentials_access_token)

        views.mtpms = MagicMock()
        views.mtpms.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.post("/snowflake/mtpm/metrics/",
                                    {"ui_level": '1', "datetimestart": "2022-12-01", "datetimeend": "2023-02-01", "plant_technology_id": "5b5fbd07-4b28-11ed-b700-1cc10cb4aa68"},
                                    format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

        response = self.client.post("/snowflake/mtpm/metrics/",
                                    {"ui_level": '1', "datetimestart": "2023-02-02 12:00:00", "datetimeend": "2023-02-09 12:00:00", "plant_technology_id": "419d77de-4b28-11ed-b700-1cc10cb4aa68"},
                                    format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_mtpm_id_post_metric(self):
        authorization = set_auth_header(self.client_credentials_access_token)

        views.mtpms = MagicMock()
        views.mtpms.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.post("/snowflake/mtpm/metrics/", {"mtpm": ["6a4b28ac-4e81-11ed-b701-1cc10cb4aa68",
                                                                          "6531ce65-4f21-11ed-b701-1cc10cb4aa68",
                                                                          "6531ce56-4f21-11ed-b701-1cc10cb4aa68",
                                                                          "4ff9067b-4f2a-11ed-b701-1cc10cb4aa68",
                                                                          "87c2b54d-5fa3-11ed-b708-f4ee08e53170",
                                                                          "6531ce50-4f21-11ed-b701-1cc10cb4aa68",
                                                                          "a90ec2b0-5fa3-11ed-b708-f4ee08e53170",
                                                                          "6531ce53-4f21-11ed-b701-1cc10cb4aa68"],
                                                                 "plant_technology_id": "5b5fbd07-4b28-11ed-b700-1cc10cb4aa68",
                                                                 "preferred_uom": "metric",
                                                                 "datetimestart": "2022-12-01",
                                                                 "datetimeend": "2023-01-01"},
                                    format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_mtpm_id_post_imperial(self):
        authorization = set_auth_header(self.client_credentials_access_token)

        views.mtpms = MagicMock()
        views.mtpms.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.post("/snowflake/mtpm/metrics/", {'mtpm': ['ID1R1P1T1MTPM1', 'ID1R1P1T1MTPM2', 'ID1R1P1T1MTPM3', '4ff9067b-4f2a-11ed-b701-1cc10cb4aa68'],
                                                         "datetimestart": "2023-01-01", "datetimeend": "2023-01-12",
                                                         "plant_technology_id": "5b5fbd07-4b28-11ed-b700-1cc10cb4aa68",
                                                         "preferred_uom": "imperial"},
                                    format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_mtpm_id_missing_required_data(self):
        authorization = set_auth_header(self.client_credentials_access_token)

        views.mtpms = MagicMock()
        views.mtpms.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.post("/snowflake/mtpm/metrics/", {'mtpm': ['ID1R1P1T1MTPM1', 'ID1R1P1T1MTPM2', 'ID1R1P1T1MTPM3', '4ff9067b-4f2a-11ed-b701-1cc10cb4aa68'],
                                                         "datetimestart": "2023-01-01", "datetimeend": "2023-01-12",
                                                         "preferred_uom": "metric"},
                                    format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results == "Missing the plant_technology_id filter"

        response = self.client.post("/snowflake/mtpm/metrics/", {"datetimestart": "2023-01-01", "datetimeend": "2023-01-12",
                                                         "plant_technology_id": "5b5fbd07-4b28-11ed-b700-1cc10cb4aa68",
                                                         "preferred_uom": "metric"},
                                    format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results == "Missing the list of mtpm ids or a ui_level filter"

        response = self.client.post("/snowflake/mtpm/metrics/", {'mtpm': ['ID1R1P1T1MTPM1', 'ID1R1P1T1MTPM2', 'ID1R1P1T1MTPM3', '4ff9067b-4f2a-11ed-b701-1cc10cb4aa68'],
                                                         "datetimestart": "2023-01-01",
                                                         "plant_technology_id": "5b5fbd07-4b28-11ed-b700-1cc10cb4aa68",
                                                         "preferred_uom": "metric"},
                                    format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results == "Datetime fields are required"

    def test_snowflake_mtpm_ui_level_post_imperial(self):
        authorization = set_auth_header(self.client_credentials_access_token)

        views.mtpms = MagicMock()
        views.mtpms.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.post("/snowflake/mtpm/metrics/",
                                    {"ui_level": '1', "datetimestart": "2022-12-01", "datetimeend": "2023-02-01", "plant_technology_id": "5b5fbd07-4b28-11ed-b700-1cc10cb4aa68",
                                    "preferred_uom": "imperial"},
                                    format='json', **authorization)

        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_mtpm_leadingindicators(self):
        authorization = set_auth_header(self.client_credentials_access_token)

        views.leading_indicators = MagicMock()
        views.leading_indicators.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.get("/snowflake/mtpm/leadingindicators/",
                                    {"mtpm_id": '581b8150-6b76-11ed-b70c-1cc10cb4aa68', "datetimestart": "2022-12-30", "datetimeend": "2023-01-11"},
                                    format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    # def get_results(self, content):
    #     decoded = content.decode("UTF-8")
    #     return ast.literal_eval(decoded)
    def get_results(self, content):
        log.debug("content: %s", content)
        return json.loads(content.decode())["results"]

    @patch('snowflake_drf.task_data_import.login')
    @patch('snowflake_drf.task_data_import.download_flow')
    def test_celery_task_element_unify(self, mock_download_flow, mock_login):
        msg = tasks.checks_for_updates_on_element_unify()
        self.assertEquals(msg, "Element data processed.")

    def test_element_unify_data_import_process_region(self):
        data = {'region_id': ["EMEA", "NA"], 'region_name': ["EMEA", "NA"]}
        region_list = pd.DataFrame.from_dict(data)

        process_region(region_list)
        objects = Region.objects.all()
        self.assertEquals(2, len(objects))

    def test_element_unify_data_import_process_technology(self):
        data = {'technology_id': ["WHEATAREA", "CORNWETMILL"], 'technology_name': ["Wheat Area", "Corn Wet Mill"]}
        technology_list = pd.DataFrame.from_dict(data)

        process_technology(technology_list)
        objects = Technology.objects.filter(technology_name="Wheat Area").all()
        self.assertEquals(1, len(objects))

    def test_element_unify_data_import_process_plant(self):
        data = {'plant_id': ["91a52305-6bf7-11ed-b70c-1cc10cb4aa68", "0171c2b1-6bf7-11ed-b70c-1cc10cb4aa68"],
                'plant_name': ["Sim Plant 105", "Sim Plant 090"],
                'reporting_day_start': ["3", "6"],
                'timezone': ["Europe/Amsterdam", "America/Chicago"],
                'utc_offset': ["1", "-7"],
                'region': ["EMEA", "NA"],
                'plant_path': ["\\MSUSAMEACP3307.na.corp.cargill.com\CSST PDDF MTPM\Sim Plant 105",
                               "\\MSUSAMEACP3307.na.corp.cargill.com\CSST PDDF MTPM\Sim Plant 090"]}
        plant_list = pd.DataFrame.from_dict(data)

        process_plant(plant_list)
        objects = Plant.objects.filter(plant_name="Sim Plant 105").all()
        self.assertEquals(1, len(objects))

    def test_element_unify_data_import_process_plant_technology(self):
        data = {'technology_id': ["WHEATAREA", "CORNWETMILL"], 'technology_name': ["Wheat Area", "Corn Wet Mill"]}
        technology_list = pd.DataFrame.from_dict(data)
        process_technology(technology_list)

        data = {'plant_id': ["8304d559-4a3b-11ed-b700-1cc10cb4aa68", "e7b4fb0f-4a3b-11ed-b700-1cc10cb4aa68"],
                'plant_name': ["Sim Plant 105", "Sim Plant 090"],
                'reporting_day_start': ["3", "6"],
                'timezone': ["Europe/Amsterdam", "America/Chicago"],
                'utc_offset': ["1", "-7"],
                'region': ["EMEA", "NA"],
                'plant_path': ["\\MSUSAMEACP3307.na.corp.cargill.com\CSST PDDF MTPM\Sim Plant 105",
                               "\\MSUSAMEACP3307.na.corp.cargill.com\CSST PDDF MTPM\Sim Plant 090"]}
        plant_list = pd.DataFrame.from_dict(data)
        process_plant(plant_list)

        data = {'plant_technology_id': ["419d77de-4b28-11ed-b700-1cc10cb4aa68", "5b5fbd07-4b28-11ed-b700-1cc10cb4aa68"],
                'plant_technology_path': [
                    "\\MSUSAMEACP3307.na.corp.cargill.com\CSST PDDF MTPM\Bergen op Zoom\_Reporting\Wheat Area",
                    "\\MSUSAMEACP3307.na.corp.cargill.com\CSST PDDF MTPM\Fort Dodge\_Reporting\Corn Wet Mill"],
                'technology_id': ["WHEATAREA", "CORNWETMILL"],
                'plant_id': ["8304d559-4a3b-11ed-b700-1cc10cb4aa68", "e7b4fb0f-4a3b-11ed-b700-1cc10cb4aa68"],
                'support_daily_financial_entries': ['False', 'False']}
        plant_technology_list = pd.DataFrame.from_dict(data)
        process_plant_technology(plant_technology_list)

        objects = PlantTechnology.objects.filter(plant_technology_id="419d77de-4b28-11ed-b700-1cc10cb4aa68")
        self.assertEquals(1, len(objects))

    def test_date_range(self):
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 10)

        result = list(date_range(start_date, end_date))

        expected_result = [
            (datetime(2023, 1, 1), datetime(2023, 1, 4)),
            (datetime(2023, 1, 4), datetime(2023, 1, 7)),
            (datetime(2023, 1, 7), datetime(2023, 1, 10)),
        ]

        self.assertEqual(result, expected_result)

    def test_convert_boolean_value(self):
        value = 1
        methods = SnowflakeMethods()
        result = methods.convert_boolean_value(value)

        self.assertEqual(result, 1)

    def test_convert_boolean_value_none_value(self):
        value = None
        methods = SnowflakeMethods()
        result = methods.convert_boolean_value(value)

        self.assertEqual(result, None)

    def test_handle_nulls(self):
        value = 'test'
        methods = SnowflakeMethods()
        result = methods.handle_nulls(value)

        self.assertEqual(result, 'test')

    def test_handle_nulls_none_value(self):
        value = None
        methods = SnowflakeMethods()
        result = methods.handle_nulls(value)

        self.assertEqual(result, '')


    @patch.object(LeadingIndicators, 'get_snowflake_leading_indicator_metric')
    def test_get_snowflake_leading_indicator_metric_data(self, leading_indicator_data):
        mtpm_id = 1
        date_time_start = '2023-01-01'
        date_time_end = '2023-01-10'
        db_schema = f'{os.getenv("SNOWFLAKE_DATABASE")}.{os.getenv("SNOWFLAKE_SCHEMA")}'
        leading_indicator_data.return_value = [('1234-abc-1234-abcd-1234', 'test leading_indicator_name',
                           'test leading_indicator_display_name', 0, 0, 'test corrective_action', None, 'test cmo',
                           Decimal('100.00000'), Decimal('0.00000'), Decimal('100.00000'), Decimal('0.00000'),
                           Decimal('20.50000'), Decimal('17.00000'), Decimal('20.50000'), Decimal('17.00000'), 'Be',
                           'Be', '%', None, None, None, None, 1, ''), [(0.15499999932944775,
                            0.00004094666793837184, "2023-03-15T09:35:55", "2023-03-15T04:35:55", None)]]

        methods = LeadingIndicators()
        methods.wrapper = self.wrapper
        records = methods.get_snowflake_leading_indicator_metric_data('74b3f612-4fd6-11ed-b702-f4ee08e53170', '', date_time_start, date_time_end)

        self.assertIsInstance(records, list)
        self.assertGreater(len(records), 0)

        for record in records:
            self.assertIn('leading_indicator_id', record)
            self.assertIn('time_series', record)
            self.assertIsInstance(record['leading_indicator_id'], str)
            self.assertIsInstance(record['time_series'], list)

    @patch.object(LeadingIndicators, 'get_snowflake_leading_indicator_metric')
    def test_get_snowflake_leading_indicator_metric_data_with_leading_indicator_id(self, leading_indicator_data):
        leading_indicator_id = 1
        mtpm_id = 1
        date_time_start = '2023-01-01T00:00:00'
        date_time_end = '2023-01-10T00:00:00'

        db_schema = f'{os.getenv("SNOWFLAKE_DATABASE")}.{os.getenv("SNOWFLAKE_SCHEMA")}'
        leading_indicator_data.return_value = [('1234-abc-1234-abcd-1234', 'test leading_indicator_name',
                           'test leading_indicator_display_name', 0, 0, 'test corrective_action', None, 'test cmo',
                           Decimal('100.00000'), Decimal('0.00000'), Decimal('100.00000'), Decimal('0.00000'),
                           Decimal('20.50000'), Decimal('17.00000'), Decimal('20.50000'), Decimal('17.00000'), 'Be',
                           'Be', '%', None, None, None, None, 1, ''), [(0.15499999932944775,
                            0.00004094666793837184, "2023-03-15T09:35:55", "2023-03-15T04:35:55", None)]]

        methods = LeadingIndicators()
        methods.wrapper = self.wrapper
        records = methods.get_snowflake_leading_indicator_metric_data(leading_indicator_id, mtpm_id, date_time_start,
                                                                               date_time_end)
        self.assertIsInstance(records, list)
        self.assertGreater(len(records), 0)

        for record in records:
            self.assertIn('leading_indicator_id', record)
            self.assertIn('time_series', record)
            self.assertIsInstance(record['leading_indicator_id'], str)
            self.assertIsInstance(record['time_series'], list)

    @patch.object(LeadingIndicators, 'get_snowflake_leading_indicator_metric')
    def test_get_snowflake_leading_indicator_metric_data_with_invalid_dates(self, metric_data):
        mtpm_id = 1
        date_time_start = 'not a date'
        date_time_end = '2023-01-10'

        methods = LeadingIndicators()
        methods.wrapper = self.wrapper
        with self.assertRaises(ValueError):
            methods.get_snowflake_leading_indicator_metric_data('', mtpm_id, date_time_start, date_time_end)

    def test_get_snowflake_region_dim_data(self):
        region_id = 'test_region'

        methods = PlantData()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [('test_region', 'test_region')]

        results = methods.get_snowflake_region_dim_data(region_id)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)

        for result in results:
            self.assertIn('region_id', result)
            self.assertIn('region_name', result)

    def test_get_snowflake_plant_dim_data(self):
        plant_id = '123-abc-123-abc-1234'
        region_id = 'test_region'

        methods = PlantData()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [('123-abc-123-abc-1234', 'test_region',
                                                           'test_plant', 'test_region')]
        results = methods.get_snowflake_plant_dim_data(region_id, plant_id)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)

        for result in results:
            self.assertIn('plant_id', result)
            self.assertIn('region_id', result)
            self.assertIn('plant_name', result)
            self.assertIn('region_name', result)

    def test_get_snowflake_plant_technology_dim_data(self):
        plant_id = '123-abc-123-abc-1234'
        plant_technology_id = '321-abc-321-abc-4321'

        methods = PlantData()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [('321-abc-321-abc-4321', '123-abc-123-abc-1234',
                                                           'test_technology', 'test_plant', 'test_plant_technology_path')]

        results = methods.get_snowflake_plant_technology_dim_data(plant_id, plant_technology_id)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)

        for result in results:
            self.assertIn('plant_technology_id', result)
            self.assertIn('plant_id', result)
            self.assertIn('technology_name', result)
            self.assertIn('plant_name', result)
            self.assertIn('plant_technology_path', result)

    def test_get_snowflake_plant_technology_dim_data_plant_technology_id_only(self):
        plant_technology_id = '321-abc-321-abc-4321'

        methods = PlantData()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [('321-abc-321-abc-4321', '123-abc-123-abc-1234', 'test_technology',
                                           'test_plant', 'test_plant_technology_path')]

        results = methods.get_snowflake_plant_technology_dim_data('', plant_technology_id)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)

        for result in results:
            self.assertIn('plant_technology_id', result)
            self.assertIn('plant_id', result)
            self.assertIn('technology_name', result)
            self.assertIn('plant_name', result)
            self.assertIn('plant_technology_path', result)

    def test_get_snowflake_mtpm_dim_data(self):
        plant_technology_id = '321-abc-321-abc-4321'

        methods = Mtpms()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [('123-abc-123-abc-1234', 'test_mtpm_name',
                                           'test_mtpm_display_name', 'test_area_name', 0, 0, 0, '2',
                                           Decimal('12.99000'),
                                           Decimal('12.99000'), Decimal('15.00000'), 0, 0, '%', 
                                           Decimal('11.80000'), Decimal('11.80000'),
                                           Decimal('11.80000'), Decimal('11.80000'), Decimal('11.80000'), Decimal('11.80000'), Decimal('11.80000'), 'PB', 0)]

        results = methods.get_snowflake_mtpm_dim_data(plant_technology_id)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)

        for result in results:
            self.assertIn('mtpm_id', result)
            self.assertIn('plant_id', result)
            self.assertIn('plant_technology_id', result)
            self.assertIn('mtpm_name', result)
            self.assertIn('mtpm_display_name', result)
            self.assertIn('target_type', result)

    @patch.object(Mtpms, 'get_mtpm_dim_results')
    @patch.object(Mtpms, 'get_mtpm_ts_results')
    def test_get_snowflake_mtpm_target_data(self, ts_results, mtpm_results):
        plant_technology_id = '321-abc-321-abc-4321'
        mtpm_list = 'test_mtpm_list'
        datetimestart = '2022-12-16 12:00:00'
        datetimeend = '2023-01-16'
        preferred_uom = 'test_preferred_uom'

        methods = Mtpms()
        
        mtpm_results.return_value = [('123-abc-123-abc-1234', 'test_mtpm_name',
                                           'test_mtpm_display_name', 'test_area_name', 0, 0, 0, '2',
                                           Decimal('12.99000'),
                                           Decimal('12.99000'), Decimal('15.00000'), 0, 0, '%', 
                                           Decimal('11.80000'), Decimal('11.80000'),
                                           Decimal('11.80000'), Decimal('11.80000'), Decimal('11.80000'), Decimal('11.80000'), Decimal('11.80000'), 'PB', 0)]
        ts_results.return_value = [(Decimal('12.99000'), datetime(2022, 12, 16, 12, 8), Decimal('12.99000'), Decimal('12.99000'), Decimal('12.99000'))]
        methods.wrapper = self.wrapper
        results = methods.get_snowflake_mtpm_target_data_generic(plant_technology_id, datetimestart,
                                                                  datetimeend,
                                                                  preferred_uom, mtpm_list)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)

        for result in results:
            self.assertIn('mtpm_id', result)
            self.assertIn('mtpm_name', result)
            self.assertIn('mtpm_display_name', result)
            self.assertIn('target_type', result)

    def test_get_snowflake_mtpm_target_data_no_preferred_uom(self):
        plant_technology_id = '321-abc-321-abc-4321'
        mtpm_list = 'test_mtpm_list'
        datetimestart = '2022-12-16 12:00:00'
        datetimeend = '2023-01-16'

        methods = Mtpms()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [('123-abc-123-abc-1234', 'test_mtpm_name',
                                           'test_mtpm_display_name', 'test_area_name', 0, 0, 0, '2',
                                           Decimal('12.99000'),
                                           Decimal('12.99000'), Decimal('15.00000'), Decimal('15.00000'),
                                           Decimal('25.50000'), Decimal('25.50000'), Decimal('15.00000'),
                                           Decimal('25.50000'), Decimal('25.50000'), 0, 0, Decimal('25.50000'), Decimal('25.50000'), Decimal('15.00000'),
                                           helpers.get_datetime_obj('2023-02-09 06:00:00'), Decimal('25.50000'),
                                           '%', '%', 400, 10, 700, 1, 1000, 9, 2000, 20, 'targ_type')]
        results = methods.get_snowflake_mtpm_target_data_generic(plant_technology_id, datetimestart,
                                                                  datetimeend, None, mtpm_list)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)

        for result in results:
            self.assertIn('mtpm_id', result)
            self.assertIn('mtpm_name', result)
            self.assertIn('mtpm_display_name', result)

    def test_get_snowflake_mtpm_target_data_ui_level(self):
        ui_level = '1'
        plant_technology_id = '321-abc-321-abc-4321'
        datetimestart = '2022-12-16 12:00:00'
        datetimeend = '2023-01-16'

        methods = Mtpms()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [('123-abc-123-abc-1234', 'test_mtpm_name',
                                           'test_mtpm_display_name', 'test_area_name', 0, 0, 0, '2',
                                           Decimal('12.99000'),
                                           Decimal('12.99000'), Decimal('15.00000'), Decimal('15.00000'),
                                           Decimal('25.50000'), Decimal('25.50000'), Decimal('15.00000'),
                                           Decimal('25.50000'), Decimal('25.50000'), 0, 0, Decimal('25.50000'), Decimal('25.50000'), Decimal('15.00000'),
                                           helpers.get_datetime_obj('2023-02-09 06:00:00'), Decimal('25.50000'),
                                           '%', '%', 400, 10, 700, 1, 1000, 9, 2000, 20, 'targ_type')]
        results = methods.get_snowflake_mtpm_target_data_generic(plant_technology_id,
                                                                           datetimestart,
                                                                           datetimeend, None, None, ui_level)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)

        for result in results:
            self.assertIn('mtpm_id', result)
            self.assertIn('mtpm_name', result)
            self.assertIn('mtpm_display_name', result)
    
    def test_get_snowflake_mtpm_query(self):
        mtpm_list = 'test_mtpm_list'
        plant_technology_id = '321-abc-321-abc-4321'
        datetimestart = '2022-12-16 12:00:00'
        datetimeend = '2023-01-16'
        preferred_uom = 'metric'

        methods = Mtpms()
        result = methods.build_mtpm_query_preferred_uom(plant_technology_id,
                                                                           datetimestart,
                                                                           datetimeend, preferred_uom, mtpm_list)
        self.assertIsInstance(result, str)

    def test_get_snowflake_ts_query(self):
        mtpm_list = 'test_mtpm_list'
        plant_technology_id = '321-abc-321-abc-4321'
        datetimestart = '2022-12-16 12:00:00'
        datetimeend = '2023-01-16'
        preferred_uom = 'metric'

        methods = Mtpms()
        result = methods.build_mtpm_time_series_query(datetimestart, datetimeend, preferred_uom, mtpm_list)
        self.assertIsInstance(result, str)
    
    def test_get_snowflake_target_query(self):
        mtpm_list = 'test_mtpm_list'
        plant_technology_id = '321-abc-321-abc-4321'
        datetimestart = '2022-12-16 12:00:00'
        datetimeend = '2023-01-16'
        preferred_uom = 'metric'

        methods = Mtpms()
        result = methods.build_mtpm_target_query(preferred_uom, mtpm_list, datetimestart, datetimeend)
        self.assertIsInstance(result, str)
    
    def test_build_mtpm_ts_data_with_target(self):
        ts_results = [(Decimal('12.99000'), datetime(2023, 3, 16, 12, 8))]
        target_results = [('2023-02-01 06:00:00.000', Decimal('11'), 'Budget'), ('2023-02-10 06:00:00.000', Decimal('12'), 'LFY'),
                            ('2023-03-15 06:00:00.000', Decimal('14'), 'PB'),
                            ('2023-03-17 06:00:00.000', Decimal('13'), 'PB')]

        methods = Mtpms()
        result = methods.get_mtpm_time_series_preferred_uom(ts_results, target_results, None, None, None, True)
        self.assertEqual(Decimal('11'), result[0][0]['target_value_budget'])
        self.assertEqual(Decimal('12'), result[0][0]['target_value_lfy'])
        self.assertEqual(Decimal('14'), result[0][0]['target_value_pb'])
    
    def test_build_mtpm_ts_data_with_target_out_of_order(self):
        ts_results = [(Decimal('12.99000'), datetime(2023, 3, 16, 12, 8))]
        target_results = [('2023-02-01 06:00:00.000', Decimal('11'), 'Budget'), ('2023-02-10 06:00:00.000', Decimal('12'), 'LFY'),
                            ('2023-03-17 06:00:00.000', Decimal('13'), 'PB'),
                            ('2023-03-15 06:00:00.000', Decimal('14'), 'PB')]

        methods = Mtpms()
        result = methods.get_mtpm_time_series_preferred_uom(ts_results, target_results, None, None, None, True)
        self.assertEqual(Decimal('11'), result[0][0]['target_value_budget'])
        self.assertEqual(Decimal('12'), result[0][0]['target_value_lfy'])
        self.assertNotEqual(Decimal('14'), result[0][0]['target_value_pb'])

    def test_build_mtpm_ts_data_with_target_varying_data(self):
        ts_results = [(Decimal('12.99000'), datetime(2023, 6, 16, 12, 8)),(Decimal('12.99000'), datetime(2023, 6, 17, 12, 8)),(Decimal('12.99000'), datetime(2023, 6, 18, 12, 8)),
            (Decimal('12.99000'), datetime(2023, 6, 19, 12, 8)),(Decimal('12.99000'), datetime(2023, 6, 20, 12, 8)),(Decimal('12.99000'), datetime(2023, 6, 21, 12, 8)),
            (Decimal('12.99000'), datetime(2023, 6, 22, 12, 8))]
        target_results = [('2023-06-15 11:00:00.000', Decimal('320379.6'), 'PB'),('2023-06-16 11:00:00.000', Decimal('316553.97'), 'PB'), ('2023-06-16 11:00:00.000', Decimal('316553.97'), 'Budget'),
            ('2023-06-16 11:00:00.000', Decimal('-16290.241'), 'LFY'),('2023-06-17 11:00:00.000', Decimal('22739.055'), 'LFY'),('2023-06-17 11:00:00.000', Decimal('321180.3'), 'Budget'),
            ('2023-06-17 11:00:00.000', Decimal('321180.3'), 'PB'),('2023-06-18 11:00:00.000', Decimal('319901.2'), 'Budget'),('2023-06-18 11:00:00.000', Decimal('319901.2'), 'PB'),
            ('2023-06-19 11:00:00.000', Decimal('-65154.17'), 'LFY'),('2023-06-19 11:00:00.000', Decimal('319383.6'), 'Budget'),('2023-06-19 11:00:00.000', Decimal('319383.6'), 'PB'),
            ('2023-06-20 11:00:00.000', Decimal('317746.56'), 'PB'),('2023-06-20 11:00:00.000', Decimal('317746.56'), 'Budget'),('2023-06-20 11:00:00.000', Decimal('-65154.17'), 'LFY')]

        methods = Mtpms()
        result = methods.get_mtpm_time_series_preferred_uom(ts_results, target_results, None, None, None, True)
        self.assertEqual(Decimal('316553.97'), result[0][0]['target_value_budget'])
        self.assertEqual(Decimal('-16290.241'), result[0][0]['target_value_lfy'])
        self.assertEqual(Decimal('316553.97'), result[0][0]['target_value_pb'])

        self.assertEqual(Decimal('319383.6'), result[0][3]['target_value_budget'])
        self.assertEqual(Decimal('-65154.17'), result[0][3]['target_value_lfy'])
        self.assertEqual(Decimal('319383.6'), result[0][3]['target_value_pb'])
    
    def test_build_mtpm_ts_data_with_no_target_data(self):
        ts_results = [(Decimal('12.99000'), datetime(2023, 6, 16, 12, 8)),(Decimal('12.99000'), datetime(2023, 6, 17, 12, 8)),(Decimal('12.99000'), datetime(2023, 6, 18, 12, 8)),
            (Decimal('12.99000'), datetime(2023, 6, 19, 12, 8)),(Decimal('12.99000'), datetime(2023, 6, 20, 12, 8)),(Decimal('12.99000'), datetime(2023, 6, 21, 12, 8)),
            (Decimal('12.99000'), datetime(2023, 6, 22, 12, 8))]
        target_results = []

        methods = Mtpms()
        result = methods.get_mtpm_time_series_preferred_uom(ts_results, target_results, Decimal('9872.3'), Decimal('65154.17'), Decimal('36549.98'), True)
        self.assertEqual(Decimal('36549.98'), result[0][0]['target_value_budget'])
        self.assertEqual(Decimal('9872.3'), result[0][0]['target_value_lfy'])
        self.assertEqual(Decimal('65154.17'), result[0][0]['target_value_pb'])

        self.assertEqual(Decimal('36549.98'), result[0][3]['target_value_budget'])
        self.assertEqual(Decimal('9872.3'), result[0][3]['target_value_lfy'])
        self.assertEqual(Decimal('65154.17'), result[0][3]['target_value_pb'])


    def test_get_snowflake_mtpm_target_data_ui_level_no_preferred_uom(self):
        ui_level = '1'
        plant_technology_id = '321-abc-321-abc-4321'
        datetimestart = '2022-12-16'
        datetimeend = '2023-01-16'

        methods = Mtpms()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [('123-abc-123-abc-1234', 'test_mtpm_name',
                                           'test_mtpm_display_name', 'test_area_name', 0, 0, 0, '2',
                                           Decimal('12.99000'),
                                           Decimal('12.99000'), Decimal('15.00000'), Decimal('15.00000'),
                                           Decimal('25.50000'), Decimal('25.50000'), Decimal('15.00000'),
                                           Decimal('25.50000'), Decimal('25.50000'), 0, 0, Decimal('11.80000'),
                                           Decimal('11.80000'), datetime(2022, 12, 16, 12, 8),
                                           datetime(2023, 1, 16, 6, 8), '%', '%', 20, 10, 50, 20, 20, 10, 50, 20, '%')]
        results = methods.get_snowflake_mtpm_target_data_generic(plant_technology_id,
                                                                           datetimestart,
                                                                           datetimeend, None, None, ui_level)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)

        for result in results:
            self.assertIn('mtpm_id', result)
            self.assertIn('mtpm_name', result)
            self.assertIn('mtpm_display_name', result)

    def test_snowflake_build_mtpm_dim_object(self):
        results = [('123-abc-123-abc-1234', 'test_mtpm_name',
                    'test_mtpm_display_name', 'test_area_name', 0, 0, 0, '2',
                    Decimal('12.99000'),
                    Decimal('12.99000'), Decimal('15.00000'), Decimal('15.00000'),
                    Decimal('25.50000'), Decimal('25.50000'), Decimal('15.00000'),
                    Decimal('25.50000'), Decimal('25.50000'), 0, 0, Decimal('11.80000'),
                    Decimal('11.80000'), 0, 0, 0, datetime(2022, 12, 16, 12, 8),
                    datetime(2023, 1, 16, 6, 8), '%', '%', 100, 0, 100, 0, 100, 0, 100, 0,
                    'Upper')]

        self.get_snowflake_mtpm_time_series.return_value = [{
            "mtpm_value_metric": Decimal("5214361.50000"),
            "mtpm_value_imperial": Decimal("5214361.50000"),
            "mtpm_value_timestamp_utc": datetime(2022, 12, 16, 12, 5),
            "mtpm_value_timestamp_utc": datetime(2022, 12, 16, 12, 5),
        }]

        methods = Mtpms()
        records = methods.snowflake_build_mtpm_dim_object(results, True)
        self.assertIsInstance(records, list)
        self.assertGreater(len(records), 0)

        for record in records:
            self.assertIn('mtpm_id', record)
            self.assertIn('mtpm_name', record)
            self.assertIn('mtpm_display_name', record)

    def test_snowflake_build_mtpm_dim_object_opportunity_endpoint(self):
        results = [('123-abc-123-abc-1234', 'test_mtpm_name',
                    'test_mtpm_display_name', 'test_area_name', 0, 0, 0, '2',
                    Decimal('12.99000'),
                    Decimal('12.99000'), Decimal('15.00000'), Decimal('15.00000'),
                    Decimal('25.50000'), Decimal('25.50000'), Decimal('15.00000'),
                    Decimal('25.50000'), Decimal('25.50000'), 0, 0, Decimal('11.80000'),
                    Decimal('11.80000'), 0, 0, 0, datetime(2022, 12, 16, 12, 8),
                    datetime(2023, 1, 16, 6, 8), '%', '%', 100, 0, 100, 0, 100, 0, 100, 0,
                    'Upper')]

        methods = Mtpms()
        records = methods.snowflake_build_mtpm_dim_object(results, True)
        self.assertIsInstance(records, list)
        self.assertGreater(len(records), 0)

        for record in records:
            self.assertIn('mtpm_id', record)
            self.assertIn('mtpm_name', record)
            self.assertIn('mtpm_display_name', record)
            self.assertNotIn('time_series', record)

    @patch.object(Mtpms, 'snowflake_build_mtpm_dim_object')
    def test_snowflake_build_mtpm_dim_object_preferred_uom(self, time_series_results):
        preferred_uom = 'test_preferred_uom'

        results = [('123-abc-123-abc-1234', 'test_mtpm_name',
                    'test_mtpm_display_name', 'test_area_name', 0, 0, 0, '2',
                    Decimal('12.99000'),
                    Decimal('12.99000'), Decimal('15.00000'), Decimal('15.00000'),
                    Decimal('25.50000'), Decimal('25.50000'), 0, 0, Decimal('11.80000'),
                    Decimal('11.80000'), Decimal('11.80000'), Decimal('11.80000'), Decimal('11.80000'), '%', 0)]
        
        time_series_results.return_value = [{
            "value": Decimal("5214361.50000"),
            "target_value_lfy": 5,
            "target_value_pb": 10,
            "target_value_budget": 4,
            "date": datetime(2022, 12, 16, 12, 5),
            "date_formatted": datetime(2022, 12, 16, 12, 5),
        }]

        methods = Mtpms()
        records = methods.snowflake_build_mtpm_dim_object_preferred_uom(results, time_series_results)
        self.assertIsInstance(records, list)
        self.assertGreater(len(records), 0)

        for record in records:
            self.assertIn('mtpm_id', record)
            self.assertIn('mtpm_name', record)
            self.assertIn('mtpm_display_name', record)

    @patch.object(Mtpms, 'get_mtpm_time_series_preferred_uom')
    def test_snowflake_build_mtpm_dim_object_preferred_uom_no_time_series(self, time_series_results):
        results = [('123-abc-123-abc-1234', 'test_mtpm_name',
                    'test_mtpm_display_name', 'test_area_name', 0, 0, 0, '2',
                    Decimal('12.99000'),
                    Decimal('12.99000'), Decimal('15.00000'), Decimal('15.00000'),
                    Decimal('25.50000'), Decimal('25.50000'), 0, 0, Decimal('11.80000'),
                    Decimal('11.80000'), Decimal('11.80000'), Decimal('11.80000'), Decimal('11.80000'), '%')]

        methods = Mtpms()
        records = methods.snowflake_build_mtpm_dim_object_preferred_uom(results, None)
        self.assertIsInstance(records, list)
        self.assertGreater(len(records), 0)

        for record in records:
            self.assertIn('mtpm_id', record)
            self.assertIn('mtpm_name', record)
            self.assertIn('mtpm_display_name', record)
            self.assertNotIn('time_series', record)

    def test_get_snowflake_mtpm_time_series(self):
        preferred_uom = 'test_preferred_uom'
        filtered_mtpms = [('123-abc-123-abc-1234', 'Steam', 'Steam', 'Corn Wet Mill',
                           0, 0, 1, '2', None, None, None, None, None, None, 0, 0, 0,0,0,
                           Decimal('11495699321.39731'), Decimal('5214361.50000'),
                           datetime(2023, 1, 15, 12, 5), datetime(2023, 1, 15, 6, 5), 'ton', 't')]

        methods = Mtpms()
        records = methods.get_snowflake_mtpm_time_series(filtered_mtpms)
        self.assertIsInstance(records, list)
        self.assertGreater(len(records), 0)

        for record in records:
            self.assertIn('mtpm_value_metric', record)
            self.assertIn('mtpm_value_timestamp_utc', record)
            self.assertIn('mtpm_value_timestamp_local', record)

    def test_get_snowflake_mtpm_time_series_start_zero(self):
        filtered_mtpms = [('123-abc-123-abc-1234', 'Steam', 'Steam', 'Corn Wet Mill',
                           0, 0, 1, '2', None, None, None, None, None, None, 0, 0,
                           0, 0, 0, Decimal('11495699321.39731'), Decimal('5214361.50000'),
                           datetime(2023, 1, 15, 12, 5), datetime(2023, 1, 15, 6, 5), 'ton', 't')]

        new_self = Mtpms()
        records = new_self.get_snowflake_mtpm_time_series(filtered_mtpms)
        self.assertIsInstance(records, list)
        self.assertGreater(len(records), 0)

        for record in records:
            self.assertIn('mtpm_value_metric', record)
            self.assertIn('mtpm_value_imperial', record)
            self.assertIn('mtpm_value_timestamp_utc', record)
            self.assertIn('mtpm_value_timestamp_local', record)
            self.assertEqual(record.get('mtpm_value_metric'), Decimal('5214361.50000'))
            self.assertEqual(record.get('mtpm_value_imperial'), Decimal('11495699321.39731'))

    def test_get_snowflake_leading_indicator_dim_data(self):
        mtpm_id = '123-abc-123-abc-1234'
        leading_indicator_id = '321-abc-321-abc-4321'

        methods = LeadingIndicators()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [('123-abc-123-abc-1234', 'test_leading_indicator_name',
                                           'test_leading_indicator_display_name', 0, 0, 'test_corrective_action', None,
                                           'test_cmo', Decimal('100.00000'), Decimal('0.00000'), Decimal('100.00000'),
                                           Decimal('0.00000'), None, None, None, None, '%', '%', '%', None)]

        results = methods.get_snowflake_leading_indicator_dim_data(mtpm_id, leading_indicator_id)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)

        for result in results:
            self.assertIn('leading_indicator_id', result)
            self.assertIn('leading_indicator_name', result)
            self.assertIn('leading_indicator_display_name', result)

    def test_get_snowflake_leading_indicator_time_series(self):
        filtered_indicators = [('123-abc-123-abc-1234', 'Steam', 'Steam', 'Corn Wet Mill',
                                0, 0, 1, '2', None, None, None, None, None, None, 0, 0,
                                Decimal('11495699321.39731'), Decimal('5214361.50000'),
                                datetime(2023, 1, 15, 12, 5), datetime(2023, 1, 15, 6, 5), 'ton', 't',
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)]

        methods = LeadingIndicators()
        records = methods.get_snowflake_leading_indicator_time_series(filtered_indicators)
        self.assertIsInstance(records, list)
        self.assertGreater(len(records), 0)

        for record in records:
            self.assertIn('leading_indicator_value_imperial', record)

    def test_get_snowflake_leading_indicator_time_series_null_mins_and_maxes(self):
        filtered_indicators = [(datetime(2023, 1, 15, 12, 5), None), (datetime(2023, 1, 17, 12, 5), None)]

        methods = LeadingIndicators()
        records = methods.get_snowflake_leading_indicator_time_series_and_sum(filtered_indicators)
        self.assertEquals(records[1], None)
        self.assertEquals(records[2], None)

    def test_get_snowflake_equipment_tag_dim_data(self):
        leading_indicator_id = '321-abc-321-abc-4321'
        equipment_tag_id = '123-abc-123-abc-1234'


        methods = EquipmentTags()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [('123-abc-123-abc-1234', '321-abc-321-abc-4321',
                                           'test_equipment_tag_name', 'test_equipment_tag_display_name', Decimal('100.00000'),
                                           Decimal('0.00000'), Decimal('100.00000'), Decimal('0.00000'), Decimal('0.15000'),
                                           Decimal('0.00000'), Decimal('0.15000'), Decimal('0.00000'), 'ml/15mL', 'ml/15mL')]

        results = methods.get_snowflake_equipment_tag_dim_data(leading_indicator_id, equipment_tag_id)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)

        for result in results:
            self.assertIn('equipment_tag_id', result)
            self.assertIn('leading_indicator_id', result)
            self.assertIn('equipment_tag_name', result)

    @patch.object(EquipmentTags, 'get_snowflake_equipment_tag_metric_time_series_data')
    def test_get_snowflake_equipment_tag_metric_data(self, time_series_patch):
        leading_indicator_id = '321-abc-321-abc-4321'
        date_time_start = '2022-12-01T00:00:00'
        date_time_end = '2023-01-16T00:00:00'
        equipment_tag_id = '123-abc-123-abc-1234'

        time_series_patch.return_value = []
        self.get_snowflake_equipment_tag_time_series.return_value = []

        methods = EquipmentTags()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [('123-abc-123-abc-1234', '321-abc-321-abc-4321',
                                           'test_equipment_tag_name', 'test_equipment_tag_display_name', Decimal('100.00000'),
                                           Decimal('0.00000'), Decimal('100.00000'), Decimal('0.00000'), Decimal('0.15000'),
                                           Decimal('0.00000'), Decimal('0.15000'), Decimal('0.00000'), 'ml/15mL', 'ml/15mL')]

        results = methods.get_snowflake_equipment_tag_metric_data(equipment_tag_id, leading_indicator_id, date_time_start, date_time_end)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)

        for result in results:
            self.assertIn('equipment_tag_id', result)
            self.assertIn('leading_indicator_id', result)
            self.assertIn('equipment_tag_name', result)

    def test_get_leading_indicator_time_series_empty_start_date(self):
        leading_indicator_id = '321-abc-321-abc-4321'
        date_time_start = '2022-12-01T00:00:00'
        date_time_end = '2023-01-16T00:08:00'

        self.get_snowflake_equipment_tag_time_series.return_value = []

        methods = LeadingIndicators()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [(0, 0,'2022-12-03T00:00:00', '2022-12-04T00:00:00', None),
                                                          (0, 0,'2023-01-16T00:00:00', '2023-01-16T00:00:00', None)]
        results = methods.get_leading_indicator_metric_time_series_data(date_time_start, date_time_end, '', '')
        self.assertIsInstance(results, list)
        self.assertEquals(len(results), 3)
        self.assertEquals(results[0][2], date_time_start)

    def test_get_leading_indicator_time_series_empty_end_date(self):
        leading_indicator_id = '321-abc-321-abc-4321'
        date_time_start = '2022-12-01T00:00:00'
        date_time_end = '2023-01-12T00:00:00'

        self.get_snowflake_equipment_tag_time_series.return_value = []

        methods = LeadingIndicators()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [(0, 0,date_time_start, date_time_start, None),
                                                          (0, 0,date_time_end, date_time_end, None)]
        results = methods.get_leading_indicator_metric_time_series_data(date_time_start, date_time_end, '', '')
        self.assertIsInstance(results, list)
        self.assertEquals(len(results), 2)
        self.assertEquals(results[1][2], date_time_end)


    def test_get_leading_indicator_time_series_empty_start_and_end_date(self):
        leading_indicator_id = '321-abc-321-abc-4321'
        date_time_start = '2022-12-01T00:00:00'
        date_time_end = '2023-01-14T00:00:00'

        self.get_snowflake_equipment_tag_time_series.return_value = []

        methods = LeadingIndicators()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [(0, 0,'2022-12-02T00:00:00', '2022-12-02T00:00:00', None), (0, 0,date_time_end, date_time_end, None)]

        results = methods.get_leading_indicator_metric_time_series_data(date_time_start, date_time_end, '', '')
        self.assertIsInstance(results, list)
        self.assertEquals(len(results), 3)
        self.assertEquals(results[2][2], date_time_end)
        self.assertEquals(results[0][2], date_time_start)

    def test_get_leading_indicator_time_series_correct_dates(self):
        leading_indicator_id = '321-abc-321-abc-4321'
        date_time_start = '2022-12-01T00:00:00'
        date_time_end = '2023-01-16T00:08:00'

        self.get_snowflake_equipment_tag_time_series.return_value = []

        methods = LeadingIndicators()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [(0, 0, datetime(2022, 12, 1, 10, 00, 00), '2022-12-01T00:05:00', None), (0, 0,'2023-01-16T00:03:00', '2023-01-16T00:03:00', None)]
        results = methods.get_leading_indicator_metric_time_series_data(date_time_start, date_time_end, '', '')
        self.assertIsInstance(results, list)
        self.assertEquals(len(results), 2)
        self.assertEquals(results[1][2], '2023-01-16T00:03:00')
        self.assertEquals(results[0][2], datetime(2022, 12, 1, 10, 0))

    def test_get_snowflake_equipment_tag_time_series(self):
        filtered_tags = [('123-abc-123-abc-1234', '321-abc-321-abc-4321', 'test_equipment_tag_name',
                          'test_equipment_tag_display_name', Decimal('100.00000'), Decimal('0.00000'), Decimal('100.00000'),
                          Decimal('0.00000'), Decimal('0.15000'), Decimal('0.00000'), Decimal('0.15000'), Decimal('0.00000'),
                          'ml/15mL', 'ml/15mL', datetime(2023, 1, 23, 0, 0), datetime(2023, 1, 22, 18, 0), Decimal('0.15000'),
                          Decimal('0.00723'), None)]

        methods = EquipmentTags()
        records = methods.get_snowflake_equipment_tag_time_series(filtered_tags)
        self.assertIsInstance(records, list)
        self.assertGreater(len(records), 0)

        for record in records:
            self.assertIn('equipment_tag_value_timestamp_utc', record)
            self.assertIn('equipment_tag_value_timestamp_local', record)
            self.assertIn('equipment_tag_value_metric', record)

    def test_get_equipment_tag_time_series_empty_start_date(self):
        date_time_start = '2022-12-01T00:00:00'
        date_time_end = '2023-01-16T00:08:00'

        self.get_snowflake_equipment_tag_time_series.return_value = []

        methods = EquipmentTags()
        methods.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = [(0, 0,'2022-12-03T00:00:00', '2022-12-04T00:00:00', None),
                                                          (0, 0,'2023-01-16T00:00:00', '2023-01-16T00:00:00', None)]
        results = methods.get_snowflake_equipment_tag_metric_time_series_data('', date_time_start, date_time_end)
        self.assertIsInstance(results, list)
        self.assertEquals(len(results), 3)
        self.assertEquals(results[0][2], date_time_start)

    def test_get_snowflake_equipment_tag_time_series_beta_validate_min_and_datetime(self):
        fact_table_select = [('2023-05-08 14:15:00.000', 2809.600035249), 
                        ('2023-05-08 15:00:00.000', 2823.929446405), 
                        ('2023-05-08 18:25:00.000', 2882.399354639)]
        expected_min = Decimal('2809.6000')
        expected_datetime = '2023-05-08 08:15:00-06:00'
        methods = EquipmentTags()
        records = methods.get_snowflake_equipment_tag_time_series_beta(fact_table_select, 'America/Costa Rica')
        time_series = records[0]
        ts_min = records[1]
        self.assertEquals(expected_min, ts_min)
        self.assertEquals(str(time_series[0]['time']), expected_datetime)
        self.assertEquals(time_series[0]['formatted_date'], '08 May, 08:15')

    def test_get_snowflake_equipment_tag_time_series_beta_validate_min_and_datetime_no_timezone(self):
        fact_table_select = [('2023-05-08 14:15:00.000', 2809.600035249),
                        ('2023-05-08 15:00:00.000', 2823.929446405),
                        ('2023-05-08 18:25:00.000', 2882.399354639)]
        expected_min = Decimal('2809.6000')
        expected_datetime = '2023-05-08 14:15:00+00:00'
        methods = EquipmentTags()
        records = methods.get_snowflake_equipment_tag_time_series_beta(fact_table_select, None)
        time_series = records[0]
        ts_min = records[1]
        self.assertEquals(expected_min, ts_min)
        self.assertEquals(str(time_series[0]['time']), expected_datetime)
        self.assertEquals(time_series[0]['formatted_date'], '08 May, 14:15')

    def test_get_leading_indicator_tag_time_series_beta_validate_min_and_datetime(self):
        fact_table_select = [('2023-05-08 14:15:00.000', 2809.600035249), 
                        ('2023-05-08 15:00:00.000', 2823.929446405),
                        ('2023-05-08 18:25:00.000', 2882.399354639)]
        expected_min = Decimal('2809.6000')
        expected_datetime = '2023-05-08 08:15:00-06:00'
        methods = LeadingIndicators()
        records = methods.get_snowflake_leading_indicator_time_series_and_sum(fact_table_select, 'America/Costa Rica')
        time_series = records[0]
        ts_min = records[1]
        self.assertEquals(expected_min, ts_min)
        self.assertEquals(str(time_series[0]['time']), expected_datetime)
        self.assertEquals(time_series[0]['formatted_date'], '08 May, 08:15')

    def test_trim_date_string(self):
        long_date = '2023-02-10T00:00:00'

        trimmed_date = helpers.trim_date_string(long_date)
        self.assertEqual(trimmed_date, '2023-02-10')

        no_date = ''
        trimmed_date = helpers.trim_date_string(no_date)
        self.assertEqual(trimmed_date, '')

    def test_get_datetime_obj(self):
        import pytz
        from dateutil import tz
        long_date = '2023-02-10 00:00:00'
        short_date = '2023-02-10'
        timezone_date = '2023-02-10T00:00:00'
        desired_result_aware = datetime(2023, 2, 10, 0, 0, tzinfo=tz.tzutc())
        desired_result_unaware = datetime(2023, 2, 10, 0, 0)

        date_obj = helpers.get_datetime_obj(long_date, tz.tzutc())
        self.assertEqual(date_obj, desired_result_aware)
        date_obj = helpers.get_datetime_obj(long_date)
        self.assertEqual(date_obj, desired_result_unaware)

        date_obj = helpers.get_datetime_obj(short_date, tz.tzutc())
        self.assertEqual(date_obj, desired_result_aware)
        date_obj = helpers.get_datetime_obj(short_date)
        self.assertEqual(date_obj, desired_result_unaware)

        date_obj = helpers.get_datetime_obj(timezone_date, tz.tzutc())
        self.assertEqual(date_obj, desired_result_aware)
        date_obj = helpers.get_datetime_obj(timezone_date)
        self.assertEqual(date_obj, desired_result_unaware)

    def test_convert_utc_to_local(self):
        import pytz
        from dateutil import tz
        long_date = '2023-06-22 10:25:00.000'
        timezone_string = 'America/Costa Rica'
        desired_result = datetime(2023, 6, 22, 4, 25, tzinfo=tz.gettz(timezone_string))

        date_obj = helpers.convert_utc_to_local(helpers.get_datetime_obj(long_date), timezone_string)
        self.assertEqual(date_obj, desired_result)

    def test_convert_utc_to_local_with_datetime_obj(self):
        import pytz
        from dateutil import tz
        long_date = datetime(2023, 6, 22, 10, 25)
        timezone_string = 'America/Costa Rica'
        desired_result = datetime(2023, 6, 22, 4, 25, tzinfo=tz.gettz(timezone_string))

        date_obj = helpers.convert_utc_to_local(helpers.get_datetime_obj(long_date), timezone_string)
        self.assertEqual(date_obj, desired_result)

    def test_convert_local_to_utc(self):
        import pytz
        from dateutil import tz
        long_date = '2023-06-22 10:25:00.000'
        timezone_string = 'US/Pacific'
        desired_result = datetime(2023, 6, 22, 17, 25, tzinfo=tz.gettz('UTC'))

        date_obj = helpers.convert_local_to_utc(helpers.get_datetime_obj(long_date), timezone_string)
        self.assertEqual(date_obj, desired_result)

    def test_format_datetime(self):
        import pytz
        from dateutil import tz
        long_date = '2021-06-22 10:25:00.000'
        desired_result = '22 Jun, 10:25'

        date_obj = helpers.format_datetime(helpers.get_datetime_obj(long_date))
        self.assertEqual(date_obj, desired_result)

    def test_custom_encoder(self):
        from django.http import JsonResponse
        from .encoders import MdpJSONEncoder
        filtered_indicators = [('2021-06-22 10:25:00.000', 13.47)]
        methods = LeadingIndicators()
        records = methods.get_snowflake_leading_indicator_time_series_and_sum(filtered_indicators)
        json_response = JsonResponse(encoder=MdpJSONEncoder, data={'results': list([{'time_series_data': records}])})
        decoded_response = json.loads(json_response.content.decode())["results"]
        self.assertEqual(decoded_response[0]["time_series_data"][0][0]["value"], 13.47)

    def test_set_group_memberships(self):
        decoded_read_only_token = {'name': 'Test Testman', 'roles': ['DEV_CSST_MDP_Cockpit_SNOWFLAKE_USER_READ']}
        decoded_write_token = {'name': 'Test Testman', 'roles': ['DEV_CSST_MDP_Cockpit_USER_WRITE']}
        decoded_admin_token = {'name': 'Test Testman', 'roles': ['DEV_CSST_MDP_Cockpit_USER_ADMIN']}
        decoded_prd_token = {'name': 'Test Testman', 'roles': [
                'DEV_CSST_MDP_Cockpit_User_Write_PRD',
                'DEV_CSST_MDP_Cockpit_User_Read_PRD',
                'DEV_CSST_MDP_Cockpit_User_Admin_PRD'
            ]}

        userGroupMembership, user_name, _ = set_group_memberships(decoded_read_only_token)

        self.assertEqual('Test Testman', user_name)
        self.assertEqual({'SNOWFLAKE_USER_READ'}, userGroupMembership)

        userGroupMembership, _, _ = set_group_memberships(decoded_write_token)

        self.assertEqual({'SNOWFLAKE_USER_READ', 'USER_WRITE'}, userGroupMembership)

        userGroupMembership, _, _ = set_group_memberships(decoded_admin_token)

        self.assertEqual({'SNOWFLAKE_USER_READ', 'USER_WRITE', 'USER_ADMIN'}, userGroupMembership)

        os.environ['ROLE_SUFFIX'] = '_PRD'
        userGroupMembership, _, _ = set_group_memberships(decoded_prd_token)

        self.assertEqual({'SNOWFLAKE_USER_READ', 'USER_WRITE', 'USER_ADMIN'}, userGroupMembership)
    

    def test_snowflake_mtpm_leadingindicators_summary(self):
        authorization = set_auth_header(self.write_access_token)

        views.leading_indicators = MagicMock()
        views.leading_indicators.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.post("/snowflake/mtpm/leadingindicators/summary/",
                                    {'mtpm': ['57ad768c-4e81-11ed-b701-1cc10cb4aa68', '57ad768c-4e81-11ed-b701-1cc10cb4aa68'], "datetimestart": "2022-12-30", "datetimeend": "2023-01-11",
                                    "plant_technology_id": '419d77de-4b28-11ed-b700-1cc10cb4aa68', "preferred_uom": "metric", "top": 5},
                                    format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_mtpm_leadingindicators_summary_missing_params(self):
        authorization = set_auth_header(self.write_access_token)

        views.leading_indicators = MagicMock()
        views.leading_indicators.wrapper = self.wrapper
        self.wrapper.validate_and_execute.return_value = []

        response = self.client.post("/snowflake/mtpm/leadingindicators/summary/",
                                    {"datetimestart": "2022-12-30", "datetimeend": "2023-01-11",
                                    "plant_technology_id": '419d77de-4b28-11ed-b700-1cc10cb4aa68', "preferred_uom": "metric", "top": 5},
                                    format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results == 'Missing the list of mtpm ids filter'

    def test_get_snowflake_leading_indicator_summary_data_with_top(self):

        db_schema = f'{os.getenv("SNOWFLAKE_DATABASE")}.{os.getenv("SNOWFLAKE_SCHEMA")}'
        dim_data =  self.get_dim_top_data()
        self.wrapper.validate_and_execute.return_value = [('5279b7ce-4fd6-11ed-b702-f4ee08e53170', 0, 0, 0, 0, 'B'),
                    ('843754ca-4fd6-11ed-b702-f4ee08e53170', 0, 0, 0, 0, 'C'),
                    ('3e2c7a75-4fd6-11ed-b702-f4ee08e53170', 0, 0, 0, 0, 'AWS'),
                    ('5279b7d1-4fd6-11ed-b702-f4ee08e53170', 0, 0, 0, 0, 'D'),
                    ('4c0afefc-4fd6-11ed-b702-f4ee08e53170', 0, 0, 0, 0, 'Ada'),
                    ('843754cd-4fd6-11ed-b702-f4ee08e53170', 0, 0, 0, 0, 'Z'),
                    ('843754d0-4fd6-11ed-b702-f4ee08e53170', 0, 0, 0, 0.012195, 'R'),
                    ('5279b7cb-4fd6-11ed-b702-f4ee08e53170', 0, 0.01227, 0.014286, 0, 'L'),
                    ('843754c1-4fd6-11ed-b702-f4ee08e53170', 0.026026, 0.012245, 0.00713, 0, 'E'),
                    ('843754c7-4fd6-11ed-b702-f4ee08e53170', 0.032723, 0.039409, 0.04499, 0.035242, 'CWM'),
                    ('843754c4-4fd6-11ed-b702-f4ee08e53170', 0.041594, 0.028346, 0.02627, 0, 'I/O P'),
                    ('5279b7d7-4fd6-11ed-b702-f4ee08e53170', 1, 1, 1, 0.333333, 'L')]

        methods = LeadingIndicators()
        methods.wrapper = self.wrapper
        records = methods.execute_summary_query('', dim_data, 5)
        self.assertEquals(len(records), 5)
        self.assertEquals(records[1]['leading_indicator_id'], '3e2c7a75-4fd6-11ed-b702-f4ee08e53170')
        self.assertEquals(records[4]['leading_indicator_id'], '5279b7d1-4fd6-11ed-b702-f4ee08e53170')
        self.assertEquals(records[0]['leading_indicator_id'], '4c0afefc-4fd6-11ed-b702-f4ee08e53170')
        self.assertEquals(records[1]['l24h'], 0)

    def test_get_snowflake_leading_indicator_summary_data_no_top(self):
        from dateutil import tz
        db_schema = f'{os.getenv("SNOWFLAKE_DATABASE")}.{os.getenv("SNOWFLAKE_SCHEMA")}'
        dim_data =  [('4c0afefc-4fd6-11ed-b702-f4ee08e53170', 'CGM Product Moisture', 12, 9, '%', '%', '57ad768c-4e81-11ed-b701-1cc10cb4aa68','Corn Gluten Meal Protein', None, None),
                    ('5279b7d7-4fd6-11ed-b702-f4ee08e53170', 'HSW Addition Rate', 1.1356, 0, 'm3/h', '%', '57ad768c-4e81-11ed-b701-1cc10cb4aa68','Corn Gluten Meal Protein', None, None),
                    ('3e2c7a75-4fd6-11ed-b702-f4ee08e53170', 'Primary Feed Density',14,8,'Be','%','57ad768c-4e81-11ed-b701-1cc10cb4aa68','Corn Gluten Meal Protein', '2023-07-17 08:03:08.676', 11.210000038),
                    ('5279b7d1-4fd6-11ed-b702-f4ee08e53170', 'Primary Feed Flow', 3217.6, 1703.4353, 'l/min', '%', '57ad768c-4e81-11ed-b701-1cc10cb4aa68', 'Corn Gluten Meal Protein', '2023-07-17 14:03:07.852', 199.600648491),
                    ('5279b7d4-4fd6-11ed-b702-f4ee08e53170', 'Primary OF Spinners', 1, 0.8, 'mL', '%', '57ad768c-4e81-11ed-b701-1cc10cb4aa68', 'Corn Gluten Meal Protein', None, None),
                    ('5279b7cb-4fd6-11ed-b702-f4ee08e53170', 'Primary UF Baume', 21.1, 18.1,'Be','%','57ad768c-4e81-11ed-b701-1cc10cb4aa68','Corn Gluten Meal Protein','2023-07-17 12:03:17.746', 20.345000267),
                    ('5279b7ce-4fd6-11ed-b702-f4ee08e53170', 'Primary UF Valve Position', 90, 60, '%', '%', '57ad768c-4e81-11ed-b701-1cc10cb4aa68', 'Corn Gluten Meal Protein', '2023-07-17 14:03:07.852', 81.255699158),
                    ('843754c1-4fd6-11ed-b702-f4ee08e53170', 'FE Baume', 12.2, 10.7,	'Be', '%', '6531ce65-4f21-11ed-b701-1cc10cb4aa68', 'Starch in Germ', '2023-07-17 08:03:08.676', 10.239999771),
                    ('843754c4-4fd6-11ed-b702-f4ee08e53170', 'Germ Purity', 46, 43, 'db', '%', '6531ce65-4f21-11ed-b701-1cc10cb4aa68', 'Starch in Germ', '2023-07-17 13:03:18.671', 47.659999847),
                    ('843754ca-4fd6-11ed-b702-f4ee08e53170', 'Germ Wash Water (GTMW) Ratio', None, None, 'm3/t', '%', '6531ce65-4f21-11ed-b701-1cc10cb4aa68', 'Starch in Germ', '2023-07-17 14:03:07.852', 6.889848709),
                    ('843754c7-4fd6-11ed-b702-f4ee08e53170', 'K1 O/S Ratios', 25, 20, '%', '%', '6531ce65-4f21-11ed-b701-1cc10cb4aa68', 'Starch in Germ', '2023-07-17 14:03:07.852', 20.549545288),
                    ('843754cd-4fd6-11ed-b702-f4ee08e53170', 'LSW pH', 4.2, 0, 'pH', '%', '6531ce65-4f21-11ed-b701-1cc10cb4aa68', 'Starch in Germ', '2023-07-17 13:03:18.671', 3.99000001),
                    ('843754d0-4fd6-11ed-b702-f4ee08e53170', 'Steeptime', 32, 27, 'h', '%', '6531ce65-4f21-11ed-b701-1cc10cb4aa68', 'Starch in   Germ', '2023-07-17 14:03:07.852', 36.999799728)]
        self.wrapper.validate_and_execute.return_value = [('5279b7ce-4fd6-11ed-b702-f4ee08e53170', 0, 0, 0, 0),
                    ('843754ca-4fd6-11ed-b702-f4ee08e53170', 0, 0, 0, 0),
                    ('3e2c7a75-4fd6-11ed-b702-f4ee08e53170', 0, 0, 0, 0),
                    ('5279b7d1-4fd6-11ed-b702-f4ee08e53170', 0, 0, 0, 0),
                    ('4c0afefc-4fd6-11ed-b702-f4ee08e53170', 0, 0, 0, 0),
                    ('843754cd-4fd6-11ed-b702-f4ee08e53170', 0, 0, 0, 0),
                    ('843754d0-4fd6-11ed-b702-f4ee08e53170', 0, 0, 0, 0.012195),
                    ('5279b7cb-4fd6-11ed-b702-f4ee08e53170', 0, 0.01227, 0.014286, 0),
                    ('843754c1-4fd6-11ed-b702-f4ee08e53170', 0.026026, 0.012245, 0.00713, 0),
                    ('843754c7-4fd6-11ed-b702-f4ee08e53170', 0.032723, 0.039409, 0.04499, 0.035242),
                    ('843754c4-4fd6-11ed-b702-f4ee08e53170', 0.041594, 0.028346, 0.02627, 0),
                    ('5279b7d7-4fd6-11ed-b702-f4ee08e53170', 1, 1, 1, 0.333333)]

        methods = LeadingIndicators()
        methods.wrapper = self.wrapper
        records = methods.execute_summary_query('', dim_data, None)
        self.assertEquals(len(records), 13)
        self.assertEquals(records[3]['leading_indicator_id'], '5279b7d1-4fd6-11ed-b702-f4ee08e53170')
        self.assertEquals(records[3]['uom_inside_envelope'], '%')
        self.assertEquals(records[8]['last_refresh_timestamp'], datetime(2023, 7, 17, 20, 3, 18, 671000, tzinfo=tz.tzutc()))
    
    def test_get_snowflake_leading_indicator_summary_dim_query(self):
        db_schema = f'{os.getenv("SNOWFLAKE_DATABASE")}.{os.getenv("SNOWFLAKE_SCHEMA")}'
        expected_query = f"""WITH cte_recent_vals AS (
                        SELECT fl.leading_indicator_id, fl.leading_indicator_value_metric
                        FROM {db_schema}."FACT_LEADING_INDICATOR" fl
                        INNER JOIN (SELECT leading_indicator_id, max(leading_indicator_value_timestamp_utc) AS dtc 
                            FROM {db_schema}."FACT_LEADING_INDICATOR" 
                            WHERE leading_indicator_value_timestamp_utc <= '2023-07-31 23:00:00'
                            GROUP BY leading_indicator_id) fli 
                        ON fli.leading_indicator_id = fl.leading_indicator_id and fl.leading_indicator_value_timestamp_utc = dtc
                        WHERE fl.leading_indicator_id in 
                            (SELECT l.leading_indicator_id from DIM_LEADING_INDICATOR l 
                                INNER JOIN DIM_MTPM m ON m.mtpm_id = l.mtpm_id 
                                WHERE l.mtpm_id IN (['57ad768c-4e81-11ed-b701-1cc10cb4aa68', '57ad768c-4e81-11ed-b701-1cc10cb4aa68'])
                            )
                        ),
                        cte_timestamp_vals AS (
                        SELECT fl.leading_indicator_id, fl.cdp_datetime_updated
                        FROM {db_schema}."FACT_LEADING_INDICATOR" fl
                        INNER JOIN (SELECT leading_indicator_id, max(leading_indicator_value_timestamp_utc) AS dtc 
                            FROM {db_schema}."FACT_LEADING_INDICATOR" 
                            GROUP BY leading_indicator_id) fli 
                        ON fli.leading_indicator_id = fl.leading_indicator_id and fl.leading_indicator_value_timestamp_utc = dtc
                        WHERE fl.leading_indicator_id in 
                            (SELECT l.leading_indicator_id from DIM_LEADING_INDICATOR l 
                                INNER JOIN DIM_MTPM m ON m.mtpm_id = l.mtpm_id 
                                WHERE l.mtpm_id IN (['57ad768c-4e81-11ed-b701-1cc10cb4aa68', '57ad768c-4e81-11ed-b701-1cc10cb4aa68']))
                            ) 
                        SELECT l.leading_indicator_id,
                        l.leading_indicator_display_name,
                        l.max_metric,
                        l.min_metric,
                        l.uom_metric,
                        l.uom_inside_envelope,
                        m.mtpm_id,
                        m.mtpm_display_name,
                        ts.cdp_datetime_updated,
                        rv.leading_indicator_value_metric
                    FROM
                        {db_schema}."DIM_LEADING_INDICATOR" l
                    INNER JOIN {db_schema}."DIM_MTPM" m 
                    ON m.mtpm_id = l.mtpm_id
                    LEFT JOIN cte_recent_vals rv on rv.leading_indicator_id = l.leading_indicator_id
                    LEFT JOIN cte_timestamp_vals ts on ts.leading_indicator_id = l.leading_indicator_id
                    WHERE l.mtpm_id in (['57ad768c-4e81-11ed-b701-1cc10cb4aa68', '57ad768c-4e81-11ed-b701-1cc10cb4aa68'])
                    ORDER BY mtpm_display_name, leading_indicator_display_name"""

        methods = LeadingIndicators()
        query = methods.get_li_summary_dim_query(['57ad768c-4e81-11ed-b701-1cc10cb4aa68', '57ad768c-4e81-11ed-b701-1cc10cb4aa68'], 'metric',
            db_schema, datetime(2023, 7, 17), datetime(2023, 7, 31, 23, 0, 0))
        self.assertEquals(query, expected_query)

    def test_get_snowflake_leading_indicator_summary_target_query(self):
        db_schema = f'{os.getenv("SNOWFLAKE_DATABASE")}.{os.getenv("SNOWFLAKE_SCHEMA")}'
        expected_query = f"""
                        SELECT
                            di.leading_indicator_id,
                            lih24h.leading_indicator_inside_envelope_last24h_value p_24h,
                            lih12h.leading_indicator_inside_envelope_last12h_value p_12h,
                            lih8h.leading_indicator_inside_envelope_last8h_value p_8h,
                            lih1h.leading_indicator_inside_envelope_last1h_value p_1h,
                            di.leading_indicator_display_name
                                FROM {db_schema}."DIM_LEADING_INDICATOR" di
                                LEFT JOIN (SELECT leading_indicator_id, leading_indicator_inside_envelope_last1h_value, leading_indicator_value_timestamp_utc, ROW_NUMBER() OVER (PARTITION BY leading_indicator_id ORDER BY leading_indicator_value_timestamp_utc DESC) row_id
                                FROM {db_schema}."FACT_LEADING_INDICATOR_TARGET_HEALTH"
                                WHERE leading_indicator_value_timestamp_utc BETWEEN '2023-06-01 00:00:00' AND '2023-06-30 00:00:00'
                                AND leading_indicator_inside_envelope_last1h_value IS NOT NULL) lih1h
                                ON lih1h.leading_indicator_id = di.leading_indicator_id AND lih1h.row_id = 1
                                LEFT JOIN (SELECT leading_indicator_id, leading_indicator_inside_envelope_last8h_value, leading_indicator_value_timestamp_utc, ROW_NUMBER() OVER (PARTITION BY leading_indicator_id ORDER BY leading_indicator_value_timestamp_utc DESC) row_id
                                FROM {db_schema}."FACT_LEADING_INDICATOR_TARGET_HEALTH"
                                WHERE leading_indicator_value_timestamp_utc BETWEEN '2023-06-01 00:00:00' AND '2023-06-30 00:00:00' 
                                AND leading_indicator_inside_envelope_last8h_value IS NOT NULL) lih8h
                                ON lih8h.leading_indicator_id = di.leading_indicator_id AND lih8h.row_id = 1
                                LEFT JOIN (SELECT leading_indicator_id, leading_indicator_inside_envelope_last12h_value, leading_indicator_value_timestamp_utc, ROW_NUMBER() OVER (PARTITION BY leading_indicator_id ORDER BY leading_indicator_value_timestamp_utc DESC) row_id
                                FROM {db_schema}."FACT_LEADING_INDICATOR_TARGET_HEALTH"
                                WHERE leading_indicator_value_timestamp_utc BETWEEN '2023-06-01 00:00:00' AND '2023-06-30 00:00:00'
                                AND leading_indicator_inside_envelope_last12h_value IS NOT NULL) lih12h
                                ON lih12h.leading_indicator_id = di.leading_indicator_id AND lih12h.row_id = 1
                                LEFT JOIN (SELECT leading_indicator_id, leading_indicator_inside_envelope_last24h_value, leading_indicator_value_timestamp_utc, ROW_NUMBER() OVER (PARTITION BY leading_indicator_id ORDER BY leading_indicator_value_timestamp_utc DESC) row_id
                                FROM {db_schema}."FACT_LEADING_INDICATOR_TARGET_HEALTH"
                                WHERE leading_indicator_value_timestamp_utc BETWEEN '2023-06-01 00:00:00' AND '2023-06-30 00:00:00'
                                AND leading_indicator_inside_envelope_last24h_value IS NOT NULL) lih24h
                                ON lih24h.leading_indicator_id = di.leading_indicator_id AND lih24h.row_id = 1
                                WHERE di.leading_indicator_id in (
                                    SELECT l.leading_indicator_id 
                                    FROM {db_schema}."DIM_LEADING_INDICATOR" l 
                                    INNER JOIN {db_schema}."DIM_MTPM" m ON m.mtpm_id = l.mtpm_id 
                                    WHERE l.mtpm_id IN (['57ad768c-4e81-11ed-b701-1cc10cb4aa68', '57ad768c-4e81-11ed-b701-1cc10cb4aa68']))
                                ORDER BY p_24h, p_12h, p_8h, p_1h ASC
                        """
        methods = LeadingIndicators()
        query = methods.get_li_summary_query(['57ad768c-4e81-11ed-b701-1cc10cb4aa68', '57ad768c-4e81-11ed-b701-1cc10cb4aa68'], datetime(2023,6,1), datetime(2023,6,30), db_schema)
        self.assertEquals(query, expected_query)

    @patch.object(LeadingIndicators, 'execute_dim_query')
    @patch.object(LeadingIndicators, 'execute_summary_query')
    def test_call_parent_summary_method(self, summary_results, dim_results):
        summary_results.return_value = [{
                                    "leading_indicator_id": '4c0afefc-4fd6-11ed-b702-f4ee08e53170',
                                    "leading_indicator_display_name": 'CGM Product Moisture',
                                    "max_target_value": 12,
                                    "min_target_value": 9,
                                    "uom": '%',
                                    "uom_inside_envelope": '%',
                                    "mtpm_id": '57ad768c-4e81-11ed-b701-1cc10cb4aa68',
                                    "mtpm_display_name": 'Corn Gluten Meal Protein',
                                    "last_refresh_timestamp": None,
                                    "value": None,  
                                    "l24h": None,"l12h": None, "l8h": None,"l1h": None}]
        dim_results.return_value = self.get_dim_top_data()
        methods = LeadingIndicators()
        results = methods.get_snowflake_leading_indicator_summary(['57ad768c-4e81-11ed-b701-1cc10cb4aa68', '57ad768c-4e81-11ed-b701-1cc10cb4aa68'], '2023-06-01', '2023-06-30', 'metric', 5)
        self.assertEquals('4c0afefc-4fd6-11ed-b702-f4ee08e53170', results[0]['leading_indicator_id'])
        self.assertEquals(1, len(results))

    def get_dim_top_data(self):
        return {'4c0afefc-4fd6-11ed-b702-f4ee08e53170': {
                                    "leading_indicator_id": '4c0afefc-4fd6-11ed-b702-f4ee08e53170',
                                    "leading_indicator_display_name": 'CGM Product Moisture',
                                    "max_target_value": 12,
                                    "min_target_value": 9,
                                    "uom": '%',
                                    "uom_inside_envelope": '%',
                                    "mtpm_id": '57ad768c-4e81-11ed-b701-1cc10cb4aa68',
                                    "mtpm_display_name": 'Corn Gluten Meal Protein',
                                    "last_refresh_timestamp": None,
                                    "value": None},
                    '5279b7d7-4fd6-11ed-b702-f4ee08e53170': {
                                    "leading_indicator_id": '5279b7d7-4fd6-11ed-b702-f4ee08e53170',
                                    "leading_indicator_display_name": 'HSW Addition Rate',
                                    "max_target_value": 1.1356,
                                    "min_target_value": 0,
                                    "uom": 'm3/h',
                                    "uom_inside_envelope": '%',
                                    "mtpm_id": '57ad768c-4e81-11ed-b701-1cc10cb4aa68',
                                    "mtpm_display_name": 'Corn Gluten Meal Protein',
                                    "last_refresh_timestamp": None,
                                    "value": None}, 
                    '3e2c7a75-4fd6-11ed-b702-f4ee08e53170': {
                                    "leading_indicator_id": '3e2c7a75-4fd6-11ed-b702-f4ee08e53170',
                                    "leading_indicator_display_name": 'Primary Feed Density',
                                    "max_target_value": 14,
                                    "min_target_value": 8,
                                    "uom": 'Be',
                                    "uom_inside_envelope": '%',
                                    "mtpm_id": '57ad768c-4e81-11ed-b701-1cc10cb4aa68',
                                    "mtpm_display_name": 'Corn Gluten Meal Protein',
                                    "last_refresh_timestamp": '2023-07-17 08:03:08.676',
                                    "value": 11.210000038},
                    '5279b7d1-4fd6-11ed-b702-f4ee08e53170': {
                                    "leading_indicator_id": '5279b7d1-4fd6-11ed-b702-f4ee08e53170',
                                    "leading_indicator_display_name": 'Primary Feed Flow',
                                    "max_target_value": 3217.6,
                                    "min_target_value": 1703.4353,
                                    "uom": 'l/min',
                                    "uom_inside_envelope": '%',
                                    "mtpm_id": '57ad768c-4e81-11ed-b701-1cc10cb4aa68',
                                    "mtpm_display_name": 'Corn Gluten Meal Protein',
                                    "last_refresh_timestamp": '2023-07-17 14:03:07.852',
                                    "value": 199.600648491},
                    '5279b7d4-4fd6-11ed-b702-f4ee08e53170': {
                                    "leading_indicator_id": '5279b7d4-4fd6-11ed-b702-f4ee08e53170',
                                    "leading_indicator_display_name": 'Primary OF Spinners',
                                    "max_target_value": 1,
                                    "min_target_value": 0.8,
                                    "uom": 'mL',
                                    "uom_inside_envelope": '%',
                                    "mtpm_id": '57ad768c-4e81-11ed-b701-1cc10cb4aa68',
                                    "mtpm_display_name": 'Corn Gluten Meal Protein',
                                    "last_refresh_timestamp": None,
                                    "value": None},
                    '5279b7cb-4fd6-11ed-b702-f4ee08e53170': {
                                    "leading_indicator_id": '5279b7cb-4fd6-11ed-b702-f4ee08e53170',
                                    "leading_indicator_display_name": 'Primary UF Baume',
                                    "max_target_value": 21.1,
                                    "min_target_value": 18.1,
                                    "uom": 'Be',
                                    "uom_inside_envelope": '%',
                                    "mtpm_id": '57ad768c-4e81-11ed-b701-1cc10cb4aa68',
                                    "mtpm_display_name": 'Corn Gluten Meal Protein',
                                    "last_refresh_timestamp": '2023-07-17 12:03:17.746',
                                    "value": 20.345000267},
                    '5279b7ce-4fd6-11ed-b702-f4ee08e53170': {
                                    "leading_indicator_id": '5279b7ce-4fd6-11ed-b702-f4ee08e53170',
                                    "leading_indicator_display_name": 'Primary UF Valve Position',
                                    "max_target_value": 90,
                                    "min_target_value": 60,
                                    "uom": '%',
                                    "uom_inside_envelope": '%',
                                    "mtpm_id": '57ad768c-4e81-11ed-b701-1cc10cb4aa68',
                                    "mtpm_display_name": 'Corn Gluten Meal Protein',
                                    "last_refresh_timestamp": '2023-07-17 14:03:07.852',
                                    "value": 81.255699158},
                    '843754c1-4fd6-11ed-b702-f4ee08e53170': {
                                    "leading_indicator_id": '843754c1-4fd6-11ed-b702-f4ee08e53170',
                                    "leading_indicator_display_name": 'FE Baume',
                                    "max_target_value": 12.2,
                                    "min_target_value": 10.7,
                                    "uom": 'Be',
                                    "uom_inside_envelope": '%',
                                    "mtpm_id": '6531ce65-4f21-11ed-b701-1cc10cb4aa68',
                                    "mtpm_display_name": 'Starch in Germ',
                                    "last_refresh_timestamp": '2023-07-17 08:03:08.676',
                                    "value": 10.239999771},
                    '843754c4-4fd6-11ed-b702-f4ee08e53170': {
                                    "leading_indicator_id": '843754c4-4fd6-11ed-b702-f4ee08e53170',
                                    "leading_indicator_display_name": 'Germ Purity',
                                    "max_target_value": 46,
                                    "min_target_value": 43,
                                    "uom": 'db',
                                    "uom_inside_envelope": '%',
                                    "mtpm_id": '6531ce65-4f21-11ed-b701-1cc10cb4aa68',
                                    "mtpm_display_name": 'Starch in Germ',
                                    "last_refresh_timestamp": '2023-07-17 13:03:18.671',
                                    "value": 47.659999847},
                    '843754ca-4fd6-11ed-b702-f4ee08e53170': {
                                    "leading_indicator_id": '843754ca-4fd6-11ed-b702-f4ee08e53170',
                                    "leading_indicator_display_name": 'Germ Wash Water (GTMW) Ratio',
                                    "max_target_value": None,
                                    "min_target_value": None,
                                    "uom": 'm3/t',
                                    "uom_inside_envelope": '%',
                                    "mtpm_id": '6531ce65-4f21-11ed-b701-1cc10cb4aa68',
                                    "mtpm_display_name": 'Starch in Germ',
                                    "last_refresh_timestamp": '2023-07-17 14:03:07.852',
                                    "value": 6.889848709},
                    '843754c7-4fd6-11ed-b702-f4ee08e53170': {
                                    "leading_indicator_id": '843754c7-4fd6-11ed-b702-f4ee08e53170',
                                    "leading_indicator_display_name": 'K1 O/S Ratios',
                                    "max_target_value": 25,
                                    "min_target_value": 20,
                                    "uom": '%',
                                    "uom_inside_envelope": '%',
                                    "mtpm_id": '6531ce65-4f21-11ed-b701-1cc10cb4aa68',
                                    "mtpm_display_name": 'Starch in Germ',
                                    "last_refresh_timestamp": '2023-07-17 14:03:07.852',
                                    "value": 20.549545288},
                    '843754cd-4fd6-11ed-b702-f4ee08e53170': {
                                    "leading_indicator_id": '843754cd-4fd6-11ed-b702-f4ee08e53170',
                                    "leading_indicator_display_name": 'LSW pH',
                                    "max_target_value": 4.2,
                                    "min_target_value": 0,
                                    "uom": 'pH',
                                    "uom_inside_envelope": '%',
                                    "mtpm_id": '6531ce65-4f21-11ed-b701-1cc10cb4aa68',
                                    "mtpm_display_name": 'Starch in Germ',
                                    "last_refresh_timestamp": '2023-07-17 13:03:18.671',
                                    "value": 3.99000001},
                    '843754d0-4fd6-11ed-b702-f4ee08e53170': {
                                    "leading_indicator_id": '843754d0-4fd6-11ed-b702-f4ee08e53170',
                                    "leading_indicator_display_name": 'Steeptime',
                                    "max_target_value": 32,
                                    "min_target_value": 27,
                                    "uom": 'h',
                                    "uom_inside_envelope": '%',
                                    "mtpm_id": '6531ce65-4f21-11ed-b701-1cc10cb4aa68',
                                    "mtpm_display_name": 'Starch in Germ',
                                    "last_refresh_timestamp": '2023-07-17 14:03:07.852',
                                    "value": 36.999799728}}