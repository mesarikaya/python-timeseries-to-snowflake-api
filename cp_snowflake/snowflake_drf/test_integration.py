import concurrent.futures
from datetime import datetime
from decimal import Decimal

from django.test import TestCase, tag
import json
import logging
import os

from . import helpers
from .queries.methods import date_range, SnowflakeMethods
from .token_utils import generate_non_sso_token, generate_client_credentials_token
from .test_utils import set_auth_header, set_sso_client_credential_token, set_sso_user_token

from snowflake_drf.task_data_import import process_element_data, process_region, process_technology, process_plant, \
    process_plant_technology

log = logging.getLogger(__name__)
from rest_framework.test import APIClient
from snowflake_drf import tasks
from unittest.mock import MagicMock, patch

from snowflake_drf.models import Region, Technology, PlantTechnology, Plant
import pandas as pd

log = logging.getLogger(__name__)

@tag('integration')
class SnowflakeIntegrationTestCase(TestCase):
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
        
        if os.environ.get('SNOWFLAKE_USER') is None:
            raise Exception('Snowflake user data must be populated for integration tests to run.')

    def test_snowflake_mtpm_list(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get("/snowflake/mtpm/", format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_mtpm_list_parameter(self):
        authorization = set_auth_header(self.write_access_token)

        response = self.client.get("/snowflake/mtpm/", {"plant_technology_id": "0171c2b7-6bf7-11ed-b70c-1cc10cb4aa68"},
                                   format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_mtpm_id_post(self):
        authorization = set_auth_header(self.client_credentials_access_token)

        response = self.client.post("/snowflake/mtpm/metrics/", {'mtpm': ['ID1R1P1T1MTPM1', 'ID1R1P1T1MTPM2', 'ID1R1P1T1MTPM3', '4ff9067b-4f2a-11ed-b701-1cc10cb4aa68'],
                                                         "datetimestart": "2023-01-01", "datetimeend": "2023-01-12",
                                                         "plant_technology_id": "5b5fbd07-4b28-11ed-b700-1cc10cb4aa68"},
                                    format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_mtpm_ui_level_post(self):
        authorization = set_auth_header(self.client_credentials_access_token)

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

        response = self.client.post("/snowflake/mtpm/metrics/",
                                    {"ui_level": '1', "datetimestart": "2022-12-01", "datetimeend": "2023-02-01", "plant_technology_id": "5b5fbd07-4b28-11ed-b700-1cc10cb4aa68",
                                    "preferred_uom": "imperial"},
                                    format='json', **authorization)

        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def test_snowflake_mtpm_leadingindicators(self):
        authorization = set_auth_header(self.client_credentials_access_token)

        response = self.client.get("/snowflake/mtpm/leadingindicators/",
                                    {"mtpm_id": '581b8150-6b76-11ed-b70c-1cc10cb4aa68', "datetimestart": "2022-12-30", "datetimeend": "2023-01-11"},
                                    format='json', **authorization)
        results = self.get_results(response.content)
        assert response.status_code == 200
        assert results is not None

    def get_results(self, content):
        log.debug("content: %s", content)
        return json.loads(content.decode())["results"]