import os 
from django.test import TestCase
from rest_framework.test import APIClient
from unittest.mock import MagicMock, patch
from .test_utils import set_sso_client_credential_token, set_auth_header
from snowflake_drf import views

class MonitoringTests(TestCase):
    client = APIClient()

    def setUp(self):
        self.wrapper = MagicMock()
        self.wrapper.is_usable = MagicMock()
        self.wrapper.is_connection_available = MagicMock()

    @classmethod
    def setUpTestData(cls):
        cls.client_credentials_access_token = set_sso_client_credential_token()

    def test_snowflake_usable(self):

        views.wrapper =  MagicMock()
        views.wrapper = self.wrapper

        self.wrapper.is_usable.return_value = True
    
        authorization = set_auth_header(self.client_credentials_access_token)

        payload = {
            "results": "OK"
        }

        response = self.client.get("/monitoring/snowflake/usable/", payload, format='json', **authorization)
        results = response.data
        assert results["results"] == payload["results"]
        assert response.status_code == 200

    def test_snowflake_usable_failed(self):

        views.wrapper =  MagicMock()
        views.wrapper = self.wrapper

        self.wrapper.is_usable.return_value = False

        authorization = set_auth_header(self.client_credentials_access_token)

        payload = {
            "results": "Not Available"
        }

        response = self.client.get("/monitoring/snowflake/usable/", payload, format='json', **authorization)
        results = response.data
        assert results["results"] == payload["results"]
        assert response.status_code == 400

    def test_snowflake_persistent_connection(self):

        views.wrapper =  MagicMock()
        views.wrapper = self.wrapper

        self.wrapper.is_connection_available.return_value = True

        authorization = set_auth_header(self.client_credentials_access_token)

        payload = {
            "results": "OK"
        }

        response = self.client.get("/monitoring/snowflake/connection/", payload, format='json', **authorization)
        results = response.data
        assert results["results"] == payload["results"]
        assert response.status_code == 200
    
    def test_snowflake_persistent_connection_failed(self):

        views.wrapper =  MagicMock()
        views.wrapper = self.wrapper

        self.wrapper.is_connection_available.return_value = False

        authorization = set_auth_header(self.client_credentials_access_token)

        payload = {
            "results": "Not Available"
        }

        response = self.client.get("/monitoring/snowflake/connection/", payload, format='json', **authorization)
        results = response.data
        assert results["results"] == payload["results"]
        assert response.status_code == 400