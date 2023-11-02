import copy
import json
import logging
import os
import jwt
from django.test import TestCase
from rest_framework.exceptions import ValidationError, AuthenticationFailed

from .test_utils import set_sso_user_token, set_auth_header, set_sso_client_credential_token
from .token_utils import generate_client_credentials_token, generate_non_sso_token, generate_key, \
    get_payload_and_headers, get_kid, get_jwks_uri, get_jwks, get_jwk, CommunicationError, InvalidToken

log = logging.getLogger(__name__)
from rest_framework.test import APIClient

log = logging.getLogger(__name__)

class AuthLayerTestCase(TestCase):
    token = None
    client = APIClient()
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

        """SET TEST_WITH_REAL_SSO env variable to false in .env to be able to test without real sso"""
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

    def test_without_auth_header(self):
        response = self.client.get("/forms/regions/", format='json')
        assert response.status_code != 200

    def test_with_auth_header_and_no_token(self):
        authorization = {'HTTP_AUTHORIZATION': f'Bearer {None}'}
        response = self.client.get("/test-authentication/", format='json', **authorization)
        assert response.status_code != 200

    def test_read_access_user(self):
        authorization = set_auth_header(self.read_only_access_token)
        response = self.client.get("/forms/regions/", format='json', **authorization)
        assert response.status_code == 200

    def test_write_access_user_for_admin_access_need(self):
        authorization = set_auth_header(self.write_access_token)
        response = self.client.get("/test-authentication/", format='json', **authorization)
        assert response.status_code != 200

    def test_client_credential_with_user_token(self):
        authorization = set_auth_header(self.write_access_token)
        response = self.client.get("/snowflake/mtpm/leadingindicators/", format='json', **authorization)
        assert response.status_code == 200

    def test_client_credential_without_auth_header(self):
        response = self.client.get("/snowflake/mtpm/leadingindicators/", format='json')
        assert response.status_code != 200

    def test_client_credential_without_token(self):
        authorization = {'HTTP_AUTHORIZATION': f'Bearer {None}'}
        response = self.client.get("/snowflake/mtpm/leadingindicators/", format='json', **authorization)
        assert response.status_code != 200

    def test_sso_sign_in_with_wrong_credentials_and_with_right_client_id(self):
        response = self.client.post("/generate-sso-token/", format='json',
                                    data= {"client-secret": os.environ['CLIENT_ID'],
                                           "username": "wrong_user",
                                           "password": "wrong_password"})
        assert response.status_code == 400

    def test_sso_sign_in_with_wrong_credentials_and_wrong_client_id(self):
        response = self.client.post("/generate-sso-token/", format='json',
                                    data= {"client-secret": os.environ['CLIENT_ID']+'wrong',
                                           "username": "wrong_user",
                                           "password": "wrong_password"})
        assert response.status_code == 400

    def test_sso_admin_user_sign_in(self):
        if 'TEST_WITH_REAL_SSO' in os.environ and os.environ['TEST_WITH_REAL_SSO'] == 'true':
            token = set_sso_user_token("ADMIN")
            assert (token is not None)

    def test_sso_write_user_sign_in(self):
        if 'TEST_WITH_REAL_SSO' in os.environ and os.environ['TEST_WITH_REAL_SSO'] == 'true':
            token = set_sso_user_token("WRITE")
            assert (token is not None)

    def test_sso_read_user_sign_in(self):
        if 'TEST_WITH_REAL_SSO' in os.environ and os.environ['TEST_WITH_REAL_SSO'] == 'true':
            token = set_sso_user_token("READ")
            assert (token is not None)

    def test_sso_client_credentials_sign_in(self):
        if 'TEST_WITH_REAL_SSO' in os.environ and os.environ['TEST_WITH_REAL_SSO'] == 'true':
            token = set_sso_client_credential_token()
            assert(token is not None)

    def test_sso_client_credentials_from_view(self):
        authorization = set_auth_header(self.write_access_token)
        response = self.client.get("/generate-client-credential-token/", format='json', **authorization)
        assert response.status_code == 200

    def test_token_utils(self):
        if 'TEST_WITH_REAL_SSO' in os.environ and os.environ['TEST_WITH_REAL_SSO'] == 'true':
            token = set_sso_user_token("ADMIN")
            decoded_token = jwt.decode(token, options={"verify_signature": False})
            assert (decoded_token is not None)

            # Test valid kid value
            kid_value = get_kid(token)
            assert(kid_value is not None)

        # Temporarily change to 'TEST_WITH_REAL_SSO', 'false' to test corrupted jwt
        os.environ['TEST_WITH_REAL_SSO'] = 'false'

        # Modify payload and headers value
        private_key = generate_key()
        assert(private_key is not None)

        public_key = private_key.public_key
        assert (public_key is not None)

        payload, headers = get_payload_and_headers(['DEV_CSST_MDP_Cockpit_USER_WRITE'])
        assert(payload is not None and headers is not None)

        headless_encoded_token = jwt.encode(payload, private_key, headers=None, algorithm="RS256")
        assert(headless_encoded_token is not None)

        # Test invalid kid value
        with self.assertRaises(InvalidToken):
            get_kid(headless_encoded_token)

        # Test invalid jwk related value access
        with self.assertRaises(CommunicationError):
            get_jwk("made-up-kid", tenant_id="made-up-tenant-id")

        with self.assertRaises(CommunicationError):
            get_jwks_uri("made-up-tenant-id")

        with self.assertRaises(CommunicationError):
            get_jwks("made-up-tenant-id")

        # Test wrong client access
        payload['aud'] = 'Made-up-aud'
        correctly_encoded_token = jwt.encode(payload, private_key, headers=headers, algorithm="RS256")
        assert (correctly_encoded_token is not None)

        authorization = set_auth_header(correctly_encoded_token)
        response = self.client.get("/forms/regions/", format='json', **authorization)
        result = json.loads(response.content.decode())["detail"]
        assert response.status_code == 403
        assert "ValidationError" in result

        # Test expired token
        payload, headers = get_payload_and_headers(['DEV_CSST_MDP_Cockpit_USER_ADMIN'])
        assert(payload is not None and headers is not None)
        payload['exp'] = -1
        correctly_encoded_token = jwt.encode(payload, private_key, headers=headers, algorithm="RS256")
        authorization = set_auth_header(correctly_encoded_token)
        response = self.client.get("/forms/regions/", format='json', **authorization)
        result = json.loads(response.content.decode())["detail"]
        assert response.status_code == 403
        assert "Invalid Token" in result