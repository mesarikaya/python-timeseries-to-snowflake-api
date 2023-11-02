import json
import os

from rest_framework.exceptions import AuthenticationFailed

from cp_snowflake.snowflake_drf.AzureADToken import AzureADToken

# available access types
access_types = ['READ', 'WRITE', 'ADMIN']


def get_token(content):
    return json.loads(content.decode())["token"]


def set_auth_header(token):
    return {'HTTP_AUTHORIZATION': f'Bearer {token}'}


def set_sso_user_token(access_type):
    """This utility function is for setting user auth token for test purposes.
    access_type should be wither READ, WRITE OR ADMIN"""
    assert (access_type in access_types)

    token_config = set_sso_user_token_config(access_type)
    try:
        token = AzureADToken.msgraph_auth(token_config)
        assert (token is not None)
        return token
    except AuthenticationFailed as err:
        assert False, f'raised an exception {err}'


def set_sso_user_token_config(access_type):
    client_id = os.environ.get("CLIENT_ID")
    tenant_id = os.environ.get("TENANT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")

    assert (access_type in access_types)
    if access_type == "READ":
        username = os.environ.get("TEST_READ_SSO_USERNAME")
        password = os.environ.get("TEST_READ_SSO_PASSWORD")
    elif access_type == "WRITE":
        username = os.environ.get("TEST_WRITE_SSO_USERNAME")
        password = os.environ.get("TEST_WRITE_SSO_PASSWORD")
    else:
        username = os.environ.get("TEST_ADMIN_SSO_USERNAME")
        password = os.environ.get("TEST_ADMIN_SSO_PASSWORD")

    token_config = AzureADToken(tenant_id=tenant_id,
                                client_id=client_id,
                                client_secret=client_secret,
                                username=username,
                                password=password)

    assert (token_config is not None)

    return token_config

def set_sso_client_credential_token():
    """This utility function is for setting client credentials token for test purposes."""
    try:
        token_config = set_sso_client_credential_token_config()
        token = AzureADToken.client_credential_auth(token_config)
        assert (token is not None)
    except AuthenticationFailed as err:
        assert False, f'raised an exception {err}'
    return token

def set_sso_client_credential_token_config():
    client_id = os.environ.get("CLIENT_CREDENTIAL_ID")
    tenant_id = os.environ.get("TENANT_ID")
    client_secret = os.environ.get("CLIENT_CREDENTIAL_SECRET")
    resource_id = os.environ.get("RESOURCE_ID")
    token_config = AzureADToken(tenant_id=tenant_id,
                                client_id=client_id,
                                client_secret=client_secret,
                                resource_id=resource_id)

    assert (token_config is not None)
    return token_config
