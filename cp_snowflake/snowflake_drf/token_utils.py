import base64
import functools
import logging
import os

import jwt
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicNumbers
from rest_framework.exceptions import AuthenticationFailed

from .AzureADToken import AzureADToken

OID_DISCOVERY_COMMON_URL = 'https://login.microsoftonline.com/common/.well-known/openid-configuration'
OID_DISCOVERY_TENANT_URL = 'https://login.microsoftonline.com/{tenant_id}/.well-known/openid-configuration'

_jwks_cache = {}

log = logging.getLogger(__name__)


class TokenError(Exception):
    pass


class CommunicationError(TokenError):
    pass


class InvalidToken(TokenError):
    pass


def generate_key():
    return rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )

def generate_public_key(private_key):
    return private_key.public_key()


def ensure_bytes(key):
    if isinstance(key, str):
        key = key.encode('utf-8')
    return key


def decode_value(val):
    decoded = base64.urlsafe_b64decode(ensure_bytes(val) + b'==')
    return int.from_bytes(decoded, 'big')


def rsa_pem_from_jwk(jwk):
    return RSAPublicNumbers(
        n=decode_value(jwk['n']),
        e=decode_value(jwk['e'])
    ).public_key(default_backend()).public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )


def _fetch_discovery_meta(tenant_id=None):
    discovery_url = OID_DISCOVERY_TENANT_URL.format(tenant_id=tenant_id) if tenant_id else OID_DISCOVERY_COMMON_URL
    try:
        response = requests.get(discovery_url)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        logging.debug(response.text)
        raise CommunicationError(f'Error getting issuer discovery meta from {discovery_url}', err)
    return response.json()


def get_kid(token):
    headers = jwt.get_unverified_header(token)
    if not headers:
        raise InvalidToken('missing headers')
    try:
        return headers['kid']
    except KeyError:
        raise InvalidToken('missing kid')


def get_jwks_uri(tenant_id=None):
    meta = _fetch_discovery_meta(tenant_id)
    if 'jwks_uri' in meta:
        return meta['jwks_uri']
    else:
        raise CommunicationError('jwks_uri not found in the issuer meta')


@functools.lru_cache
def get_jwks(tenant_id=None):
    jwks_uri = get_jwks_uri(tenant_id)
    try:
        response = requests.get(jwks_uri)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        log.debug("Response text: %s", response.text)
        raise CommunicationError(f'Error getting issuer jwks from {jwks_uri}', err)
    return response.json()


def get_jwk(kid, tenant_id=None):
    for jwk in get_jwks(tenant_id).get('keys'):
        if jwk.get('kid') == kid:
            return jwk
    raise InvalidToken('Unknown kid')


def get_public_key(token, tenant_id=None):
    kid = get_kid(token)
    jwk = get_jwk(kid, tenant_id)
    return rsa_pem_from_jwk(jwk)


def check_token_expiration(token):
    """Checks if the token is unexpired. Returns true if it is valid."""
    try:
        remaining_time = token['exp'] - token['iat']
        if remaining_time <= 0:
            log.debug('Token expired: Remaining time %s', remaining_time)
            raise jwt.ExpiredSignatureError("Expired Token")
        log.debug('Token is not expired: Remaining time %s', remaining_time)
        return True
    except Exception as err:
        log.error('Unexpected error: %s', err)
        raise AuthenticationFailed("Invalid Token")


def get_payload_and_headers(roles=['USER_WRITE', 'SNOWFLAKE_USER_READ', 'USER_ADMIN']):
    exp_time, iat_time, nbf_time = create_token_time_fields()
    payload = {
        "aud": "374b1465-dd0b-483b-b07e-f623e7619a12",
        "iss": "https://sts.windows.net/57368c21-b8cf-42cf-bd0b-43ecd4bc62ae/",
        "iat": iat_time,
        "nbf": nbf_time,
        "exp": exp_time,
        "acr": "1",
        "aio": "ATQAy/8TAAAAJ6/AuYZpiscU7QSXDRx8Q7FYOTlclZKW562sPmOVxOUN8egWNvy/wYyfWc+cAWT4",
        "amr": [
            "pwd"
        ],
        "appid": "374b1465-dd0b-483b-b07e-f623e7619a12",
        "appidacr": "1",
        "family_name": "Sarikaya",
        "given_name": "Ergin",
        "ipaddr": "212.161.9.53",
        "name": "Ergin Sarikaya",
        "oid": "ed5a7a52-6dd8-40d8-b429-c5eb38dea929",
        "onprem_sid": "S-1-5-21-155022373-702559647-618671499-76989513",
        "rh": "0.AQMAIYw2V8-4z0K9C0Ps1LxirmUUSzcL3TtIsH72I-dhmhIDANE.",
        "roles": roles,
        "scp": "User.Read",
        "sub": "K7Wn7Qy8jtYU34Xo0kemjvSaFvjKBcHfz4EEpgwsE_k",
        "tid": "57368c21-b8cf-42cf-bd0b-43ecd4bc62ae",
        "unique_name": "Ergin_Sarikaya@cargill.com",
        "upn": "Ergin_Sarikaya@cargill.com",
        "uti": "aUJRSw7PCUu5ln_bOq4zAQ",
        "ver": "1.0"
    }

    headers = {
        "typ": "JWT",
        "alg": "RS256",
        "x5t": "-KI3Q9nNR7bRofxmeZoXqbHZGew",
        "kid": "-KI3Q9nNR7bRofxmeZoXqbHZGew"
    }
    return payload, headers


def create_token_time_fields():
    from time import time
    iat_time = int(time() * 1000)
    nbf_time = iat_time
    exp_time = iat_time + (60 * 60 * 1000)
    return exp_time, iat_time, nbf_time


def generate_non_sso_token(roles=[
    "DEV_CSST_MDP_Cockpit_User_Write",
    "DEV_CSST_MDP_Cockpit_User_Admin",
    "DEV_CSST_MDP_Cockpit_Snowflake_User_Read"
]):
    """
    API endpoint for generation a test jwt token
    """
    private_key = generate_key()
    public_key = private_key.public_key
    payload, headers = get_payload_and_headers(roles)

    encoded = jwt.encode(payload, private_key, headers=headers, algorithm="RS256")
    return encoded


def generate_client_credentials_token():
    """
    API endpoint for generation a test jwt token
    """
    private_key = generate_key()
    exp_time, iat_time, nbf_time = create_token_time_fields()
    payload = {
        "aud": "a7845ef7-4d03-44aa-af0c-3f4101fd1e59",
        "iss": "https://sts.windows.net/57368c21-b8cf-42cf-bd0b-43ecd4bc62ae/",
        "iat": iat_time,
        "nbf": nbf_time,
        "exp": exp_time,
    }

    headers = {
        "typ": "JWT",
        "alg": "RS256",
        "x5t": "-KI3Q9nNR7bRofxmeZoXqbHZGew",
        "kid": "-KI3Q9nNR7bRofxmeZoXqbHZGew"
    }

    encoded = jwt.encode(payload, private_key, headers=headers, algorithm="RS256")
    return encoded
