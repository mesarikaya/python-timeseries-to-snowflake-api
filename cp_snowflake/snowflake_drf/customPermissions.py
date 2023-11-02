import logging
import os

import jwt
from rest_framework import exceptions
from rest_framework import permissions
from rest_framework.exceptions import AuthenticationFailed, ValidationError

from .token_utils import get_public_key, check_token_expiration

log = logging.getLogger(__name__)


class IsReadOnlyGroup(permissions.BasePermission):
    """
        Permissions for READ access level - aimed for Dynamic Charts
    """

    def has_permission(self, request, view):
        return is_authorized(request, ["SNOWFLAKE_USER_READ"])


class IsWriteGroup(permissions.BasePermission):
    """
        Permissions for READ and WRITE access level - aimed for forms ONLY
    """

    def has_permission(self, request, view):
        return is_authorized(request, ["SNOWFLAKE_USER_READ", "USER_WRITE"])


class IsAdminGroup(permissions.BasePermission):
    """
        Permissions for ADMIN access level - aimed for general admin tasks
    """

    def has_permission(self, request, view):
        return is_authorized(request, ["SNOWFLAKE_USER_READ", "USER_WRITE", "USER_ADMIN"])


def is_authorized(request, access_levels):
    # Change this to HTTP_AUTHORIZATION - HTTP_AZURETOKEN
    log.debug("CHECKING ACCESS_LEVELs %s", access_levels)
    metadata = request.META
    authorization = metadata.get('HTTP_AUTHORIZATION')

    if authorization is None:
        log.info('AUTH_ERROR_SC: No Token Provided')
        raise exceptions.ValidationError('AUTH_ERROR_SC: No Token Provided')

    try:
        available_groups, decodedToken, userGroupMembership, user_name = set_user_auth_details(authorization)

        # Checking Client ID and Issuer for developers
        hasEntitlements = any(item in userGroupMembership for item in available_groups)
        if not hasEntitlements:
            raise AuthenticationFailed('The entitlements dont match, you cannot access our data.')
        else:
            if decodedToken['aud'] == os.environ.get("CLIENT_ID"):
                log.debug("Has all required access levels: %s",
                          all(access_level in userGroupMembership for access_level in access_levels))
                return all(access_level in userGroupMembership for access_level in access_levels)
            else:
                raise ValidationError(f'Authentication failed with request meta: {metadata}')
    except jwt.DecodeError as wrong_token_type:
        log.debug(f"Error {wrong_token_type=}, {type(wrong_token_type)=}")
        raise exceptions.AuthenticationFailed('Invalid Token')
    except ValueError as value_error:
        log.debug(f"Error {value_error=}, {type(value_error)=}")
        raise exceptions.AuthenticationFailed(f"Error {value_error=}, {type(value_error)=}")
    except OSError as os_error:
        log.debug(f"Error {os_error=}, {type(os_error)=}")
        raise exceptions.AuthenticationFailed(f"Error {OSError=}, {type(OSError)=}")
    except Exception as err:
        log.debug(f"Unexpected Error {err=}, {type(err)=}")
        raise exceptions.AuthenticationFailed(f"Unexpected Error {err=}, {type(err)=}")


def set_user_auth_details(authorization):
    # Splitting the authentication header to get the access token
    authorization = authorization.split()
    token = authorization[1]
    decodedToken = decode_token(token)
    userGroupMembership, user_name, available_groups = set_group_memberships(decodedToken)
    return available_groups, decodedToken, userGroupMembership, user_name


def decode_token(token):
    # This is necessary as there is no other way to make integration tests with real user
    # based token retrieval from Azure
    if 'TEST_WITH_REAL_SSO' in os.environ and os.environ['TEST_WITH_REAL_SSO'] == 'false':
        # this is an unsafe verification
        # in normal runtime, proper signature verification is done with the next branch
        decodedToken = jwt.decode(token, options={"verify_signature": False})
    else:
        tenant_id = os.environ.get("TENANT_ID")
        public_key = get_public_key(token, tenant_id)
        # Try to get a token for a SSO client ID
        try:
            decodedToken = jwt_decode(public_key, tenant_id, token, os.environ.get("CLIENT_ID"))
        except:
            # Fallback to get the token from a resource (client credential)
            auds = os.environ.get("AUTHORIZED_AUDS").split(',')
            decodedToken = jwt_decode(public_key, tenant_id, token, auds)

    check_token_expiration(decodedToken)

    return decodedToken


def set_group_memberships(decodedToken):
    uEntitlements = list()
    role_prefix = os.environ.get("ROLE_PREFIX")
    role_suffix = os.environ.get("ROLE_SUFFIX")
    if 'roles' in decodedToken:
        for role in decodedToken['roles']:
            if role.startswith(role_prefix):
                role_rw = role.split(role_prefix)[1].upper()
                if role_suffix is not None and role_suffix in role_rw:
                    role_rw = role_rw.replace(role_suffix, '')
                uEntitlements.append(role_rw)

    # Set user group memberships
    userGroupMembership = set()
    user_name = decodedToken['name']
    available_groups = ['USER_WRITE', 'SNOWFLAKE_USER_READ', 'USER_READ', 'USER_ADMIN']
    for entitlement in uEntitlements:
        if entitlement in available_groups:
            if entitlement == 'SNOWFLAKE_USER_READ' or entitlement == 'USER_READ':
                userGroupMembership.add("SNOWFLAKE_USER_READ")
            if entitlement == 'USER_WRITE':
                userGroupMembership.add(entitlement)
                userGroupMembership.add('SNOWFLAKE_USER_READ')
            elif entitlement == 'USER_ADMIN':
                userGroupMembership.add(entitlement)
                userGroupMembership.add('USER_WRITE')
                userGroupMembership.add('SNOWFLAKE_USER_READ')
    return userGroupMembership, user_name, available_groups


def jwt_decode(public_key, tenant_id, token, audience):
    decodedToken = jwt.decode(token,
                              public_key,
                              options={"verify_signature": True},
                              algorithms=['RS256'],
                              issuer='https://sts.windows.net/{tenant_id}/'.format(tenant_id=tenant_id),
                              audience=audience)
    return decodedToken