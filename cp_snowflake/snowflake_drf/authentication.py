import logging
import os
from datetime import timedelta

import jwt
from django.conf import settings
from django.utils import timezone
# For Azure Token Authentication - Start
from rest_framework import authentication
from rest_framework import exceptions
from rest_framework.authentication import TokenAuthentication
from rest_framework.authtoken.models import Token
from rest_framework.exceptions import AuthenticationFailed

from .customPermissions import set_user_auth_details, decode_token

log = logging.getLogger(__name__)


class Client_Credential_Authentication(authentication.BaseAuthentication):
    """
        Client credential authentication, check if the token's app id or audience exists 
        within the client credential app created for this project
    """

    def authenticate(self, request):

        # Comma separated audiences defined for authorization
        cs_aud_ids = os.environ.get("AUTHORIZED_AUDS")

        if cs_aud_ids is None:
            logging.error('AUTH_ERROR: Authorized audiences not defined')
            raise exceptions.AuthenticationFailed('AUTH_ERROR: Authorized audiences not defined')

        aud_ids = cs_aud_ids.split(',')

        # Getting the headers from the request - STEP 1
        authorization = request.META.get('HTTP_AUTHORIZATION')

        # Splitting the authentication header to get the access token
        if authorization is None:
            logging.error('AUTH_ERROR: Token not present')
            raise exceptions.AuthenticationFailed('AUTH_ERROR: Token not present')
        else:
            authorization = authorization.split()
            token = authorization[1]

            # Step 2 - Token decode
            try:
                decodedToken = decode_token(token)

                # Trying to get the appId, if not, it will get the audience
                try:
                    client_id = decodedToken['appid']
                except:
                    client_id = decodedToken['aud']

                # Step 3 - Check if appId exists wihtin the app id registration list
                if client_id in aud_ids:
                    return None, True
                else:
                    logging.error('AUTH_ERROR: Invalid Token')
                    raise exceptions.AuthenticationFailed('Invalid Client ID or issuer, try again')
            except jwt.ExpiredSignatureError as expired_token_error:
                log.debug(f"Error {expired_token_error=}, {type(expired_token_error)=}")
                raise exceptions.AuthenticationFailed('Invalid Token')
            except jwt.DecodeError as wrong_token_type:
                log.debug(f"Error {wrong_token_type=}, {type(wrong_token_type)=}")
                raise exceptions.AuthenticationFailed('Invalid Token')

            # Step 4
            return None, False