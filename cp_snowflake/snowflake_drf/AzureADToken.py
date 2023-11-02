import logging
import os

import msal
from rest_framework import exceptions

log = logging.getLogger(__name__)
logging.getLogger("msal").setLevel(logging.ERROR)


class AzureADToken:
    """ Application Registration is required and proper scope access need to be given.
        :param tenant_id      AD Tenant ID
        :param client_id      App Reg Client ID
        :param client_secret  App Reg Secret
        """

    def __init__(self, tenant_id, client_id, client_secret, resource_id=None, username=None, password=None):
        self._tenant_id = tenant_id
        self._client_id = client_id
        self._client_secret = client_secret
        self._resource_id = resource_id
        self._graph_uri = 'https://graph.microsoft.com'
        # self._scope = ['https://graph.microsoft.com/.default']
        self._scope = [f'{self._client_id}/.default']
        self.username = username
        self.password = password

    def msgraph_auth(self):
        authority = 'https://login.microsoftonline.com/' + self._tenant_id

        verify_setting = True
        log.debug("Is local: %s", os.getenv('LOCAL_ENVIRONMENT'))
        if os.getenv('LOCAL_ENVIRONMENT') == 'true':
            verify_setting = False  # On the local, it gives SSL certificate chain error

        app = msal.ConfidentialClientApplication(client_id=self._client_id,
                                                 authority=authority,
                                                 client_credential=self._client_secret,
                                                 verify=verify_setting)

        try:
            # Acquire an access token for given account, without user interaction.
            # https://msal-python.readthedocs.io/en/latest/#msal.PublicClientApplication.acquire_token_silent
            account = app.get_accounts(username=self.username)
            token_details = app.acquire_token_by_username_password(username=self.username,
                                                                   password=self.password,
                                                                   scopes=self._scope)
            if token_details:
                log.debug('Attempting to create completely new token!', )
                access_token = token_details['access_token']
                return access_token
            else:
                log.debug('Error acquiring authorization token. Check your tenantID, clientid,'
                          ' clientsecret, username and password')
                raise exceptions.AuthenticationFailed('Error acquiring authorization token. '
                                                      'Check your related inputs')
        except KeyError as key_error:
            log.error(f'Error while getting the token from Azure -> {key_error}')
            raise exceptions.AuthenticationFailed('You do not have authorization to access the data. '
                                                  'Please request access or check your credentials.')
        except Exception as err:
            log.error(f"Error while getting the token from Azure {err=}, {type(err)=}")
            raise exceptions.AuthenticationFailed(f"Error {err=}, {type(err)=}")

    def client_credential_auth(self):

        scope = [f'{self._resource_id}/.default']
        authority = f'https://login.microsoftonline.com/{self._tenant_id}'
        if os.getenv('LOCAL_ENVIRONMENT') == 'true':
            verify_setting = False  # On the local, it gives SSL certificate chain error
        else:
            verify_setting = True

        # Creating connection with Azure using the Client Credentials Flow
        app = msal.ConfidentialClientApplication(client_id=self._client_id,
                                                 client_credential=self._client_secret,
                                                 authority=authority, verify=verify_setting)
        try:
            # Acquire an access token for given account, without user interaction.
            # https://msal-python.readthedocs.io/en/latest/#msal.PublicClientApplication.acquire_token_silent
            token_details = app.acquire_token_for_client(scope)
            if token_details:
                access_token = token_details['access_token']
                return access_token
            else:
                log.debug('Error acquiring authorization token. Check your inputs')
                raise exceptions.AuthenticationFailed('Error acquiring authorization token. '
                                                      'Check your related inputs')
        except KeyError as key_error:
            log.error(f'Error while getting the token from Azure -> {key_error}')
            raise exceptions.AuthenticationFailed('You do not have authorization to access the data.')
        except Exception as err:
            logging.error(f'ClientCredentialToken - Something went wrong '
                          f'while getting the token from Azure -> {err}')
            raise exceptions.AuthenticationFailed(f'ClientCredentialToken - Something went wrong while '
                                                  f'getting the token from Azure -> {err}')