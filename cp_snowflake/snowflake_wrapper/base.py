import json
import os

import django.http
import requests
import snowflake.connector as Database
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from django.conf import settings
from django.db.backends.base.base import BaseDatabaseWrapper
from django.utils.asyncio import async_unsafe
from snowflake.connector.errors import ForbiddenError, OperationalError, ProgrammingError

from .client import DatabaseClient  # NOQA isort:skip
from .creation import DatabaseCreation  # NOQA isort:skip
from .features import DatabaseFeatures  # NOQA isort:skip
from .introspection import DatabaseIntrospection  # NOQA isort:skip
from .operations import DatabaseOperations  # NOQA isort:skip
from .schema import DatabaseSchemaEditor  # NOQA isort:skip
from snowflake_drf.AzureADToken import AzureADToken
import logging


log = logging.getLogger(__name__)

class SnowflakeWrapper(BaseDatabaseWrapper):
    connection = None
    vendor = 'snowflake'
    display_name = 'Snowflake'
    data_types = {
        'AutoField': 'INTEGER',
        'BigAutoField': 'BIGINT',
        'BinaryField': 'BINARY',
        'BooleanField': 'BOOLEAN',
        'CharField': 'VARCHAR(%(max_length)s)',
        'DateField': 'DATE',
        'DateTimeField': 'TIMESTAMP_TZ',
        'DecimalField': 'FLOAT',
        'DurationField': 'BIGINT',
        'FileField': 'VARCHAR(%(max_length)s)',
        'FilePathField': 'VARCHAR(%(max_length)s)',
        'FloatField': 'FLOAT',
        'IntegerField': 'INTEGER',
        'BigIntegerField': 'BIGINT',
        'IPAddressField': 'VARCHAR(15)',
        'GenericIPAddressField': 'VARCHAR(15)',
        'JSONField': 'VARCHAR',
        'NullBooleanField': 'BOOLEAN',
        'OneToOneField': 'INTEGER',
        'PositiveBigIntegerField': 'BIGINT',
        'PositiveIntegerField': 'INTEGER',
        'PositiveSmallIntegerField': 'INTEGER',
        'SlugField': 'VARCHAR(%(max_length)s)',
        'SmallAutoField': 'VARCHAR',
        'SmallIntegerField': 'VARCHAR',
        'TextField': 'VARCHAR',
        'TimeField': 'TIME',
        'UUIDField': 'VARCHAR(32)',
    }
    data_type_check_constraints = {
        'PositiveBigIntegerField': '"%(column)s" >= 0',
        'PositiveIntegerField': '"%(column)s" >= 0',
        'PositiveSmallIntegerField': '"%(column)s" >= 0',
    }
    operators = {
        'exact': '= %s',
        'iexact': '= UPPER %s',
        'contains': 'LIKE %s',
        'icontains': 'ILIKE %s',
        'regex': 'REGEXP %s',
        'iregex': 'REGEXP_LIKE(%s, i)',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE %s',
        'endswith': 'LIKE %s',
        'istartswith': 'LIKE UPPER(%s)',
        'iendswith': 'LIKE UPPER(%s)',
    }

    pattern_esc = r"REPLACE(REPLACE(REPLACE({}, '\', '\\'), '%%', '\%%'), '_', '\_')"
    pattern_ops = {
        'contains': "LIKE '%%' || {} || '%%'",
        'icontains': "LIKE '%%' || UPPER({}) || '%%'",
        'startswith': "LIKE {} || '%%'",
        'istartswith': "LIKE UPPER({}) || '%%'",
        'endswith': "LIKE '%%' || {}",
        'iendswith': "LIKE '%%' || UPPER({})",
    }

    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor

    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    auth_token = ""

    def __init__(self):
        self.connection = self.get_new_connection({})

    def is_connection_available(self):
        return self.connection is not None

    def get_connection_params(self):
        conn_params = {
            'session_parameters': {},
            'user': settings.SNOWFLAKE_USER,
            'account': settings.SNOWFLAKE_ACCOUNT,
            'warehouse': settings.SNOWFLAKE_WAREHOUSE,
            'database': settings.SNOWFLAKE_DATABASE,
            'schema': settings.SNOWFLAKE_SCHEMA
        }

        if settings.SNOWFLAKE_CONNECTION_MODE == 'oauth':
            self.get_auth_token()
            conn_params['token'] = self.auth_token
            conn_params['user'] = settings.SNOWFLAKE_OAUTH_USER
            conn_params['authenticator'] = settings.SNOWFLAKE_CONNECTION_MODE
        elif settings.SNOWFLAKE_CONNECTION_MODE == 'keypair':
            rsa_private_key = os.getenv('RSA_PRIVATE_KEY')
            if rsa_private_key is None:
                return conn_params
            private_key = serialization.load_pem_private_key(
                rsa_private_key.encode(),
                password=None,
                backend=default_backend())

            key_bytes = private_key.private_bytes(encoding=serialization.Encoding.DER,
                                                  format=serialization.PrivateFormat.PKCS8,
                                                  encryption_algorithm=serialization.NoEncryption())
            conn_params['private_key'] = key_bytes
            if os.getenv('DEBUG'):
                conn_params['insecure_mode'] = True

        return conn_params

    def get_auth_token(self):
        # Azure Auth Configurations
        client_id = settings.SNOWFLAKE_OAUTH_CLIENT_ID  # Client credential app registration id
        client_secret = settings.SNOWFLAKE_OAUTH_CLIENT_SECRET  # Client credential app registration secret
        resource_id = settings.SNOWFLAKE_OAUTH_RESOURCE_ID  # Resource app registration id
        tenant_id = settings.TENANT_ID  # Standard for all Cargill

        self.auth_token = AzureADToken(tenant_id=tenant_id,
                                       client_id=client_id,
                                       client_secret=client_secret,
                                       resource_id=resource_id).client_credential_auth()
        return

    def get_new_connection(self, conn_params):
        try:
            if not (bool(conn_params)):
                conn_params = self.get_connection_params()
            connection = Database.connect(**conn_params)
            return connection
        except ForbiddenError as auth_err:
            # todo check for specific error number e.g. auth_err errno
            # todo this is dependent on reproduction of the error (having a test environment)
            self.get_auth_token()
            conn_params['token'] = self.auth_token
            conn_params['authenticator'] = settings.SNOWFLAKE_CONNECTION_MODE
            connection = Database.connect(**conn_params)
            return connection
        except OperationalError as opp_err:
            # this means there is a connection problem such as a firewall error
            return None
        except ProgrammingError as prog_err:
            # This error is generated when the passkey is empty
            return None

    def init_connection_state(self):
        pass

    @async_unsafe
    def create_cursor(self, name=None):
        return self.connection.cursor()

    def _set_autocommit(self, autocommit):
        with self.wrap_database_errors:
            self.connection.autocommit(autocommit)

    def validate_and_execute(self, query):
        result = []
        if self.connection is None:
            log.debug('snowflake connection is closed')
            self.connection = self.get_new_connection({})
        try:
            with self.connection.cursor() as cursor:
                result = cursor.execute(query).fetchall()
                cursor.close()
        except Database.Error as e:
            log.error(e)
            log.debug('closing and refreshing unhealthy snowflake connection')
            self.connection.close()
            self.connection = self.get_new_connection({})
            for i in range(0, 2):
                if len(result) == 0:
                    log.debug(f'snowflake query retry #{i+1}')
                    with self.connection.cursor() as cursor:
                        result = cursor.execute(query).fetchall()
                        cursor.close()
                else:
                    break

        return result

    def is_usable(self):
        if self.connection is None:
            return False
        try:
            with self.connection.cursor() as cursor:
                cursor.execute('SELECT current_version()')
                cursor.close()
        except Database.Error:
            return False
        else:
            return True

    def _rollback(self):
        try:
            BaseDatabaseWrapper._rollback(self)
        except Database.NotSupportedError:
            pass
