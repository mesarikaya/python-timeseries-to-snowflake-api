"""
Django settings for cp_snowflake_api project.

Generated by 'django-admin startproject' using Django 3.2.14.

For more information on this file, see
https://docs.djangoproject.com/en/3.2/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/3.2/ref/settings/
"""

from pathlib import Path
import os
from socket import gethostname, gethostbyname_ex, gethostname
from dotenv import load_dotenv

load_dotenv()

import sys

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_DIR = Path(__file__).resolve().parent

# Take environment variables from .env file
dotenv_path = os.path.join(ENV_DIR, '.env')
load_dotenv(dotenv_path=dotenv_path)

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.2/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-jl@%ixac$)nw7(jzs44na#a0=u)8*iznwl@d)atr@jld5hu@-e'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True if os.getenv('DEBUG') == 'True' else False
DJANGO_LOG_LEVEL = os.getenv('LOG_LEVEL')
# print('Local environment:', os.getenv('LOCAL_ENVIRONMENT'))
HOSTS = []
if os.getenv('LOCAL_ENVIRONMENT') == 'true':
    HOSTS = ['*']
else:
    if os.getenv('ENVIRONMENT') == 'dev':
        HOSTS = ['cockpit-snowflake-api.dev.cglcloud.in', 'localhost', '127.0.0.1', '100.64.129.211',
                 'cockpitsnowflakeapi.boscdataservice', gethostname(), ] + gethostbyname_ex(gethostname())[2]
    elif os.getenv('ENVIRONMENT') == 'stage':
        HOSTS = ['cockpit-snowflake-api.stage.cglcloud.in', 'cockpitsnowflakeapi.boscdataservice', gethostname(), ] + \
                gethostbyname_ex(gethostname())[2]
    elif os.getenv('ENVIRONMENT') == 'prod':
        HOSTS = ['cockpit-snowflake-api.cglcloud.com', 'cockpitsnowflakeapi.boscdataservice', gethostname(), ] + \
                gethostbyname_ex(gethostname())[2]
    else:
        ##Added as a contigency in case the backend cannot load the env variable
        HOSTS = ['*']

ALLOWED_HOSTS = HOSTS

# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'rest_framework.authtoken',
    'corsheaders',
    'snowflake_drf',
    'snowflake_wrapper',
    'drf_spectacular',
    'django_celery_results',
    'django_celery_beat',
]

# Authentication User model
AUTH_USER_MODEL = 'snowflake_drf.User'

# Azure Token Generation
CLIENT_CREDENTIAL_ID = os.environ.get("CLIENT_CREDENTIAL_ID")
CLIENT_CREDENTIAL_SECRET = os.environ.get("CLIENT_CREDENTIAL_SECRET")
RESOURCE_ID = os.environ.get("RESOURCE_ID")
TENANT_ID = os.environ.get("TENANT_ID")

# Snowflake connection config
SNOWFLAKE_CONNECTION_MODE = os.getenv('SNOWFLAKE_CONNECTION_MODE')  # keypair or oauth
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
SNOWFLAKE_OAUTH_CLIENT_ID = os.getenv('SNOWFLAKE_OAUTH_CLIENT_ID')
SNOWFLAKE_OAUTH_CLIENT_SECRET = os.getenv('SNOWFLAKE_OAUTH_CLIENT_SECRET')
SNOWFLAKE_OAUTH_RESOURCE_ID = os.getenv('SNOWFLAKE_OAUTH_RESOURCE_ID')
SNOWFLAKE_OAUTH_USER = os.getenv('SNOWFLAKE_OAUTH_USER')

# Snowflake OAuth 2.0 config
JWT_CLIENT_ID = os.getenv('JWT_CLIENT_ID')
JWT_CLIENT_SECRET = os.getenv('JWT_CLIENT_SECRET')
JWT_AUTH_URL = os.getenv('JWT_AUTH_URL')

REST_FRAMEWORK = {
    # Use Django's standard `django.contrib.auth` permissions,
    # or allow read-only access for unauthenticated users.
    'DEFAULT_AUTHENTICATION_CLASSES': [],
    'TEST_REQUEST_DEFAULT_FORMAT': 'json',
    'DEFAULT_SCHEMA_CLASS': 'drf_spectacular.openapi.AutoSchema',
}

SPECTACULAR_SETTINGS = {
    'TITLE': 'Cockpit Snowflake API',
    'DESCRIPTION': 'Interface between Cockpit and Snowflake Schema',
    'VERSION': '1.0.0',
    'SERVE_INCLUDE_SCHEMA': False,
}

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'corsheaders.middleware.CorsMiddleware',
]

# Gunicorn config
WORKERS = 3
CONNECTIONS = 1000

ROOT_URLCONF = 'cp_snowflake_api.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'cp_snowflake_api.wsgi.application'

# Database
# https://docs.djangoproject.com/en/3.2/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.getenv("DB_NAME", "TEST"),
        'HOST': os.getenv("DB_HOST"),
        'PORT': os.getenv("DB_PORT"),
        'USER': os.getenv("DB_USERNAME"),
        'PASSWORD': os.getenv("DB_PASSWORD"),
    }
}

if 'test' in sys.argv or 'test\_coverage' in sys.argv:  # Covers regular testing and django-coverage
    DATABASES['default']['ENGINE'] = 'django.db.backends.sqlite3'
    DATABASES['default']['NAME'] = ':memory:'

# Password validation
# https://docs.djangoproject.com/en/3.2/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
# https://docs.djangoproject.com/en/3.2/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.2/howto/static-files/

STATIC_URL = '/static/'

# Default primary key field type
# https://docs.djangoproject.com/en/3.2/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# REDIS
REDIS_PRIMARY_ENDPOINT = os.getenv("REDIS_PRIMARY_ENDPOINT")
REDIS_PORT = os.getenv("REDIS_PORT", 6379)
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_DB = os.getenv("REDIS_DB", 1)
REDIS_PROTOCOL = os.getenv("REDIS_PROTOCOL", "rediss")

REDIS_URL = f'{REDIS_PROTOCOL}://:{REDIS_PASSWORD}@{REDIS_PRIMARY_ENDPOINT}:{REDIS_PORT}'

REDIS_LOCAL_URL = "redis://127.0.0.1:6379"

CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": REDIS_URL,
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient"
        },
        "KEY_PREFIX": "cpsnowflake"
    }
}

# Cache time to live is 5 minutes.
CACHE_TTL = 60 * 5

# CORS Configuration -#Do we really want to allow all? Api gateway will probably need more specific setup than here
CORS_ALLOWED_ORIGINS = [
    "https://api.cglcloud.com",
    "https://api-dev.dev.dev-cglcloud.com",
    "https://api-stage.stage.cglcloud.in",
    "http://127.0.0.1:3000",
    "http://localhost:3000",
    "http://127.0.0.1:8000"
]

CORS_ALLOW_HEADERS = [
    'accept',
    'accept-encoding',
    'X-Auth-Token',
    'authorization',
    'content-type',
    'dnt',
    'origin',
    'user-agent',
    'x-csrftoken',
    'x-requested-with',
    'cache-control'
]

CORS_PREFLIGHT_MAX_AGE = 3600
CORS_ALLOW_CREDENTIALS = True

CORS_ALLOW_METHODS = [
    'DELETE',
    'GET',
    'OPTIONS',
    'PATCH',
    'POST',
    'PUT',
]

# LOGGING
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(name)s[%(lineno)d]: %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': os.getenv('LOG_LEVEL'),
            'formatter': 'verbose',
        },
    },
    'root': {
        'handlers': ['console'],
        'level': os.getenv('LOG_LEVEL'),
    },
    'loggers': {
        # Setup the default logger to log everything to the console.
        '': {
            'handlers': ['console', ],
            'level': os.getenv('LOG_LEVEL'),
            'propagate': True,
            'formatter': 'verbose',
        },
        'requests.packages.urllib3': {
            # remove the urlllib3
            'handlers': ['console', ],
            'level': 'CRITICAL',
            'propagate': False,
        }

    }
}

# Element Integration
ELEMENT_URL = os.getenv('ELEMENT_URL')
ELEMENT_USERNAME = os.getenv('ELEMENT_USERNAME')
ELEMENT_PASSWORD = os.getenv('ELEMENT_PASSWORD')
ELEMENT_REGION_PIPELINE = os.getenv('ELEMENT_REGION_PIPELINE')
ELEMENT_REGION_FLOW = os.getenv('ELEMENT_REGION_FLOW')
ELEMENT_TECHNOLOGY_PIPELINE = os.getenv('ELEMENT_TECHNOLOGY_PIPELINE')
ELEMENT_TECHNOLOGY_FLOW = os.getenv('ELEMENT_TECHNOLOGY_FLOW')
ELEMENT_PLANT_PIPELINE = os.getenv('ELEMENT_PLANT_PIPELINE')
ELEMENT_PLANT_FLOW = os.getenv('ELEMENT_PLANT_FLOW')
ELEMENT_PLANTTECHNOLOGY_PIPELINE = os.getenv('ELEMENT_PLANTTECHNOLOGY_PIPELINE')
ELEMENT_PLANTTECHNOLOGY_FLOW = os.getenv('ELEMENT_PLANTTECHNOLOGY_FLOW')
ELEMENT_MTPM_PIPELINE = os.getenv('ELEMENT_MTPM_PIPELINE')
ELEMENT_MTPM_FLOW = os.getenv('ELEMENT_MTPM_FLOW')
ELEMENT_LEADING_INDICATOR_PIPELINE = os.getenv('ELEMENT_LEADING_INDICATOR_PIPELINE')
ELEMENT_LEADING_INDICATOR_FLOW = os.getenv('ELEMENT_LEADING_INDICATOR_FLOW')
ELEMENT_ORG_ID = os.getenv('ELEMENT_ORG_ID')
ELEMENT_PROTOCOL = os.getenv('ELEMENT_PROTOCOL')