"""cp_snowflake_api URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from django.urls import path, include
from snowflake_drf import views
from drf_spectacular.views import (
    SpectacularJSONAPIView,
    SpectacularRedocView,
    SpectacularSwaggerView,
    SpectacularYAMLAPIView,
)

urlpatterns = [
    path('admin/', admin.site.urls),
    path('snowflake/regions/', views.get_snowflake_regions),
    path('snowflake/plants/', views.get_snowflake_plants),
    path('snowflake/technologies/', views.get_snowflake_technologies),
    path('snowflake/mtpm/', views.get_snowflake_mptm_list),
    path('snowflake/mtpm/metrics/', views.get_snowflake_mptm_targets),
    path('snowflake/mtpm/metrics/opportunity/', views.get_snowflake_mptm_opportunities),
    path('snowflake/mtpm/leadingindicators/', views.get_snowflake_mtpm_dim_leading_indicators),
    path('snowflake/mtpm/leadingindicators/summary/', views.get_snowflake_mtpm_leading_indicators_summary),
    path('snowflake/mtpm/leadingindicators/metrics/', views.get_snowflake_mtpm_metric_leading_indicators),
    path('snowflake/mtpm/leadingindicators/metrics/beta/', views.get_snowflake_mtpm_metric_leading_indicators_beta),
    path('snowflake/mtpm/equipmenttags/', views.get_snowflake_mtpm_dim_equipment_tags),
    path('snowflake/mtpm/equipmenttags/metrics/', views.get_snowflake_mtpm_metric_equipment_tags),
    path('snowflake/mtpm/equipmenttags/metrics/beta/', views.get_snowflake_mtpm_metric_equipment_tags_beta),
    path('forms/', include('snowflake_drf.form_urls')),
    path('monitoring/snowflake/connection/', views.snowflake_persistent_connection_available),
    path('monitoring/snowflake/usable/', views.snowflake_is_usable),
    path('test-authentication/', views.test_authentication),
    path('generate-sso-token/', views.generate_token_with_sso),
    path('generate-azure-oauth-token/', views.generate_azure_oauth_token),
    path('generate-client-credential-token/', views.generate_token_with_client_credential),
    path('healthcheck/', views.health_check, name="TEST!!!!!"),
    path('api/schema/', SpectacularJSONAPIView.as_view(), name='schema'),
    path('api/schema/swagger-ui/', SpectacularSwaggerView.as_view(url_name='schema'), name='swagger-ui'),
    path('api/schema/redoc/', SpectacularRedocView.as_view(url_name='schema'), name='redoc'),
    path('api/schema/yaml/', SpectacularYAMLAPIView.as_view(), name='yaml'),
]

urlpatterns += staticfiles_urlpatterns()