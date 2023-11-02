import ast
import logging
import operator
import os
from datetime import date, timedelta
import time
from dateutil.relativedelta import relativedelta
from django.db import transaction
from django.conf import settings
from functools import reduce
from django.http import QueryDict
from rest_framework import status
from django.db.models import Q
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiTypes
from rest_framework import viewsets
from rest_framework.decorators import (
    api_view,
    permission_classes,
    authentication_classes
)
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.status import (
    HTTP_200_OK, HTTP_400_BAD_REQUEST, HTTP_201_CREATED,
)

from . import models, serializers, helpers

log = logging.getLogger(__name__)

from django.http import JsonResponse
from .encoders import MdpJSONEncoder
from .queries.methods import SnowflakeWrapper
from .queries.mtpms import Mtpms
from .queries.leading_indicators import LeadingIndicators
from .queries.equipment_tags import EquipmentTags
from .queries.plant_data import PlantData
from .queries.serializers import SnowFlakeMTPMRequestSerializer, SnowFlakeBaseResponseSerializer

from .AzureADToken import AzureADToken
from .models import (
    Region,
    Plant,
    Technology,
    PlantTechnology,
    PlantHistoricalData,
    MonthlyFinancialFormMetric,
    MonthlyFinancialFormMetricValue,
    ConfigurationFormMTPM,
    ConfigurationFormMTPMValue,
    ConfigurationFormLeadingIndicator,
    SnowflakeLogging
)

from .serializers import (
    SSORequestSerializer,
    DefaultResponseSerializer,
    RegionSerializer,
    PlantSerializer,
    TechnologySerializer,
    PlantTechnologySerializer,
    PlantHistoricalDataSerializer,
    MonthlyFinancialFormMetricSerializer,
    ConfigurationFormMTPMSerializer,
    ConfigurationFormLeadingIndicatorSerializer
)

# Permission Classes
from .authentication import Client_Credential_Authentication
from .customPermissions import IsAdminGroup, IsReadOnlyGroup, IsWriteGroup

# changing the viewset JSON returned to include "results" object
from .snowflake_mixins import CreateModelMixin, RetrieveModelMixin, UpdateModelMixin, ListModelMixin

from .monitoring.eventtypes import event_types

class CreateRetrieveUpdateListViewSet(
    CreateModelMixin,
    RetrieveModelMixin,
    UpdateModelMixin,
    ListModelMixin,
    viewsets.GenericViewSet
):
    pass


# Snowflake methods with persistent connections
wrapper = SnowflakeWrapper()
mtpms = Mtpms()
leading_indicators = LeadingIndicators()
equipment_tags = EquipmentTags()
plant_data = PlantData()


@extend_schema(responses={(HTTP_200_OK, 'application/json'): DefaultResponseSerializer})
@api_view(["GET"])
@permission_classes((AllowAny,))
def health_check(request):
    """
    API endpoint for application running health check
    """
    logging.info("health check ok")
    return Response(data={"message": "App is running"}, status=HTTP_200_OK)


@extend_schema(request=SSORequestSerializer, responses={(HTTP_200_OK, 'application/json'): DefaultResponseSerializer})
@api_view(["POST"])
@permission_classes((AllowAny,))
def generate_token_with_sso(request):
    """
    API endpoint to generate of a real sso token
    """
    client_id = os.environ.get("CLIENT_ID")
    tenant_id = os.environ.get("TENANT_ID")
    client_secret = os.environ.get("CLIENT_SECRET")
    data = request.data
    username = data['username']
    password = data['password']
    given_client_secret = data['client-secret']
    if username is None or password is None or given_client_secret is None:
        return Response(data={"message": "username, password and client _secret are required"},
                        status=HTTP_400_BAD_REQUEST)

    if client_secret == given_client_secret:
        token = AzureADToken(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password).msgraph_auth()
        if token:
            return JsonResponse(data={"token": f'{token}'}, status=HTTP_200_OK)
        else:
            return JsonResponse(data={"message": "Failed token Retrieval"}, status=HTTP_400_BAD_REQUEST)

    return JsonResponse(data={"message": "Failed token Retrieval"}, status=HTTP_400_BAD_REQUEST)


@extend_schema(responses={(HTTP_200_OK, 'application/json'): DefaultResponseSerializer})
@api_view(["GET"])
@permission_classes((IsReadOnlyGroup,))
def generate_azure_oauth_token(request):
    """
    API endpoint to generate a client credential grant token using azure
    """
    log.info("generating a azure oauth token")

    # Azure Auth Configurations
    client_id = settings.SNOWFLAKE_OAUTH_CLIENT_ID  # Client credential app registration id
    client_secret = settings.SNOWFLAKE_OAUTH_CLIENT_SECRET  # Client credential app registration secret
    resource_id = settings.SNOWFLAKE_OAUTH_RESOURCE_ID  # Resource app registration id
    tenant_id = settings.TENANT_ID  # Standard for all Cargill

    token = AzureADToken(tenant_id=tenant_id,
                         client_id=client_id,
                         client_secret=client_secret,
                         resource_id=resource_id).client_credential_auth()

    if token:
        return JsonResponse(data={"token": f'{token}'}, status=HTTP_200_OK)
    else:
        return JsonResponse(data={"message": "Failed token Retrieval"}, status=HTTP_400_BAD_REQUEST)


@extend_schema(responses={(HTTP_200_OK, 'application/json'): DefaultResponseSerializer})
@api_view(["GET"])
@permission_classes((IsReadOnlyGroup,))
def generate_token_with_client_credential(request):
    """
    API endpoint to generate a client credential grant token using the msal library
    """
    log.info("generating a client credential token")

    # Azure Auth Configurations
    client_id = settings.CLIENT_CREDENTIAL_ID  # Client credential app registration id
    client_secret = settings.CLIENT_CREDENTIAL_SECRET  # Client credential app registration secret
    resource_id = settings.RESOURCE_ID  # Resource app registration id
    tenant_id = settings.TENANT_ID  # Standard for all Cargill

    token = AzureADToken(tenant_id=tenant_id,
                         client_id=client_id,
                         client_secret=client_secret,
                         resource_id=resource_id).client_credential_auth()

    if token:
        return JsonResponse(data={"token": f'{token}'}, status=HTTP_200_OK)
    else:
        return JsonResponse(data={"message": "Failed token Retrieval"}, status=HTTP_400_BAD_REQUEST)


@extend_schema(responses={(HTTP_200_OK, 'application/json'): DefaultResponseSerializer})
@api_view(["GET"])
@permission_classes([IsAdminGroup])
def test_authentication(request):
    """
    API endpoint for application to test authorization
    """
    log.info("Token authorization endpoint is running")
    return Response(data={"message": "Valid token"}, status=HTTP_200_OK)


class RegionViewSet(CreateRetrieveUpdateListViewSet):
    queryset = Region.objects.all()
    serializer_class = serializers.RegionSerializer

    def get_permissions(self):
        self.permission_classes = [IsReadOnlyGroup, ]
        return super().get_permissions()

    @extend_schema(
        responses={(HTTP_200_OK, 'application/json'): RegionSerializer}
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self, request, *args, **kwargs):
        region_id = request.GET.get('region_id')

        if region_id:
            # filter by a variable captured from url, for example
            return self.queryset.filter(region_id=region_id).values()
        else:
            return self.queryset.all()


class PlantViewSet(CreateRetrieveUpdateListViewSet):
    queryset = Plant.objects.all()
    serializer_class = serializers.PlantSerializer

    def get_permissions(self):
        self.permission_classes = [IsReadOnlyGroup, ]
        return super().get_permissions()

    @extend_schema(parameters=[
        OpenApiParameter('region_id')
    ],
        responses={(HTTP_200_OK, 'application/json'): PlantSerializer}
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self, request, *args, **kwargs):
        region_id = request.GET.get('region_id')

        if region_id:
            # filter by a variable captured from url, for example
            return self.queryset.filter(region_id=region_id).values()
        else:
            return self.queryset.all()


class TechnologyViewSet(CreateRetrieveUpdateListViewSet):
    queryset = Technology.objects.all()
    serializer_class = serializers.TechnologySerializer

    def get_permissions(self):
        self.permission_classes = [IsReadOnlyGroup, ]
        return super().get_permissions()

    @extend_schema(parameters=[
        OpenApiParameter('plant_id'),
        OpenApiParameter('technology_id')
    ],
        responses={(HTTP_200_OK, 'application/json'): TechnologySerializer}
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self, request, *args, **kwargs):
        plant_id = request.GET.get('plant_id')
        technology_id = request.GET.get('technology_id')
        if technology_id:
            return self.queryset.filter(technology_id=technology_id).values()
        elif plant_id:
            plant_technologies = PlantTechnology.objects.filter(plant__plant_id=plant_id)
            return self.queryset.filter(technology_id__in=plant_technologies.values('technology_id'))
        else:
            return self.queryset.all()


class PlantTechnologyViewSet(CreateRetrieveUpdateListViewSet):
    queryset = PlantTechnology.objects.all()
    serializer_class = serializers.PlantTechnologySerializer

    def get_permissions(self):
        self.permission_classes = [IsReadOnlyGroup, ]
        return super().get_permissions()

    @extend_schema(parameters=[
        OpenApiParameter('plant_id')
    ],
        responses={(HTTP_200_OK, 'application/json'): PlantTechnologySerializer}
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self, request, *args, **kwargs):
        plant_id = request.GET.get('plant_id')
        if plant_id:
            return self.queryset.filter(plant_id=plant_id)
        else:
            return self.queryset.all()


class PlantHistoricalDataViewSet(CreateRetrieveUpdateListViewSet):
    queryset = PlantHistoricalData.objects.all()
    serializer_class = serializers.PlantHistoricalDataSerializer

    def get_permissions(self):
        self.permission_classes = [IsReadOnlyGroup, ]
        return super().get_permissions()

    @extend_schema(
        responses={(HTTP_200_OK, 'application/json'): PlantTechnologySerializer}
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self, request, *args, **kwargs):
        # stub for consistency, assumes future filter capabilities
        return self.queryset.all()


class MonthlyFinancialFormMetricViewSet(CreateRetrieveUpdateListViewSet):
    queryset = MonthlyFinancialFormMetric.objects.all()
    serializer_class = serializers.MonthlyFinancialFormMetricSerializer

    def get_permissions(self):
        self.permission_classes = [IsReadOnlyGroup, ]
        return super().get_permissions()

    @extend_schema(parameters=[
        OpenApiParameter('technology_id')
    ],
        responses={(HTTP_200_OK, 'application/json'): MonthlyFinancialFormMetricSerializer}
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self, request, *args, **kwargs):
        technology_id = request.GET.get('technology_id')

        if technology_id:
            return self.queryset.filter(technology_id=technology_id).values()
        else:
            return self.queryset.all()


class MonthlyFinancialFormMetricIngestionViewSet(CreateRetrieveUpdateListViewSet):
    queryset = MonthlyFinancialFormMetric.objects.all()
    serializer_class = serializers.MonthlyFinancialFormMetricSerializer
    authentication_classes = [Client_Credential_Authentication]

    @extend_schema(parameters=[
        OpenApiParameter('technology_id')
    ],
        responses={(HTTP_200_OK, 'application/json'): MonthlyFinancialFormMetricSerializer}
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self, request, *args, **kwargs):
        technology_id = request.GET.get('technology_id')

        if technology_id:
            return self.queryset.filter(technology_id=technology_id).values()
        else:
            return self.queryset.all()


class MonthlyFinancialFormMetricValueViewSet(CreateRetrieveUpdateListViewSet):
    queryset = MonthlyFinancialFormMetricValue.objects.all()
    serializer_class = serializers.MonthlyFinancialFormMetricValueSerializer

    # Keeping this conditional permission setting for future possible cases. If not needed, should remove and set
    # permission on class level
    def get_permissions(self):
        if self.action in ['list', 'retrieve']:
            self.permission_classes = [
                IsReadOnlyGroup, ]  # Keeping this conditional as example for future possible cases
        else:
            self.permission_classes = [IsWriteGroup, ]
        return super().get_permissions()

    @extend_schema(
        request=serializers.MonthlyFinancialFormMetricValueSerializer,
        responses={(HTTP_201_CREATED, 'application/json'): serializers.MonthlyFinancialFormMetricValueSerializer}
    )
    def create(self, request, *args, **kwargs):
        log.debug("args: %s", args)

        metric_value = None
        metric_value_string = None

        # Collect datetime values
        value_timestamp_local = self.request.data.get('value_timestamp_local')
        value_timestamp_utc = self.request.data.get('value_timestamp_utc')

        # Collect filter and update parameters
        plant_technology_id = self.request.data.get('plant_technology_id')
        monthly_financial_form_metric_id = self.request.data.get('monthly_financial_form_metric_id')
        plant_technology_path = self.request.data.get('plant_technology_path')
        user_created = self.request.data.get('user_created')
        user_updated = self.request.data.get('user_updated')     

        is_numeric = self.request.data.get('is_numeric')
        
        if is_numeric:
            metric_value = self.request.data.get('metric_value')
        else:
            metric_value_string = self.request.data.get('metric_value')
        
        # Update or create initial utc date metric_value
        MonthlyFinancialFormMetricValue.objects.update_or_create(
            plant_technology_id=plant_technology_id,
            monthly_financial_form_metric_id=monthly_financial_form_metric_id,
            plant_technology_path=plant_technology_path,
            value_timestamp_utc=value_timestamp_utc,
            value_timestamp_local=value_timestamp_local,
            defaults={'user_created':user_created,
                      'user_updated':user_updated, 
                      "metric_value": metric_value, 
                      "metric_value_string": metric_value_string}
        )

        # Post-update, validate if request comes from a prev month and propagate the value entered for the same metric to future months
        request_date_utc = helpers.get_datetime_obj(value_timestamp_utc)
        request_month = request_date_utc.month
        current_date = date.today()
        current_month = current_date.month

        # Get delta to check if we need to change values for prev month
        delta_date = current_date + relativedelta(months=-1)

        # This means updates should be made for previous and current months
        if delta_date.month == request_month:
            # Filter by the monthly form metric id and dates greater than the month entered
            curr_values = self.queryset.filter(plant_technology_path=plant_technology_path, monthly_financial_form_metric_id=monthly_financial_form_metric_id, value_timestamp_utc__gte=request_date_utc)

            for value in curr_values:

                iter_month = helpers.get_datetime_obj(value.value_timestamp_utc).month

                if not value.plant_technology.support_daily_financial_entries and (iter_month == request_month or iter_month == current_month):
                    # Update or create initial utc date metric_value
                    MonthlyFinancialFormMetricValue.objects.update_or_create(
                    plant_technology_id=value.plant_technology_id,
                    monthly_financial_form_metric_id=value.monthly_financial_form_metric_id,
                    plant_technology_path=value.plant_technology_path,
                    value_timestamp_utc=value.value_timestamp_utc,
                    value_timestamp_local=value.value_timestamp_local,
                    defaults={'user_created':value.user_created,
                                'user_updated':value.user_updated, 
                                "metric_value": metric_value, 
                                "metric_value_string": metric_value_string}
                    )

        return JsonResponse({'results': list(self.request.data)}, status=status.HTTP_201_CREATED)      

    def put(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)

    def patch(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)

    def partial_update(self, request, pk=None, *args, **kwargs):
        super().partial_update(request, *args, **kwargs)
        results = self.queryset.filter(monthly_financial_form_metric_value_id=pk)

        if results and len(results) > 0:        
            
            # Post-update, validate if request comes from a prev month and propagate the value entered for the same metric to future months
            request_date_utc = helpers.get_datetime_obj(results[0].value_timestamp_utc)
            request_month = request_date_utc.month
            current_date = date.today()
            current_month = current_date.month

            # Get delta to check if we need to change values for prev month
            delta_date = current_date + relativedelta(months=-1)

            # This means updates should be made for previous and current months
            if delta_date.month == request_month:
                # Filter by the monthly form metric id and dates greater than the month entered
                curr_values = self.queryset.filter(plant_technology_path=results[0].plant_technology_path, monthly_financial_form_metric_id=results[0].monthly_financial_form_metric_id, value_timestamp_utc__gte=request_date_utc)
                
                for value in curr_values:                    

                    iter_month = helpers.get_datetime_obj(value.value_timestamp_utc).month

                    if not value.plant_technology.support_daily_financial_entries and (iter_month == request_month or iter_month == current_month):
                        # Update or create initial utc date metric_value
                        MonthlyFinancialFormMetricValue.objects.update_or_create(
                        plant_technology_id=value.plant_technology_id,
                        monthly_financial_form_metric_id=value.monthly_financial_form_metric_id,
                        plant_technology_path=value.plant_technology_path,
                        value_timestamp_utc=value.value_timestamp_utc,
                        value_timestamp_local=value.value_timestamp_local,
                        defaults={'user_created':value.user_created,
                                'user_updated':value.user_updated, 
                                "metric_value": results[0].metric_value, 
                                "metric_value_string": results[0].metric_value_string}
            )

        return JsonResponse({'results': list(self.request.data)}, status=status.HTTP_200_OK)

    @extend_schema(parameters=[
        OpenApiParameter('monthly_financial_form_metric_value_id'),
        OpenApiParameter('monthly_financial_form_metric_id'),
        OpenApiParameter('plant_technology_id'),
        OpenApiParameter('value_timestamp_local', type=OpenApiTypes.DATETIME),
        OpenApiParameter('value_timestamp_utc', type=OpenApiTypes.DATETIME)

    ])
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self, *args, **kwargs):
        if len(self.request.GET) > 0:
            return self.process_filter_clauses(self.request.GET.copy())

        else:
            return self.queryset.all()

    def process_filter_clauses(self, get_data):
        monthly_financial_form_metric_value_id = self.request.GET.get('monthly_financial_form_metric_value_id')
        monthly_financial_form_metric_id = self.request.GET.get('monthly_financial_form_metric_id')
        plant_technology_id = self.request.GET.get('plant_technology_id')
        value_timestamp_local = self.request.GET.get('value_timestamp_local')
        value_timestamp_utc = get_data.get('value_timestamp_utc')

        filter_clauses = []

        if value_timestamp_local:
            timestamp_arr = value_timestamp_local.split('-')
            year = timestamp_arr[0]
            month = timestamp_arr[1]
            day = timestamp_arr[2].split()[0]

            filter_clauses.append(
                Q(value_timestamp_local__year=year, value_timestamp_local__month=month, value_timestamp_local__day=day))

        if value_timestamp_utc:
            try:
                value_timestamp_utc = helpers.get_datetime_obj(value_timestamp_utc)
                if value_timestamp_utc.year < 2000:
                    log.info('No data found for monthly target form filter query, limit: year 2000. Returning.')
                    return None
                # PBI 225146 - Change to support daily entries, date should bring values equals or less than sent date.
                # This should still support monthly entries since the exact date is passed to get last value for current month
                filter_clauses.append(Q(value_timestamp_utc__lte=value_timestamp_utc))
            except ValueError as err:
                log.error(f'Error occurred while processing filter values for monthly target form: {err}')

        if monthly_financial_form_metric_id:
            # filter by monthly financial form id fk
            filter_clauses.append(Q(monthly_financial_form_metric_id=monthly_financial_form_metric_id))
        if monthly_financial_form_metric_value_id:
            # filter by monthly financial form value id
            filter_clauses.append(Q(monthly_financial_form_metric_value_id=monthly_financial_form_metric_value_id))
        if plant_technology_id:
            # filter by plant technology id
            filter_clauses.append(Q(plant_technology_id=plant_technology_id))

        if len(filter_clauses) < 1:
            return self.queryset.all()

        results = self.queryset.filter(reduce(operator.and_, filter_clauses))

        if len(results) < 1 and value_timestamp_utc:
            try:
                get_data['value_timestamp_utc'] = helpers.get_prev_month(value_timestamp_utc)
                return self.process_filter_clauses(get_data)
            except ValueError as err:
                log.error(f'Error occurred while processing filter values for monthly target form: {err}')
                return results

        # we call order by & truncate once we're sure we have the results we want to return
        if value_timestamp_utc:
            grouped_results = []
            for res in results:
                # Filter results by each id to get last entry value to support daily entries
                filtered = list(filter(lambda a: a.monthly_financial_form_metric_id == res.monthly_financial_form_metric_id, results))
                if len(filtered) > 0:  
                    # Sort on DESC order by the value utc date   
                    sorted_obj = sorted(filtered, key=lambda x: helpers.get_datetime_obj(x.value_timestamp_utc), reverse=True)[0]
                    if sorted_obj not in grouped_results:
                        grouped_results.append(sorted_obj)                
            results = grouped_results

        return results


class IngestionMetricValueViewSet(CreateRetrieveUpdateListViewSet):
    queryset = MonthlyFinancialFormMetricValue.objects.all()
    serializer_class = serializers.MonthlyFinancialFormMetricValueSerializer
    authentication_classes = [Client_Credential_Authentication]

    @extend_schema(parameters=[
        OpenApiParameter('updated_date_greater_than')
    ],
        request=serializers.MonthlyFinancialFormMetricValueSerializer,
        responses={(HTTP_201_CREATED, 'application/json'): serializers.MonthlyFinancialFormMetricValueSerializer}
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self, request, *args, **kwargs):
        updated_date_greater_than = request.GET.get('updated_date_greater_than')

        if updated_date_greater_than:
            # filter by a variable captured from url, for example
            return self.queryset.filter(monthly_financial_form_metric__is_numeric=True, datetime_updated__gt=updated_date_greater_than)
        else:
            return self.queryset.filter(monthly_financial_form_metric__is_numeric=True)


class ConfigurationFormMTPMViewSet(CreateRetrieveUpdateListViewSet):
    queryset = ConfigurationFormMTPM.objects.all()
    serializer_class = serializers.ConfigurationFormMTPMSerializer

    def get_permissions(self):
        self.permission_classes = [IsReadOnlyGroup, ]
        return super().get_permissions()

    @extend_schema(parameters=[
        OpenApiParameter('plant_technology_id')
    ],
        responses={(HTTP_200_OK, 'application/json'): ConfigurationFormMTPMSerializer}
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self, request, *args, **kwargs):
        plant_technology_id = request.GET.get('plant_technology_id')

        if plant_technology_id:
            return self.queryset.filter(plant_technology_id=plant_technology_id).values()
        else:
            return self.queryset.all()


class ConfigurationFormMtpmValueViewSet(CreateRetrieveUpdateListViewSet):
    queryset = ConfigurationFormMTPMValue.objects.all()
    serializer_class = serializers.ConfigurationFormMTPMValueSerializer

    # Keeping this conditional permission setting for future possible cases. If not needed, should remove and set
    # permission on class level
    def get_permissions(self):
        if self.action in ['list', 'retrieve']:
            self.permission_classes = [IsReadOnlyGroup, ]
        else:
            self.permission_classes = [IsWriteGroup, ]
        return super().get_permissions()

    @extend_schema(
        request=serializers.ConfigurationFormMTPMValueSerializer,
        responses={(HTTP_201_CREATED, 'application/json'): serializers.ConfigurationFormMTPMValueSerializer}
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)

    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)

    def partial_update(self, request, *args, **kwargs):
        return super().partial_update(request, *args, **kwargs)

    @extend_schema(parameters=[
        OpenApiParameter('configuration_form_mtpm_id'),
        OpenApiParameter('plant_technology_id')
    ])
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self, *args, **kwargs):
        configuration_form_mtpm_id = self.request.GET.get('configuration_form_mtpm_id')
        plant_technology_id = self.request.GET.get('plant_technology_id')

        filter_clauses = []

        # Handling passing parameter combinations for further filtering
        if configuration_form_mtpm_id:
            filter_clauses.append(Q(configuration_form_mtpm_id=configuration_form_mtpm_id))
            if plant_technology_id:
                filter_clauses.append(Q(plant_technology_id=plant_technology_id))

        if filter_clauses:
            return self.queryset.filter(reduce(operator.and_, filter_clauses))

        return self.queryset.all()


class ConfigurationFormLeadingIndicatorViewSet(CreateRetrieveUpdateListViewSet):
    queryset = ConfigurationFormLeadingIndicator.objects.all()
    serializer_class = serializers.ConfigurationFormLeadingIndicatorSerializer

    def get_permissions(self):
        self.permission_classes = [IsReadOnlyGroup, ]
        return super().get_permissions()

    @extend_schema(parameters=[
        OpenApiParameter('configuration_form_mtpm_id'),
        OpenApiParameter('plant_technology_id')
    ],
        responses={(HTTP_200_OK, 'application/json'): ConfigurationFormLeadingIndicatorSerializer}
    )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self, request, *args, **kwargs):
        configuration_form_mtpm_id = request.GET.get('configuration_form_mtpm_id')
        plant_technology_id = request.GET.get('plant_technology_id')

        if configuration_form_mtpm_id:
            return self.queryset.filter(
                configuration_form_mtpm_id=configuration_form_mtpm_id).values()
        elif plant_technology_id:
            return self.queryset.filter(configuration_form_mtpm__plant_technology__plant_technology_id=plant_technology_id).values()
        else:
            return self.queryset.all()


class ConfigurationFormLeadingIndicatorValueViewSet(CreateRetrieveUpdateListViewSet):
    queryset = models.ConfigurationFormLeadingIndicatorValue.objects.all()
    serializer_class = serializers.ConfigurationFormLeadingIndicatorValueSerializer

    # Keeping this conditional permission setting for future possible cases. If not needed, should remove and set
    # permission on class level
    def get_permissions(self):
        if self.action in ['list', 'retrieve']:
            self.permission_classes = [
                IsReadOnlyGroup, ]  # Keeping this conditional as example for future possible cases
        else:
            self.permission_classes = [IsWriteGroup, ]
        return super().get_permissions()

    @extend_schema(
        request=serializers.ConfigurationFormLeadingIndicatorValueSerializer,
        responses={(HTTP_201_CREATED, 'application/json'): serializers.ConfigurationFormLeadingIndicatorValueSerializer}
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)

    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)

    def partial_update(self, request, *args, **kwargs):
        return super().partial_update(request, *args, **kwargs)

    @extend_schema(parameters=[
        OpenApiParameter('configuration_form_leading_indicator_id'),
        OpenApiParameter('technology_id')
    ], )
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self, *args, **kwargs):
        configuration_form_leading_indicator_id = self.request.GET.get('configuration_form_leading_indicator_id')
        plant_technology_id = self.request.GET.get('plant_technology_id')

        filter_clauses = []

        # Handling passing parameter combinations for further filtering
        if configuration_form_leading_indicator_id:
            filter_clauses.append(Q(configuration_form_leading_indicator_id=configuration_form_leading_indicator_id))
            if plant_technology_id:
                filter_clauses.append(Q(plant_technology_id=plant_technology_id))

        if filter_clauses:
            return self.queryset.filter(reduce(operator.and_, filter_clauses))

        return self.queryset.all()


class AnnualTargetFormMetricValueViewSet(CreateRetrieveUpdateListViewSet):
    queryset = models.AnnualTargetFormMetricValue.objects.all()
    serializer_class = serializers.AnnualTargetFormMetricValueSerializer

    def get_permissions(self):
        if self.action in ['list', 'retrieve']:
            self.permission_classes = [AllowAny, ]
        else:
            self.permission_classes = [IsWriteGroup, ]
        return super().get_permissions()

    @extend_schema(
        request=serializers.AnnualTargetFormMetricValueSerializer,
        responses={(HTTP_201_CREATED, 'application/json'): serializers.AnnualTargetFormMetricValueSerializer}
    )
    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)

    def update(self, request, *args, **kwargs):
        return super().update(request, *args, **kwargs)

    def partial_update(self, request, *args, **kwargs):
        return super().partial_update(request, *args, **kwargs)

    @extend_schema(parameters=[
        OpenApiParameter('annual_target_form_metric_value_id'),
        OpenApiParameter('configuration_form_mtpm_id'),
        OpenApiParameter('plant_technology_id'),
        OpenApiParameter('value_timestamp_local', type=OpenApiTypes.DATETIME),
        OpenApiParameter('value_timestamp_utc', type=OpenApiTypes.DATETIME)

    ])
    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)

    def get_queryset(self, *args, **kwargs):
        if len(self.request.GET) > 0:
            return self.process_filter_clauses(self.request.GET.copy())

        else:
            return self.queryset.all()

    def process_filter_clauses(self, get_data):
        annual_target_form_metric_value_id = get_data.get('annual_target_form_metric_value_id')
        configuration_form_mtpm_id = get_data.get('configuration_form_mtpm_id')
        plant_technology_id = get_data.get('plant_technology_id')
        value_timestamp_local = get_data.get('value_timestamp_local')
        value_timestamp_utc = get_data.get('value_timestamp_utc')

        filter_clauses = []

        if value_timestamp_local:
            filter_clauses.append(Q(value_timestamp_local=value_timestamp_local))

        if value_timestamp_utc:
            try:
                value_timestamp_utc = helpers.get_datetime_obj(value_timestamp_utc)
                if value_timestamp_utc.year < 2000:
                    log.info('No data found for annual target form filter query, limit: year 2000. Returning.')
                    return None
                filter_clauses.append(Q(value_timestamp_utc__range=[value_timestamp_utc.replace(day=1),
                                                                    value_timestamp_utc.replace(
                                                                        day=helpers.get_eom(value_timestamp_utc).day)]))
            except ValueError as err:
                log.error(f'Error occurred while processing filter values for annual target form: {err}')

        if configuration_form_mtpm_id:
            filter_clauses.append(Q(configuration_form_mtpm_id=configuration_form_mtpm_id))

        if annual_target_form_metric_value_id:
            filter_clauses.append(Q(annual_target_form_metric_value_id=annual_target_form_metric_value_id))

        if plant_technology_id:
            filter_clauses.append(Q(plant_technology_id=plant_technology_id))

        if len(filter_clauses) < 1:
            return self.queryset.all()

        results = self.queryset.filter(reduce(operator.and_, filter_clauses))

        if len(results) < 1 and value_timestamp_utc:
            try:
                get_data['value_timestamp_utc'] = helpers.get_prev_month(value_timestamp_utc)
                return self.process_filter_clauses(get_data)
            except ValueError as err:
                log.error(f'Error occurred while processing filter values for annual target form: {err}')
                return results

        # we call order by & truncate once we're sure we have the results we want to return
        if value_timestamp_utc:
            results = results.order_by('-value_timestamp_utc')[:1]

        return results


@extend_schema(parameters=[
    OpenApiParameter(name='region_id', description='Filter by region id')],
    responses={(HTTP_200_OK, 'application/json'): SnowFlakeBaseResponseSerializer})
@api_view(["GET"])
@permission_classes((IsReadOnlyGroup,))
def get_snowflake_regions(request):
    region_id = request.query_params.get('region_id') if 'region_id' in request.query_params else ''

    response = plant_data.get_snowflake_region_dim_data(region_id)
    return JsonResponse({'results': list(response)})


@extend_schema(parameters=[
    OpenApiParameter(name='plant_id', description='Filter by plant id'),
    OpenApiParameter(name='region_id', description='Filter by region id')],
    responses={(HTTP_200_OK, 'application/json'): SnowFlakeBaseResponseSerializer})
@api_view(["GET"])
@permission_classes((IsReadOnlyGroup,))
def get_snowflake_plants(request):
    region_id = request.query_params.get('region_id')
    plant_id = request.query_params.get('plant_id') if 'plant_id' in request.query_params else ''

    response = plant_data.get_snowflake_plant_dim_data(region_id, plant_id)
    return JsonResponse({'results': list(response)})


@extend_schema(parameters=[
    OpenApiParameter(name='plant_id', description='Filter by plant id'),
    OpenApiParameter(name='plant_technology_id', description='Filter by plant technology id')],
    responses={(HTTP_200_OK, 'application/json'): SnowFlakeBaseResponseSerializer})
@api_view(["GET"])
@permission_classes((IsReadOnlyGroup,))
def get_snowflake_technologies(request):
    plant_id = request.query_params.get('plant_id')
    plant_technology_id = request.query_params.get('plant_technology_id')

    response = plant_data.get_snowflake_plant_technology_dim_data(plant_id, plant_technology_id)
    return JsonResponse({'results': list(response)})


@extend_schema(parameters=[
    OpenApiParameter(name='plant_technology_id', description='Filter by plant technology id')],
    responses={(HTTP_200_OK, 'application/json'): SnowFlakeBaseResponseSerializer})
@api_view(["GET"])
@permission_classes((IsReadOnlyGroup,))
def get_snowflake_mptm_list(request):
    plant_technology_id = request.query_params.get('plant_technology_id')

    response = mtpms.get_snowflake_mtpm_dim_data(plant_technology_id)
    return JsonResponse({'results': list(response)})


@extend_schema(
    request=SnowFlakeMTPMRequestSerializer,
    responses={(HTTP_200_OK, 'application/json'): SnowFlakeBaseResponseSerializer})
@api_view(["POST"])
@authentication_classes((Client_Credential_Authentication,))
def get_snowflake_mptm_targets(request):
    response = []

    mtpm_list = None
    ui_level = None

    if 'mtpm' not in request.data and 'ui_level' not in request.data:
        return JsonResponse({'results': 'Missing the list of mtpm ids or a ui_level filter'})

    if 'plant_technology_id' not in request.data:
        return JsonResponse({'results': 'Missing the plant_technology_id filter'})

    if 'datetimestart' not in request.data or 'datetimeend' not in request.data:
        return JsonResponse({'results': 'Datetime fields are required'})

    plant_technology_id = request.data['plant_technology_id']
    datetimestart = request.data['datetimestart']
    datetimeend = request.data['datetimeend']

    if 'preferred_uom' in request.data:
        preferred_uom = request.data['preferred_uom'].lower()
    else:
        preferred_uom = None

    if 'mtpm' in request.data:
        if isinstance(request.data, QueryDict):
            mtpm_list = request.data.getlist('mtpm')
        else:
            mtpm_list = request.data['mtpm']
        response = mtpms.get_snowflake_mtpm_target_data_generic(plant_technology_id, datetimestart, datetimeend,
                                                                  preferred_uom, mtpm_list)
    else:
        ui_level = request.data['ui_level']
        response = mtpms.get_snowflake_mtpm_target_data_generic(plant_technology_id, datetimestart,
                                                                  datetimeend, preferred_uom, None, ui_level)

    return JsonResponse({'results': list(response)})


@extend_schema(
    request=SnowFlakeMTPMRequestSerializer,
    responses={(HTTP_200_OK, 'application/json'): SnowFlakeBaseResponseSerializer})
@api_view(["POST"])
@authentication_classes((Client_Credential_Authentication,))
def get_snowflake_mptm_opportunities(request):

    if 'mtpm' not in request.data:
        return JsonResponse({'results': 'Missing the list of mtpm ids or a ui_level filter'})

    if 'plant_technology_id' not in request.data:
        return JsonResponse({'results': 'Missing the plant_technology_id filter'})

    if 'datetimestart' not in request.data or 'datetimeend' not in request.data:
        return JsonResponse({'results': 'Datetime fields are required'})

    plant_technology_id = request.data['plant_technology_id']
    datetimestart = request.data['datetimestart']
    datetimeend = request.data['datetimeend']

    if 'preferred_uom' in request.data:
        preferred_uom = request.data['preferred_uom'].lower()
    else:
        preferred_uom = None

    if 'mtpm' in request.data:
        if isinstance(request.data, QueryDict):
            mtpm_list = request.data.getlist('mtpm')
        else:
            mtpm_list = request.data['mtpm']
        response = mtpms.get_snowflake_mtpm_target_data_generic(plant_technology_id, datetimestart, datetimeend,
                                                                  preferred_uom, mtpm_list, None, True)

    return JsonResponse({'results': list(response)})


@extend_schema(parameters=[
    OpenApiParameter(name='mtpm_id', description='Filter by mtpm id', required=True),
    OpenApiParameter(name='leading_indicator_id', description='Filter by leading indicator id')],
    responses={(HTTP_200_OK, 'application/json'): SnowFlakeBaseResponseSerializer})
@api_view(["GET"])
@authentication_classes((Client_Credential_Authentication,))
def get_snowflake_mtpm_dim_leading_indicators(request):
    mtpm_id = request.query_params.get('mtpm_id')
    leading_indicator_id = request.query_params.get('leading_indicator_id')

    if mtpm_id is None:
        return JsonResponse({'results': 'Parameter mtpm_id is required'})

    response = leading_indicators.get_snowflake_leading_indicator_dim_data(mtpm_id, leading_indicator_id)

    return JsonResponse({'results': list(response)})


@extend_schema(parameters=[
    OpenApiParameter(name='mtpm_id', description='Filter by mtpm id', required=True),
    OpenApiParameter(name='datetimestart', description='Filter by datetimestart', required=True,
                     type=OpenApiTypes.DATETIME),
    OpenApiParameter(name='datetimeend', description='Filter by datetimeend', required=True,
                     type=OpenApiTypes.DATETIME)],
    responses={(HTTP_200_OK, 'application/json'): SnowFlakeBaseResponseSerializer})
@api_view(["GET"])
@authentication_classes((Client_Credential_Authentication,))
def get_snowflake_mtpm_metric_leading_indicators(request):
    leading_indicator_id = request.query_params.get('leading_indicator_id')
    mtpm_id = request.query_params.get('mtpm_id')
    datetimestart = request.query_params.get('datetimestart')
    datetimeend = request.query_params.get('datetimeend')

    if mtpm_id is None:
        return JsonResponse({'results': 'Parameter mtpm_id is required'})

    if datetimestart is None or datetimeend is None:
        return JsonResponse({'results': 'Datetime fields are required'})

    response = leading_indicators.get_snowflake_leading_indicator_metric_data(leading_indicator_id, mtpm_id, datetimestart,
                                                                   datetimeend)

    return JsonResponse({'results': list(response)})


@extend_schema(parameters=[
    OpenApiParameter(name='mtpm_id', description='Filter by mtpm id', required=True),
    OpenApiParameter(name='datetimestart', description='Filter by datetimestart', required=True,
                     type=OpenApiTypes.DATETIME),
    OpenApiParameter(name='datetimeend', description='Filter by datetimeend', required=True,
                     type=OpenApiTypes.DATETIME),
    OpenApiParameter(name='preferred_uom', description='Metric or Imperial, used to narrow resultset', required=True),
    OpenApiParameter(name='timezone', description='Timezone for the time series local conversion', required=True)],
    responses={(HTTP_200_OK, 'application/json'): SnowFlakeBaseResponseSerializer})
@api_view(["GET"])
@authentication_classes((Client_Credential_Authentication,))
def get_snowflake_mtpm_metric_leading_indicators_beta(request):
    leading_indicator_id = request.query_params.get('leading_indicator_id')
    mtpm_id = request.query_params.get('mtpm_id')
    datetimestart = request.query_params.get('datetimestart')
    datetimeend = request.query_params.get('datetimeend')
    preferred_uom = request.query_params.get('preferred_uom')
    timezone = request.query_params.get('timezone')

    if mtpm_id is None:
        return JsonResponse({'results': 'Parameter mtpm_id is required'})

    if datetimestart is None or datetimeend is None:
        return JsonResponse({'results': 'Datetime fields are required'})

    if preferred_uom is None:
        return JsonResponse({'results': 'Preferred UOM is required'})

    if timezone is None:
        return JsonResponse({'results': 'Timezone is required'})

    response = leading_indicators.get_snowflake_leading_indicator_metric_data_beta(leading_indicator_id, mtpm_id, datetimestart,
                                                                        datetimeend, preferred_uom.lower(), timezone)

    return JsonResponse(encoder=MdpJSONEncoder, data={'results': list(response)})

@extend_schema(request=SnowFlakeMTPMRequestSerializer,
               responses={(HTTP_200_OK, 'application/json'): SnowFlakeBaseResponseSerializer})
@api_view(["POST"])
@permission_classes((IsReadOnlyGroup,))
def get_snowflake_mtpm_leading_indicators_summary(request):
    
    if 'mtpm' not in request.data:
        return JsonResponse({'results': 'Missing the list of mtpm ids filter'})

    if 'datetimestart' not in request.data or 'datetimeend' not in request.data:
        return JsonResponse({'results': 'Datetime fields are required'})

    datetimestart = request.data['datetimestart']
    datetimeend = request.data['datetimeend']

    if 'preferred_uom' in request.data:
        preferred_uom = request.data['preferred_uom'].lower()
    else:
        preferred_uom = None

    if 'top' in request.data:
        top = request.data['top']
    else:
        top = None

    if 'mtpm' in request.data:
        if isinstance(request.data, QueryDict):
            mtpm_list = request.data.getlist('mtpm')
        else:
            mtpm_list = request.data['mtpm']

    response = leading_indicators.get_snowflake_leading_indicator_summary(mtpm_list, datetimestart, datetimeend, preferred_uom, top)

    return JsonResponse(encoder=MdpJSONEncoder, data={'results': list(response)})

@extend_schema(parameters=[
    OpenApiParameter(name='leading_indicator_id', description='Filter by leading indicator id', required=True),
    OpenApiParameter(name='equipment_tag_id', description='Filter by equipment tag id', required=True)],
    responses={(HTTP_200_OK, 'application/json'): SnowFlakeBaseResponseSerializer})
@api_view(["GET"])
@authentication_classes((Client_Credential_Authentication,))
def get_snowflake_mtpm_dim_equipment_tags(request):
    leading_indicator_id = request.query_params.get('leading_indicator_id')
    equipment_tag_id = request.query_params.get('equipment_tag_id')

    if leading_indicator_id is None:
        return JsonResponse({'results': 'Parameter leading_indicator_id is required'})

    response = equipment_tags.get_snowflake_equipment_tag_dim_data(leading_indicator_id, equipment_tag_id)

    return JsonResponse({'results': list(response)})


@extend_schema(parameters=[
    OpenApiParameter(name='leading_indicator_id', description='Filter by leading indicator id', required=True),
    OpenApiParameter(name='datetimestart', description='Filter by datetimestart', required=True,
                     type=OpenApiTypes.DATETIME),
    OpenApiParameter(name='datetimeend', description='Filter by datetimeend', required=True,
                     type=OpenApiTypes.DATETIME),
    OpenApiParameter(name='equipment_tag_id', description='Filter by equipment_tag_id')],
    responses={(HTTP_200_OK, 'application/json'): SnowFlakeBaseResponseSerializer})
@api_view(["GET"])
@authentication_classes((Client_Credential_Authentication,))
def get_snowflake_mtpm_metric_equipment_tags(request):
    leading_indicator_id = request.query_params.get('leading_indicator_id')
    equipment_tag_id = request.query_params.get('equipment_tag_id')
    datetimestart = request.query_params.get('datetimestart')
    datetimeend = request.query_params.get('datetimeend')

    if leading_indicator_id is None:
        return JsonResponse({'results': 'Parameter leading_indicator_id is required'})

    if datetimestart is None or datetimeend is None:
        return JsonResponse({'results': 'Datetime fields are required'})

    response = equipment_tags.get_snowflake_equipment_tag_metric_data(equipment_tag_id, leading_indicator_id, datetimestart,
                                                               datetimeend)

    return JsonResponse({'results': list(response)})


@extend_schema(parameters=[
    OpenApiParameter(name='leading_indicator_id', description='Filter by leading indicator id', required=True),
    OpenApiParameter(name='datetimestart', description='Filter by datetimestart', required=True,
                     type=OpenApiTypes.DATETIME),
    OpenApiParameter(name='datetimeend', description='Filter by datetimeend', required=True,
                     type=OpenApiTypes.DATETIME),
    OpenApiParameter(name='equipment_tag_id', description='Filter by equipment_tag_id'),
    OpenApiParameter(name='preferred_uom', description='Metric or Imperial, used to narrow resultset', required=True),
    OpenApiParameter(name='timezone', description='Timezone for the time series local conversion', required=True)],
    responses={(HTTP_200_OK, 'application/json'): SnowFlakeBaseResponseSerializer})
@api_view(["GET"])
@authentication_classes((Client_Credential_Authentication,))
def get_snowflake_mtpm_metric_equipment_tags_beta(request):
    leading_indicator_id = request.query_params.get('leading_indicator_id')
    equipment_tag_id = request.query_params.get('equipment_tag_id')
    datetimestart = request.query_params.get('datetimestart')
    datetimeend = request.query_params.get('datetimeend')
    preferred_uom = request.query_params.get('preferred_uom')
    timezone = request.query_params.get('timezone')

    if leading_indicator_id is None:
        return JsonResponse({'results': 'Parameter leading_indicator_id is required'})

    if datetimestart is None or datetimeend is None:
        return JsonResponse({'results': 'Datetime fields are required'})

    if preferred_uom is None:
        return JsonResponse({'results': 'Preferred UOM is required'})

    if timezone is None:
        return JsonResponse({'results': 'Timezone is required'})

    response = equipment_tags.get_snowflake_equipment_tag_metric_data_beta(equipment_tag_id, leading_indicator_id,
                                                                    datetimestart, datetimeend, preferred_uom.lower(), timezone)

    return JsonResponse(encoder=MdpJSONEncoder, data={'results': list(response)})

'''
Monitoring methods
'''

@extend_schema(parameters=[],
    responses={(HTTP_200_OK, 'application/json'): DefaultResponseSerializer})
@api_view(["GET"])
@authentication_classes((Client_Credential_Authentication,))
def snowflake_persistent_connection_available(request):
    '''
    Call a basic method that validates the current Snowflake persistent connection.
    '''
    try:
        is_conection_available = wrapper.is_connection_available()
        status_desc = 'OK' if is_conection_available else 'Not Available'

        SnowflakeLogging.objects.create(
                status=status_desc,
                message='Validate open/persistent connection to Snowflake',
                event=event_types['snowflake_conn']
            )
        
    except Exception as err:
        raise err
    status_code = status.HTTP_200_OK if is_conection_available else status.HTTP_400_BAD_REQUEST
    return Response(data={'results': status_desc}, status=status_code)

@extend_schema(parameters=[],
    responses={(HTTP_200_OK, 'application/json'): DefaultResponseSerializer})
@api_view(["GET"])
@authentication_classes((Client_Credential_Authentication,))
def snowflake_is_usable(request):
    '''
    Call a basic method that connects to the Snowflake Cargill host and determine if its available
    '''
    try:
        t1 = time.time()
        is_usable = wrapper.is_usable()
        t2 = time.time()
        duration = round((t2 - t1) * 1000)
        status_desc = 'OK' if is_usable else 'Not Available'

        SnowflakeLogging.objects.create(
                status=status_desc,
                message=f'Duration {duration}ms',
                event=event_types['snowflake_usable']
            )
        
    except Exception as err:
        raise err
    status_code = status.HTTP_200_OK if is_usable else status.HTTP_400_BAD_REQUEST
    return Response(data={'results': status_desc}, status=status_code)

# helper function to decode nested filter calls
def decode_response(response):
    decoded = response.decode("UTF-8")
    return ast.literal_eval(decoded)