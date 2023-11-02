import logging
from dataclasses import dataclass

from rest_framework import serializers
from rest_framework.serializers import ModelSerializer, Serializer, CharField, DateTimeField
from .models import (
    User,
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
    ConfigurationFormLeadingIndicatorValue,
    AnnualTargetFormMetricValue,
    Agent,
    DataTransfer,
    DataViews,
    SequentialDataConfig,
    SequentialData
)

log = logging.getLogger(__name__)

@dataclass
class SSORequest:
    username: str
    password: str
    client_secret: str


class SSORequestSerializer(Serializer):
    username = CharField()
    password = CharField()
    client_secret = CharField()

    def save(self):
        self.username = self.context['username']
        self.password = self.context['password']
        self.client_secret = self.context['client-secret']


@dataclass
class DefaultResponse:
    message: str


class DefaultResponseSerializer(Serializer):
    message = CharField()

    def create(self, validated_data):
        return DefaultResponse(**validated_data)

    def update(self, instance, validated_data):
        instance.message = validated_data.get('message', instance.message)


class UserSerializer(ModelSerializer):
    class Meta:
        fields = ('id', 'first_name', 'last_name', 'username', 'password', 'groups', 'email')
        model = User

    def create(self, validated_data):
        user = User.objects.create(**validated_data)
        user.is_staff = True
        user.save()

        return user


class RegionSerializer(ModelSerializer):
    class Meta:
        model = Region
        fields = ['region_name', 'region_id']


class PlantSerializer(ModelSerializer):
    class Meta:
        model = Plant
        fields = ['plant_name', 'plant_id', 'plant_path', 'reporting_day_start',
                  'timezone', 'utc_offset', 'region_id']


class TechnologySerializer(ModelSerializer):
    class Meta:
        model = Technology
        fields = ['technology_name', 'technology_id']


class PlantTechnologySerializer(ModelSerializer):
    technology = TechnologySerializer(read_only=True)

    class Meta:
        model = PlantTechnology
        fields = ['plant_technology_id', 'plant_technology_path', 'plant_id', 'technology_id', 'support_daily_financial_entries', 'technology']

    def to_representation(self, instance):
        response = super().to_representation(instance)
        response['technology_name'] = instance.technology.technology_name
        response.pop("technology")
        return response


class PlantHistoricalDataSerializer(ModelSerializer):
    class Meta:
        model = PlantHistoricalData
        fields = ['plant_historical_data_id', 'timestamp', 'isDST', 'plant_id']


class MonthlyFinancialFormMetricSerializer(ModelSerializer):
    class Meta:
        model = MonthlyFinancialFormMetric
        fields = [
            'monthly_financial_form_metric_id',
            'metric_name',
            'save_to_pi',
            'pi_attribute_name',
            'uom_metric',
            'uom_imperial',
            'technology_id',
            'display_name',
            'category',
            'subcategory',
            'is_numeric'
        ]


class MonthlyFinancialFormMetricValueSerializer(ModelSerializer):
    monthly_financial_form_metric = MonthlyFinancialFormMetricSerializer(read_only=True)
    monthly_financial_form_metric_id = serializers.PrimaryKeyRelatedField(
        queryset=MonthlyFinancialFormMetric.objects.all(),
        source='monthly_financial_form_metric',
        write_only=True
    )

    plant_technology = PlantTechnologySerializer(read_only=True)
    plant_technology_id = serializers.PrimaryKeyRelatedField(
        queryset=PlantTechnology.objects.all(),
        source='plant_technology',
        write_only=True
    )

    value_timestamp_utc = DateTimeField(format="%Y-%m-%d %H:%M:%S")
    value_timestamp_local = DateTimeField(format="%Y-%m-%d %H:%M:%S")
    datetime_created = DateTimeField(format="%Y-%m-%d %H:%M:%S")
    datetime_updated = DateTimeField(format="%Y-%m-%d %H:%M:%S")

    class Meta:
        model = MonthlyFinancialFormMetricValue
        fields = ['monthly_financial_form_metric_value_id', 'plant_technology_id', 'plant_technology_path',
                  'value_timestamp_utc', 'value_timestamp_local', 'metric_value', 'metric_value_string', 'monthly_financial_form_metric_id',
                  'datetime_created', 'datetime_updated', 'user_created', 'user_updated',
                  'monthly_financial_form_metric', 'plant_technology']

    def to_representation(self, instance):
        response = super().to_representation(instance)
        response['monthly_financial_form_metric_id'] = instance.monthly_financial_form_metric.monthly_financial_form_metric_id
        response['is_numeric'] = instance.monthly_financial_form_metric.is_numeric

        metric_value = instance.metric_value

        # If the monthly financial form is the caller, convert the resulting value to a string, but not for when GI does the call to the API
        if self.context['view'].__class__.__name__ == 'MonthlyFinancialFormMetricValueViewSet':
            metric_value = str(metric_value)
        
        response['metric_value'] = metric_value if instance.monthly_financial_form_metric.is_numeric else instance.metric_value_string
        response['plant_technology_id'] = instance.plant_technology.plant_technology_id
        response.pop("metric_value_string")
        response.pop("plant_technology")
        response.pop("monthly_financial_form_metric")

        return response


class ConfigurationFormMTPMSerializer(ModelSerializer):
    class Meta:
        model = ConfigurationFormMTPM
        fields = ['configuration_form_mtpm_id', 'mtpm_name', 'uom_metric', 'uom_imperial',
                  'element_path','plant_technology_id']


class ConfigurationFormMTPMValueSerializer(ModelSerializer):
    configuration_form_mtpm = ConfigurationFormMTPMSerializer(read_only=True)
    configuration_form_mtpm_id = serializers.PrimaryKeyRelatedField(
        queryset=ConfigurationFormMTPM.objects.all(),
        source='configuration_form_mtpm',
        write_only=True
    )

    plant_technology = PlantTechnologySerializer(read_only=True)
    plant_technology_id = serializers.PrimaryKeyRelatedField(
        queryset=PlantTechnology.objects.all(),
        source='plant_technology',
        write_only=True
    )

    datetime_created = DateTimeField(format="%Y-%m-%d %H:%M:%S")
    datetime_updated = DateTimeField(format="%Y-%m-%d %H:%M:%S")

    class Meta:
        model = ConfigurationFormMTPMValue
        fields = ['configuration_form_mtpm_value_id', 'display_name', 'min', 'max',
                  'display_low', 'display_high', 'configuration_form_mtpm_id', 'configuration_form_mtpm',
                  'plant_technology_id', 'plant_technology',
                  'datetime_created', 'datetime_updated', 'user_created', 'user_updated']

    def to_representation(self, instance):
        response = super().to_representation(instance);
        response['configuration_form_mtpm_id'] = instance.configuration_form_mtpm.configuration_form_mtpm_id
        response['plant_technology_id'] = instance.plant_technology.plant_technology_id
        response.pop("plant_technology")
        response.pop("configuration_form_mtpm")
        return response


class ConfigurationFormLeadingIndicatorSerializer(ModelSerializer):
    class Meta:
        model = ConfigurationFormLeadingIndicator
        fields = ['configuration_form_leading_indicator_id', 'leading_indicator_name', 'uom_metric', 'uom_imperial',
                  'element_path','configuration_form_mtpm_id']


class ConfigurationFormLeadingIndicatorValueSerializer(ModelSerializer):
    configuration_form_leading_indicator = ConfigurationFormLeadingIndicatorSerializer(read_only=True)
    configuration_form_leading_indicator_id = serializers.PrimaryKeyRelatedField(
        queryset=ConfigurationFormLeadingIndicator.objects.all(),
        source='configuration_form_leading_indicator',
        write_only=True
    )

    plant_technology = PlantTechnologySerializer(read_only=True)
    plant_technology_id = serializers.PrimaryKeyRelatedField(
        queryset=PlantTechnology.objects.all(),
        source='plant_technology',
        write_only=True
    )

    datetime_created = DateTimeField(format="%Y-%m-%d %H:%M:%S")
    datetime_updated = DateTimeField(format="%Y-%m-%d %H:%M:%S")

    class Meta:
        model = ConfigurationFormLeadingIndicatorValue
        fields = ['configuration_form_leading_indicator_value_id', 'display_name', 'min', 'max',
                  'display_low', 'display_high', 'corrective_action', 'cmo_link',
                  'configuration_form_leading_indicator_id', 'configuration_form_leading_indicator',
                  'plant_technology_id', 'plant_technology', 'datetime_created', 'datetime_updated', 'user_created',
                  'user_updated']

    def to_representation(self, instance):
        response = super().to_representation(instance)
        response['configuration_form_leading_indicator_id'] = instance.configuration_form_leading_indicator.configuration_form_leading_indicator_id
        response['plant_technology_id'] = instance.plant_technology.plant_technology_id
        response.pop("plant_technology")
        response.pop("configuration_form_leading_indicator")
        return response


class AnnualTargetFormMetricValueSerializer(ModelSerializer):
    configuration_form_mtpm = ConfigurationFormMTPMSerializer(read_only=True)
    configuration_form_mtpm_id = serializers.PrimaryKeyRelatedField(
        queryset=ConfigurationFormMTPM.objects.all(),
        source='configuration_form_mtpm',
        write_only=True
    )

    plant_technology = PlantTechnologySerializer(read_only=True)
    plant_technology_id = serializers.PrimaryKeyRelatedField(
        queryset=PlantTechnology.objects.all(),
        source='plant_technology',
        write_only=True
    )
    value_timestamp_utc = DateTimeField(format="%Y-%m-%d %H:%M:%S.%f")
    value_timestamp_local = DateTimeField(format="%Y-%m-%d %H:%M:%S.%f")
    datetime_created = DateTimeField(format="%Y-%m-%d %H:%M:%S.%f")
    datetime_updated = DateTimeField(format="%Y-%m-%d %H:%M:%S.%f")

    class Meta:
        model = AnnualTargetFormMetricValue
        fields = ['annual_target_form_metric_value_id', 'value_timestamp_utc', 'value_timestamp_local',
                  'last_fiscal_year', 'budget', 'personal_best', 'configuration_form_mtpm_id', 'configuration_form_mtpm', 'plant_technology_id',
                  'datetime_created', 'datetime_updated', 'user_created', 'user_updated','plant_technology']

    def to_representation(self, instance):
        response = super().to_representation(instance)
        response['configuration_form_mtpm_id'] = instance.configuration_form_mtpm.configuration_form_mtpm_id
        response['plant_technology_id'] = instance.plant_technology.plant_technology_id
        response.pop("configuration_form_mtpm")
        response.pop("plant_technology")
        return response
    

class AgentSerializer(ModelSerializer):
    class meta: 
        model = Agent
        fields = [
            'log_id',
            'status',
            'cpu_usage',
            'memory_usage',
            'disk_usage',
            'memory_usage',
            'log_date'
        ]


class DataTransferSerializer(ModelSerializer):
    class meta: 
        model = DataTransfer
        fields = [
            'log_id',
            'status',
            'transfer_rate',
            'total_points_in_transfer',
            'historical_start_date',
            'historical_end_date',
            'log_date'
        ]


class DataViewsSerializer(ModelSerializer):
    class meta: 
        model = DataViews
        fields = [
            'log_id',
            'name',
            'status',
            'log_date'
        ]
        

class SequentialDataConfigSerializer(ModelSerializer):
    class meta: 
        model = SequentialDataConfig
        fields = ['last_processed_date']


class SequentialDataSerializer(ModelSerializer):
    class meta: 
        model = SequentialData
        fields = [
            'log_id',
            'status',
            'object_path',
            'object_attribute_value_timestamp',
            'object_attribute_value_timeseries',
            'historical_end_date',
            'object_attribute_name',
            'log_date'
        ]