from django.contrib.auth.models import AbstractUser, Group
from django.db import models
from datetime import datetime

class User(AbstractUser):
    groups = models.ManyToManyField(
        Group,
        verbose_name='groups',
        blank=True,
        help_text='The groups this user belongs to. A user will get all permissions granted to each of their groups.',
        related_name="user_set",
        related_query_name="user",
        through="UserGroups"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    REQUIRED_FIELDS = []

    class Meta:
        verbose_name = 'user'
        verbose_name_plural = 'users'

    def get_full_name(self):
        return '%s %s' % (self.first_name, self.last_name)

    def get_short_name(self):
        return self.first_name

    def __str__(self):
        return self.username


class UserGroups(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    group = models.ForeignKey(Group, on_delete=models.CASCADE)


class Region(models.Model):
    region_id = models.AutoField(primary_key=True)
    region_name = models.CharField(max_length=50)

    class Meta:
        db_table = 'region'


class Plant(models.Model):
    plant_id = models.CharField(primary_key=True, max_length=36)
    plant_name = models.CharField(max_length=100)
    plant_path = models.CharField(max_length=1024, default="")
    reporting_day_start = models.IntegerField(default=0)
    timezone = models.CharField(max_length=100, default="")
    utc_offset = models.FloatField(default=0.0)
    region = models.ForeignKey(Region, on_delete=models.CASCADE)

    class Meta:
        db_table = 'plant'


class Technology(models.Model):
    technology_id = models.AutoField(primary_key=True)
    technology_name = models.CharField(max_length=50)
    technology_external_id = models.CharField(max_length=100, default="")

    class Meta:
        db_table = 'technology'


class PlantTechnology(models.Model):
    plant_technology_id = models.CharField(primary_key=True,  max_length=36)
    plant_technology_path = models.CharField(max_length=1024, default="")
    plant = models.ForeignKey(Plant, on_delete=models.CASCADE)
    technology = models.ForeignKey(Technology, on_delete=models.CASCADE)
    support_daily_financial_entries = models.BooleanField(default=False)

    class Meta:
        db_table = 'plant_technology'
        
        
class PlantHistoricalData(models.Model):
    plant_historical_data_id = models.AutoField(primary_key=True)
    timestamp = models.DateTimeField()
    isDST = models.SmallIntegerField()
    plant = models.ForeignKey(Plant, on_delete=models.CASCADE)

    class Meta:
        db_table = 'plant_historical_data'


class MonthlyFinancialFormMetric(models.Model):
    monthly_financial_form_metric_id = models.AutoField(primary_key=True)
    metric_name = models.CharField(max_length=100)
    uom_metric = models.CharField(max_length=50, null=True)
    uom_imperial = models.CharField(max_length=50, null=True)
    # Feature 145099 - Postgres - Update posgres Data Model to identify values to be saved to PI
    save_to_pi = models.SmallIntegerField(default= 0)
    pi_attribute_name = models.CharField(max_length=1024, null=True, blank=True, default="")

    technology = models.ForeignKey(Technology, on_delete=models.CASCADE)
    display_name = models.CharField(max_length=100, null=True)
    category = models.CharField(max_length=50, null=True)
    subcategory = models.CharField(max_length=50, null=True)
    is_numeric = models.BooleanField(default=False)

    class Meta:
        db_table = 'monthly_financial_form_metric'


class MonthlyFinancialFormMetricValue(models.Model):
    monthly_financial_form_metric_value_id = models.AutoField(primary_key=True)
    plant_technology_path = models.CharField(max_length=1024, default="")
    value_timestamp_utc = models.DateTimeField()
    value_timestamp_local = models.DateTimeField()
    metric_value = models.FloatField(null=True)
    metric_value_string = models.CharField(max_length=100, null=True)
    monthly_financial_form_metric = models.ForeignKey(MonthlyFinancialFormMetric, on_delete=models.CASCADE)
    plant_technology = models.ForeignKey(PlantTechnology, on_delete=models.CASCADE)
    datetime_created = models.DateTimeField(auto_now_add=True)
    datetime_updated = models.DateTimeField(auto_now=True)
    user_created = models.CharField(max_length=100)
    user_updated = models.CharField(max_length=100)

    class Meta:
        db_table = 'monthly_financial_form_metric_value'


class ConfigurationFormMTPM(models.Model):
    configuration_form_mtpm_id = models.AutoField(primary_key=True)
    mtpm_name = models.CharField(max_length=100)
    uom_metric = models.CharField(max_length=50)
    uom_imperial = models.CharField(max_length=50)
    element_path = models.CharField(max_length=150, null=True, blank=True)
    plant_technology = models.ForeignKey(PlantTechnology, null=True, blank=True, on_delete=models.CASCADE)
    configuration_form_mtpm_external_id = models.CharField(max_length=100, default="")

    class Meta:
        db_table = 'configuration_form_mtpm'


class ConfigurationFormMTPMValue(models.Model):
    configuration_form_mtpm_value_id = models.AutoField(primary_key=True)
    display_name = models.CharField(max_length=100, null=True, blank=True)
    min = models.FloatField(null=True, blank=True)
    max = models.FloatField(null=True, blank=True)
    display_low = models.FloatField(null=True, blank=True)
    display_high = models.FloatField(null=True, blank=True)
    configuration_form_mtpm = models.ForeignKey(ConfigurationFormMTPM, on_delete=models.CASCADE)
    plant_technology = models.ForeignKey(PlantTechnology, on_delete=models.CASCADE)
    datetime_created = models.DateTimeField(auto_now_add=True)
    datetime_updated = models.DateTimeField(auto_now=True)
    user_created = models.CharField(max_length=100)
    user_updated = models.CharField(max_length=100)

    class Meta:
        db_table = 'configuration_form_mtpm_value'


class ConfigurationFormLeadingIndicator(models.Model):
    configuration_form_leading_indicator_id = models.AutoField(primary_key=True)
    leading_indicator_name = models.CharField(max_length=100)
    uom_metric = models.CharField(max_length=50)
    uom_imperial = models.CharField(max_length=50)
    element_path = models.CharField(max_length=150, null=True, blank=True)
    configuration_form_mtpm = models.ForeignKey(ConfigurationFormMTPM, on_delete=models.CASCADE)
    configuration_form_leading_indicator_external_id = models.CharField(max_length=100, default="")

    class Meta:
        db_table = 'configuration_form_leading_indicator'
        

class ConfigurationFormLeadingIndicatorValue(models.Model):
    configuration_form_leading_indicator_value_id = models.AutoField(primary_key=True)
    display_name = models.CharField(max_length=100,null=True, blank=True)
    min = models.FloatField(null=True,blank=True)
    max = models.FloatField(null=True,blank=True)
    display_low = models.FloatField(null=True,blank=True)
    display_high = models.FloatField(null=True,blank=True)
    corrective_action = models.TextField(null=True,blank=True)
    cmo_link = models.TextField(null=True,blank=True)
    configuration_form_leading_indicator = models.ForeignKey(ConfigurationFormLeadingIndicator, on_delete=models.CASCADE)
    plant_technology = models.ForeignKey(PlantTechnology, on_delete=models.CASCADE)
    datetime_created = models.DateTimeField(auto_now_add=True)
    datetime_updated = models.DateTimeField(auto_now=True)
    user_created = models.CharField(max_length=100)
    user_updated = models.CharField(max_length=100)

    class Meta:
        db_table = 'configuration_form_leading_indicator_value'


class AnnualTargetFormMetricValue(models.Model):
    annual_target_form_metric_value_id = models.AutoField(primary_key=True)
    value_timestamp_utc = models.DateTimeField()
    value_timestamp_local = models.DateTimeField()
    last_fiscal_year = models.FloatField()
    budget = models.FloatField()
    personal_best = models.FloatField()
    configuration_form_mtpm = models.ForeignKey(ConfigurationFormMTPM, on_delete=models.CASCADE)
    plant_technology = models.ForeignKey(PlantTechnology, on_delete=models.CASCADE)
    datetime_created = models.DateTimeField(auto_now_add=True)
    datetime_updated = models.DateTimeField(auto_now=True)
    user_created = models.CharField(max_length=100)
    user_updated = models.CharField(max_length=100)

    class Meta:
        db_table = 'annual_target_form_metric_value'


class SnowflakeLogging(models.Model):
    '''
    Cargill Snowflake/Minerva Platform availability
    '''
    log_id = models.AutoField(primary_key=True)
    status = models.CharField(max_length=50, null=True)
    message = models.CharField(max_length=100, null=True)
    event = models.CharField(max_length=50, null=True)
    log_date = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'snowflake_logging'

class Agent (models.Model):
    log_id = models.AutoField(primary_key=True)
    status = models.CharField(max_length=50)
    cpu_usage = models.CharField(max_length=50)
    memory_usage = models.CharField(max_length=50)
    disk_usage = models.CharField(max_length=50)
    memory_usage = models.CharField(max_length=50)
    log_date = models.DateTimeField(default=datetime.now, blank=True)
    
    class Meta:
        db_table = 'agent'


class DataTransfer (models.Model):
    log_id = models.AutoField(primary_key=True)
    status = models.CharField(max_length=50)
    transfer_rate = models.CharField(max_length=50)
    total_points_in_transfer = models.CharField(max_length=50)
    historical_start_date = models.CharField(max_length=50)
    historical_end_date = models.CharField(max_length=50)
    log_date = models.DateTimeField(default=datetime.now, blank=True)
    
    class Meta:
        db_table = 'data_transfer'


class DataViews (models.Model):
    log_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=50)
    status = models.CharField(max_length=50)
    log_date = models.DateTimeField(default=datetime.now, blank=True)

    class Meta:
        db_table = 'data_views'


class SequentialDataConfig(models.Model):
    last_processed_date = models.DateTimeField(default=datetime.now, blank=True)
    
    class Meta:
        db_table = 'sequential_data_config'


class SequentialData (models.Model):
    log_id = models.AutoField(primary_key=True)
    status = models.CharField(max_length=50)
    object_path = models.CharField(max_length=50)
    object_attribute_value_timestamp = models.DateTimeField(blank=True)
    object_attribute_value_timeseries = models.FloatField 
    historical_end_date = models.CharField(max_length=50)
    object_attribute_name = models.CharField(max_length=50)
    log_date = models.DateTimeField(default=datetime.now, blank=True)

    class Meta:
        db_table = 'sequential_data'
