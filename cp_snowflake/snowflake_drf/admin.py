from django.contrib import admin

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
    ConfigurationFormLeadingIndicatorValue,
    AnnualTargetFormMetricValue
)

admin.site.register(Region)
admin.site.register(Plant)
admin.site.register(Technology)
admin.site.register(PlantTechnology)
admin.site.register(PlantHistoricalData)
admin.site.register(MonthlyFinancialFormMetric)
admin.site.register(MonthlyFinancialFormMetricValue)
admin.site.register(ConfigurationFormMTPM)
admin.site.register(ConfigurationFormMTPMValue)
admin.site.register(ConfigurationFormLeadingIndicator)
admin.site.register(ConfigurationFormLeadingIndicatorValue)
admin.site.register(AnnualTargetFormMetricValue)
