from django.urls import include, path
from rest_framework import routers

from snowflake_drf import views

router = routers.DefaultRouter()

router.register(r'configuration-form-leading-indicator-value', views.ConfigurationFormLeadingIndicatorValueViewSet)
router.register(r'configuration-form-mtpm-value', views.ConfigurationFormMtpmValueViewSet)
router.register(r'annual-target-form-metric-value', views.AnnualTargetFormMetricValueViewSet)
router.register(r'monthly-financial-form-metric-value', views.MonthlyFinancialFormMetricValueViewSet),
router.register(r'ingestion/metric/values', views.IngestionMetricValueViewSet),
router.register(r'regions', views.RegionViewSet),
router.register(r'plants', views.PlantViewSet),
router.register(r'technologies', views.TechnologyViewSet),
router.register(r'plant-technologies', views.PlantTechnologyViewSet),
router.register(r'plant-historical-data', views.PlantHistoricalDataViewSet),
router.register(r'monthly-financial-form-metric', views.MonthlyFinancialFormMetricViewSet),
router.register(r'ingestion/metric', views.MonthlyFinancialFormMetricIngestionViewSet),
router.register(r'configuration-form-mtpm', views.ConfigurationFormMTPMViewSet),
router.register(r'configuration-form-leading-indicator', views.ConfigurationFormLeadingIndicatorViewSet),

urlpatterns = [
    path('', include(router.urls)),
]
