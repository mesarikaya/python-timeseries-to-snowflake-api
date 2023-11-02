from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('snowflake_drf', '0002_annualtargetformmetricvalue_configuration_form_mtpm'),
    ]

    operations = [
        migrations.RunSQL(
            """
            UPDATE annual_target_form_metric_value
            SET configuration_form_mtpm_id = annual_target_form_metrics_id;
            """
        ),
    ]