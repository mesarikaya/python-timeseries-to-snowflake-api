# Generated by Django 4.1.2 on 2023-01-17 19:50

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        (
            "snowflake_drf",
            "0005_rename_annual_target_form_metric_values_id_annualtargetformmetricvalue_annual_target_form_metric_val",
        ),
    ]

    operations = [
        migrations.AddField(
            model_name="technology",
            name="technology_external_id",
            field=models.CharField(default="", max_length=50),
        ),
    ]