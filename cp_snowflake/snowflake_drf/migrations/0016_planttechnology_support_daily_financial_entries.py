# Generated by Django 4.2.1 on 2023-09-04 18:14

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("snowflake_drf", "0015_monthlyfinancialformmetric_is_numeric"),
    ]

    operations = [
        migrations.AddField(
            model_name="planttechnology",
            name="support_daily_financial_entries",
            field=models.BooleanField(default=False),
        ),
    ]
