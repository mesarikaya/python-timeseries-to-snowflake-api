# Generated by Django 4.2.6 on 2023-10-24 21:21

import datetime
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('snowflake_drf', '0016_planttechnology_support_daily_financial_entries'),
    ]

    operations = [
        migrations.CreateModel(
            name='Agent',
            fields=[
                ('log_id', models.AutoField(primary_key=True, serialize=False)),
                ('status', models.CharField(max_length=50)),
                ('cpu_usage', models.CharField(max_length=50)),
                ('disk_usage', models.CharField(max_length=50)),
                ('memory_usage', models.CharField(max_length=50)),
                ('log_date', models.DateTimeField(blank=True, default=datetime.datetime.now)),
            ],
            options={
                'db_table': 'agent',
            },
        ),
        migrations.CreateModel(
            name='DataTransfer',
            fields=[
                ('log_id', models.AutoField(primary_key=True, serialize=False)),
                ('status', models.CharField(max_length=50)),
                ('transfer_rate', models.CharField(max_length=50)),
                ('total_points_in_transfer', models.CharField(max_length=50)),
                ('historical_start_date', models.CharField(max_length=50)),
                ('historical_end_date', models.CharField(max_length=50)),
                ('log_date', models.DateTimeField(blank=True, default=datetime.datetime.now)),
            ],
            options={
                'db_table': 'data_transfer',
            },
        ),
        migrations.CreateModel(
            name='DataViews',
            fields=[
                ('log_id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=50)),
                ('status', models.CharField(max_length=50)),
                ('log_date', models.DateTimeField(blank=True, default=datetime.datetime.now)),
            ],
            options={
                'db_table': 'data_views',
            },
        ),
        migrations.CreateModel(
            name='SequentialData',
            fields=[
                ('log_id', models.AutoField(primary_key=True, serialize=False)),
                ('status', models.CharField(max_length=50)),
                ('object_path', models.CharField(max_length=50)),
                ('object_attribute_value_timestamp', models.DateTimeField(blank=True)),
                ('historical_end_date', models.CharField(max_length=50)),
                ('object_attribute_name', models.CharField(max_length=50)),
                ('log_date', models.DateTimeField(blank=True, default=datetime.datetime.now)),
            ],
            options={
                'db_table': 'sequential_data',
            },
        ),
        migrations.CreateModel(
            name='SequentialDataConfig',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('last_processed_date', models.DateTimeField(blank=True, default=datetime.datetime.now)),
            ],
            options={
                'db_table': 'sequential_data_config',
            },
        ),
    ]
