# Generated by Django 4.1.2 on 2023-03-28 13:15

from django.db import migrations, models, transaction

model_name = 'monthlyfinancialformmetric'


def populate(apps, schema_editor):
    Model = apps.get_model('snowflake_drf', model_name)
    while Model.objects.filter(category__isnull=True).exists():
        with transaction.atomic():
            for row in Model.objects.all():
                row.category = {
                    'Electricity Price': 'NR Financial',
                    'Empyreal Value': 'Financial',
                    'Feed Value': 'Financial',
                    'Fiber Value': 'Financial',
                    'Fiber Yield': 'Yield',
                    'Fresh Water Price': 'NR Financial',
                    'Gas Price': 'NR Financial',
                    'Germ Cake Yield': 'Yield',
                    'Germ Meal': 'Financial',
                    'Germ Yield': 'Yield',
                    'Gluten Protein Value': 'Financial',
                    'Gluten Value': 'Financial',
                    'Gluten Yield': 'Yield',
                    'LSW Yield': 'Yield',
                    'Monthly Grind': 'Grind',
                    'Oil Value': 'Financial',
                    'Slurry Yield': 'Yield',
                    'Starch Value': 'Financial',
                    'Steam Price': 'NR Financial',
                    'Waste Water Price': 'NR Financial',
                }.get(row.metric_name)
                row.display_name = {
                    'Electricity Price': 'Electric Price',
                    'Fiber Value': 'Bran Value',
                    'Fiber Yield': 'Bran Yield',
                    'Fresh Water Price': 'Water Price',
                    'Germ Meal': 'Germ Meal Value',
                    'Gluten Yield': 'Protein Yield',
                    'LSW Yield': 'Steepwater Yield',
                    'Slurry Yield': 'Starch Yield'
                }.get(row.metric_name, row.metric_name)
                row.save()


def backwards(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ('snowflake_drf', '0009_configurationformleadingindicator_configuration_form_leading_indicator_external_id_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name=model_name,
            name='category',
            field=models.CharField(null=True, max_length=50),
        ),
        migrations.AddField(
            model_name=model_name,
            name='display_name',
            field=models.CharField(null=True, max_length=100),
        ),
        migrations.RunPython(populate, backwards),
    ]