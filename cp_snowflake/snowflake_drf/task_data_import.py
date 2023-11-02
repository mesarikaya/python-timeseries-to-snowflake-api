import json
from io import StringIO
import logging
from typing import Optional

import pandas as pd
import requests

from django.conf import settings

log = logging.getLogger(__name__)


def process_element_data():
    process_steps = (
        (settings.ELEMENT_REGION_PIPELINE, settings.ELEMENT_REGION_FLOW, process_region),
        (settings.ELEMENT_TECHNOLOGY_PIPELINE, settings.ELEMENT_TECHNOLOGY_FLOW, process_technology),
        (settings.ELEMENT_PLANT_PIPELINE, settings.ELEMENT_PLANT_FLOW, process_plant),
        (settings.ELEMENT_PLANTTECHNOLOGY_PIPELINE, settings.ELEMENT_PLANTTECHNOLOGY_FLOW, process_plant_technology),
        (settings.ELEMENT_MTPM_PIPELINE, settings.ELEMENT_MTPM_FLOW, process_mtpm),
        (settings.ELEMENT_LEADING_INDICATOR_PIPELINE, settings.ELEMENT_LEADING_INDICATOR_FLOW, process_lead_indicators),
    )

    org_hostname = settings.ELEMENT_URL
    org_id = settings.ELEMENT_ORG_ID
    org_protocol = settings.ELEMENT_PROTOCOL
    token = login(org_protocol, org_hostname)
    if token is None:
        return "Login unavailable."
    for pipeline_id, flow_id, fn in process_steps:
        df = download_flow(org_protocol, org_hostname, token, org_id, pipeline_id, flow_id)
        fn(df)
    return "Element data processed."


def login(org_protocol: str, org_hostname: str) -> Optional[str]:
    email = settings.ELEMENT_USERNAME
    password = settings.ELEMENT_PASSWORD
    if password is None:
        log.error('Element password is empty. Please set environment variable. Returning.')
        return None
    headers = {'Content-Type': 'application/json'}
    url = '{}://{}/api/sessions'.format(org_protocol, org_hostname)
    payload = json.dumps({"email": email, "password": password})
    response = requests.post(url, data=payload, headers=headers)
    if "authToken" in response.json():
        return response.json()["authToken"]


def download_flow(
        org_protocol: str,
        org_hostname: str,
        token: str,
        org_id: str,
        pipeline_id: str,
        flow_id: str
) -> pd.DataFrame:
    org_url = '{}://{}/tags/org/{}'.format(org_protocol, org_hostname, org_id)
    headers = {'x-auth-token': token, 'Content-Type': 'application/json'}
    pipeline_url = '{}/pipelines/{}/flows/{}/download'.format(org_url, pipeline_id, flow_id)
    response = requests.get(pipeline_url, headers=headers)
    log.debug(f'Element API call status code is: {str(response.status_code)}')
    log.debug("Fetching Element data from org {}, pipeline {}, flow {}".format(org_id, pipeline_id, flow_id))

    # Pandas will interpret the string 'NA' as 'NaN'. We are here just erasing the default as we assume
    # that the data from Element is valid. This assumption may need to be updated.
    # ref: https://stackoverflow.com/questions/33952142/prevent-pandas-from-interpreting-na-as-nan-in-a-string
    text = StringIO(response.text)
    text.seek(0)
    df = pd.read_csv(text, keep_default_na=False)
    return df


def process_region(region_list: pd.DataFrame):
    from .models import Region
    for index, row in region_list.iterrows():
        if row.region_name is None:
            log.info("Region from Element has no name")
            continue
        if row.region_name.startswith('Enter region'):
            # TODO: bad region?
            continue
        existing_region = Region.objects.filter(region_name=row.region_name).first()
        if existing_region is not None:
            log.info("Region %r from Element already exists", existing_region.region_name)
            continue
        else:
            region, created = Region.objects.update_or_create(region_name=row.region_name)
            if created:
                log.info("Region %r from Element created successfully", region.region_name)
            else:
                log.warn("Region %r did not exist but was not successfully created.", region.region_name)
            


def process_technology(technology_list: pd.DataFrame):
    from .models import Technology
    for index, row in technology_list.iterrows():
        if row.technology_id is None:
            log.info("Technology from Element has no id")
            continue
        existing_technology = Technology.objects.filter(technology_external_id=row.technology_id).first()
        if existing_technology is not None:
            log.info("Technology %r from Element already exists", existing_technology.technology_external_id)
            continue
        else:
            technology, created = Technology.objects.update_or_create(
                technology_external_id=row.technology_id,
                defaults={'technology_name': row.technology_name}
            )
            if created:
                log.info("Technology %r from Element created successfully", technology.technology_external_id)
            else:
                log.warn("Technology %r did not exist but was not successfully created.", technology.technology_external_id)
            


def process_plant(plant_list: pd.DataFrame):
    from .models import Plant, Region
    for index, row in plant_list.iterrows():
        if row.plant_name is None:
            log.info("Plant from Element has no name")
            continue
        region = None
        existing_region = Region.objects.filter(region_name=row.region).first()
        if existing_region is not None:
            region = existing_region
        else:
            region, created = Region.objects.update_or_create(region_name=row.region)
            if created:
                log.info("Region %r from Element created successfully", region.region_name)
            else:
                log.warn("Region %r did not exist but was not successfully created.", region.region_name)
        existing_plant = Plant.objects.filter(plant_id=row.plant_id).first()
        if existing_plant is not None:
            existing_plant.plant_name = row.plant_name
            existing_plant.plant_path = row.plant_path
            existing_plant.reporting_day_start = row.reporting_day_start
            existing_plant.timezone = row.timezone
            existing_plant.utc_offset = row.utc_offset
            existing_plant.region_id - region.region_id
        else:
            plant, created = Plant.objects.update_or_create(
                plant_id=row.plant_id,
                defaults={
                    'plant_name': row.plant_name,
                    'plant_path': row.plant_path,
                    'reporting_day_start': row.reporting_day_start,
                    'timezone': row.timezone,
                    'utc_offset': row.utc_offset,
                    'region_id': region.region_id
                }
            )
            if created:
                log.info("Plant having Element id %r created.", plant.plant_id)
            else:
                log.info("Plant having Element id %r already exists", plant.plant_id)


def process_plant_technology(plant_technology_list: pd.DataFrame):
    from .models import Plant, PlantTechnology, Technology
    for index, row in plant_technology_list.iterrows():
        if row.plant_technology_id is None:
            log.info("Plant Technology from Element has no path")
            continue
        plant = Plant.objects.get(pk=row.plant_id)
        if plant is None:
            log.warning("Plant with id %r does not exist", row.plant_id)
            continue
        technology = Technology.objects.filter(technology_external_id=row.technology_id).first()
        if technology is None:
            log.warning("Technology with id %r does not exist", row.technology_id)
            continue
        plant_technology, created = PlantTechnology.objects.update_or_create(
            plant_technology_id=row.plant_technology_id,
            defaults={
                'plant_technology_path': row.plant_technology_path,
                'plant': plant,
                'technology': technology,
                'support_daily_financial_entries': row.support_daily_financial_entries
            }
        )
        if created:
            log.info("Plant Technology %r created successfully", plant_technology.plant_technology_path)
        else:
            log.info("Plant technology having path %r already exists", row.plant_technology_path)


def process_lead_indicators(li_list: pd.DataFrame):
    from .models import ConfigurationFormLeadingIndicator, ConfigurationFormMTPM
    for index, row in li_list.iterrows():
        mtpm = ConfigurationFormMTPM.objects.filter(
            configuration_form_mtpm_external_id=row.configuration_form_mtpm_id
        ).first()
        if mtpm is None:
            log.warning("MTPM with id %r does not exist", row.configuration_form_mtpm_id)
            continue
        li, created = ConfigurationFormLeadingIndicator.objects.update_or_create(
            configuration_form_mtpm=mtpm,
            configuration_form_leading_indicator_external_id=row.configuration_form_leading_indicator_Id,
            defaults={
                'leading_indicator_name':  row.leading_indicator_name,
                'uom_metric': row.uom_metric,
                'uom_imperial': row.uom_imperial,
                'element_path': row.leading_indicator_path,
                'configuration_form_mtpm': mtpm,
                'configuration_form_leading_indicator_external_id': row.configuration_form_leading_indicator_Id,
            }
        )
        if created:
            log.info(
                "Lead Indicator %r/%r created successfully",
                li.configuration_form_mtpm.mtpm_name,
                li.leading_indicator_name
            )
        else:
            log.info("Leading Indicator %r already exists", row.leading_indicator_name)


def process_mtpm(mtpm_list: pd.DataFrame):
    from .models import ConfigurationFormMTPM, Technology, PlantTechnology
    for index, row in mtpm_list.iterrows():

        # Extract parts of the MTPM id (plant name, technology name and mtpm name) to leave only the plant identifier
        technology_mtpm_part = (row.technology_id + row.MTPM).replace(" ", "").upper()
        external_plant_id = row.configuration_form_mtpm_id.replace(technology_mtpm_part, "")

        # Filter by technology id to get all plant and plant technology records 
        plant_technologies = PlantTechnology.objects.select_related('plant').filter(technology__technology_external_id=row.technology_id).all()

        # Filter further on the plant identifier coming from Element (Full plant name, no spaces) vs value on the DB by doing a replace and upper on it
        plant_object = list(filter(lambda plant_tech: plant_tech.plant.plant_name.replace(" ", "").upper() == external_plant_id, plant_technologies))

        if plant_object is None or len(plant_object) == 0:
            log.warning(
                "technology having id %r does not relate to any plant; mtpm having name %r will not be saved.",
                row.technology_id,
                row.MTPM
            )
            continue

        mtpm, created = ConfigurationFormMTPM.objects.update_or_create(
            configuration_form_mtpm_external_id=row.configuration_form_mtpm_id,
            defaults={
                'mtpm_name': row.MTPM,
                'uom_metric': row.uom_metric,
                'uom_imperial': row.uom_imperial,
                'element_path': row.MTPM_path,
                # Create/update plant_technology reference
                'plant_technology': plant_object[0],
            }
        )
        if created:
            log.info(
                "MTPM %r/%r created successfully",
                mtpm.plant_technology.plant_technology_id,
                mtpm.mtpm_name
            )
        else:
            log.info("MTPM having name %r already exists", mtpm.mtpm_name)