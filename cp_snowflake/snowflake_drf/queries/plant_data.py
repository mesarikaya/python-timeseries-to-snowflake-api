import os
from snowflake_wrapper.base import SnowflakeWrapper
from datetime import timedelta, datetime
import concurrent.futures
from .. import helpers
import logging
from decimal import Decimal, DecimalException
from .methods import SnowflakeMethods

log = logging.getLogger(__name__)


class PlantData(SnowflakeMethods):

    def get_snowflake_region_dim_data(self, region_id=''):

        records = []

        if region_id:
            region_id = f"WHERE region_id = '{region_id}'"

        results = self.wrapper.validate_and_execute(f"""
                SELECT 
                    region_id, 
                    region_name 
                FROM 
                    {os.getenv("SNOWFLAKE_DATABASE")}
                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                    .DIM_REGION
                {region_id}
                    """)

        for res in results:
            records.append({
                'region_id': res[0],
                'region_name': res[1]
            })

        return records

    def get_snowflake_plant_dim_data(self, region_id, plant_id=''):

        records = []

        if region_id and plant_id:
            plant_id = f"AND p.plant_id='{plant_id}'"
        elif plant_id:
            plant_id = f"WHERE p.plant_id='{plant_id}'"

        results = self.wrapper.validate_and_execute(f"""
                SELECT 
                    p.plant_id, 
                    r.region_id, 
                    p.plant_name, 
                    COALESCE(r.region_name, p.region_name) region_name 
                FROM 
                    {os.getenv("SNOWFLAKE_DATABASE")}
                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                    .DIM_PLANT p 
                    LEFT JOIN 
                        {os.getenv("SNOWFLAKE_DATABASE")}
                        .{os.getenv("SNOWFLAKE_SCHEMA")}
                        .DIM_REGION r 
                    ON p.region_name = r.region_name
                    {f"WHERE r.region_id='{region_id}'" if region_id else ''}
                    {plant_id if plant_id else ''}
                    """)

        for res in results:
            records.append({
                'plant_id': res[0],
                'region_id': res[1],
                'plant_name': res[2],
                'region_name': res[3]
            })

        return records

    def get_snowflake_plant_technology_dim_data(self, plant_id, plant_technology_id=''):

        records = []

        if plant_id and plant_technology_id:
            plant_technology_id = f"AND plant_technology_id='{plant_technology_id}'"
        elif plant_technology_id:
            plant_technology_id = f"WHERE plant_technology_id='{plant_technology_id}'"

        results = self.wrapper.validate_and_execute(f"""
                SELECT 
                    plant_technology_id, 
                    plant_id, 
                    technology_name, 
                    plant_name, 
                    plant_technology_path
                FROM 
                    {os.getenv("SNOWFLAKE_DATABASE")}
                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                    .DIM_PLANT_TECHNOLOGY
                    {f"WHERE plant_id='{plant_id}'" if plant_id else ''}
                    {plant_technology_id if plant_technology_id else ''}
                    """)

        for res in results:
            records.append({
                'plant_technology_id': res[0],
                'plant_id': res[1],
                'technology_name': res[2],
                'plant_name': res[3],
                'plant_technology_path': res[4]
            })

        return records