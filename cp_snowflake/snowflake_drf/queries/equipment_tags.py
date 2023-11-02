import os
from snowflake_wrapper.base import SnowflakeWrapper
from datetime import timedelta, datetime
import concurrent.futures
from .. import helpers
import logging
from decimal import Decimal, DecimalException
from .methods import SnowflakeMethods

log = logging.getLogger(__name__)


class EquipmentTags(SnowflakeMethods):
    
    def get_snowflake_equipment_tag_dim_data(self, leading_indicator_id, equipment_tag_id=''):

        records = []

        if equipment_tag_id:
            equipment_tag_id = f"AND equipment_tag_id = '{equipment_tag_id}'"

        results = self.wrapper.validate_and_execute(f"""
                SELECT 
                    equipment_tag_id,
                    leading_indicator_id,
                    equipment_tag_name,
                    equipment_tag_display_name,
                    display_high_metric,
                    display_low_metric,
                    display_high_imperial,
                    display_low_imperial,
                    max_metric,
                    min_metric,
                    max_imperial,
                    min_imperial,
                    uom_metric,
                    uom_imperial
                FROM 
                    {os.getenv("SNOWFLAKE_DATABASE")}
                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                    .DIM_EQUIPMENT_TAG
                WHERE leading_indicator_id = '{leading_indicator_id}'
                {equipment_tag_id if equipment_tag_id else ''}
                    """
                                                    )

        for equipment_tag in results:
            records.append({
                'equipment_tag_id': equipment_tag[0],
                'leading_indicator_id': equipment_tag[1],
                'equipment_tag_name': equipment_tag[2],
                'equipment_tag_display_name': equipment_tag[3],
                'display_high_metric': equipment_tag[4],
                'display_low_metric': equipment_tag[5],
                'display_high_imperial': equipment_tag[6],
                'display_low_imperial': equipment_tag[7],
                'max_metric': equipment_tag[8],
                'min_metric': equipment_tag[9],
                'max_imperial': equipment_tag[10],
                'min_imperial': equipment_tag[11],
                'uom_metric': equipment_tag[12],
                'uom_imperial': equipment_tag[13],
            })

        return records

    def get_snowflake_equipment_tag_metric_data(self, equipment_tag_id, leading_indicator_id, date_time_start,
                                                date_time_end):

        records = []

        log.info(f"Fetching EQ metric data for eq tag {equipment_tag_id} and leading indicator {leading_indicator_id}")

        equipment_tag_id_clause = ''

        if equipment_tag_id:
            equipment_tag_id_clause = f" AND dt.equipment_tag_id = '{equipment_tag_id}'"
            time_series_select = f"""
                SELECT 
                    dt.equipment_tag_value_metric,
                    dt.equipment_tag_value_imperial,
                    dt.equipment_tag_value_timestamp_utc,
                    dt.equipment_tag_value_timestamp_local,
                    dt.is_equipment_tag_substituted_flag
                FROM 
                    {os.getenv("SNOWFLAKE_DATABASE")}
                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                    ."FACT_EQUIPMENT_TAG" dt                  
                WHERE dt.equipment_tag_id = '{equipment_tag_id}'
                AND (dt.equipment_tag_value_timestamp_utc BETWEEN '{date_time_start}' AND '{date_time_end}')
                ORDER BY dt.equipment_tag_value_timestamp_utc
                    """
        else:
            time_series_select = f"""
                SELECT 
                    ft.equipment_tag_value_metric,
                    ft.equipment_tag_value_imperial,
                    ft.equipment_tag_value_timestamp_utc,
                    ft.equipment_tag_value_timestamp_local,
                    ft.is_equipment_tag_substituted_flag
                FROM 
                    {os.getenv("SNOWFLAKE_DATABASE")}
                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                    ."FACT_EQUIPMENT_TAG" ft    
                LEFT JOIN 
                    {os.getenv("SNOWFLAKE_DATABASE")}
                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                    ."DIM_EQUIPMENT_TAG" dt 
                ON dt.equipment_tag_id = ft.equipment_tag_id                    
                WHERE dt.leading_indicator_id = '{leading_indicator_id}'              
                AND (ft.equipment_tag_value_timestamp_utc BETWEEN '{date_time_start}' AND '{date_time_end}')
                ORDER BY ft.equipment_tag_value_timestamp_utc
                    """

        dim_results = self.get_snowflake_equipment_tag_metric_dim_data(leading_indicator_id,
                                                                       equipment_tag_id_clause)
        time_series_results = self.get_snowflake_equipment_tag_metric_time_series_data(time_series_select,
                                                                                       date_time_start,
                                                                                       date_time_end)

        for equipment_tag in dim_results:
            records.append({
                'equipment_tag_id': equipment_tag[0],
                'leading_indicator_id': equipment_tag[1],
                'equipment_tag_name': equipment_tag[2],
                'equipment_tag_display_name': equipment_tag[3],
                'display_high_metric': equipment_tag[4],
                'display_low_metric': equipment_tag[5],
                'display_high_imperial': equipment_tag[6],
                'display_low_imperial': equipment_tag[7],
                'max_metric': equipment_tag[8],
                'min_metric': equipment_tag[9],
                'max_imperial': equipment_tag[10],
                'min_imperial': equipment_tag[11],
                'uom_metric': equipment_tag[12],
                'uom_imperial': equipment_tag[13],
                # Pass the entire filtered datase to build the time series array inside the main object
                'time_series': self.get_snowflake_equipment_tag_time_series(time_series_results)
            })

        return records

    def get_snowflake_equipment_tag_metric_dim_data(self, leading_indicator_id, equipment_tag_id_clause):

        result = self.wrapper.validate_and_execute(f"""
                    SELECT 
                        dt.equipment_tag_id,
                        dt.leading_indicator_id,
                        dt.equipment_tag_name,
                        dt.equipment_tag_display_name,
                        dt.display_high_metric,
                        dt.display_low_metric,
                        dt.display_high_imperial,
                        dt.display_low_imperial,
                        dt.max_metric,
                        dt.min_metric,
                        dt.max_imperial,
                        dt.min_imperial,
                        dt.uom_metric,
                        dt.uom_imperial
                    FROM
                        {os.getenv("SNOWFLAKE_DATABASE")}
                        .{os.getenv("SNOWFLAKE_SCHEMA")}
                        .DIM_EQUIPMENT_TAG dt
                    WHERE dt.leading_indicator_id = '{leading_indicator_id}'
                    {equipment_tag_id_clause if equipment_tag_id_clause else ''}
                        """
                                                   )

        return result

    def get_snowflake_equipment_tag_metric_time_series_data(self, time_series_select, date_time_start,
                                                            date_time_end):

        time_series_results = self.wrapper.validate_and_execute(time_series_select)
        time_series_results = self.normalize_time_series(time_series_results, date_time_start, date_time_end, 2)

        return time_series_results

    def get_snowflake_equipment_tag_time_series(self, time_series_results):

        records = []

        for res in time_series_results:
            records.append({
                'equipment_tag_value_timestamp_utc': res[2],
                'equipment_tag_value_timestamp_local': res[3],
                'equipment_tag_value_metric': res[0],
                'equipment_tag_value_imperial': res[1],
                'is_equipment_tag_substituted_flag': self.convert_boolean_value(res[4])
            })

        return records

    def get_snowflake_equipment_tag_metric_data_beta(self, equipment_tag_id, leading_indicator_id, date_time_start,
                                                     date_time_end, preferred_uom, timezone):

        records = []

        log.info(f"Fetching EQ metric data for eq tag {equipment_tag_id} and leading indicator {leading_indicator_id}")

        equipment_tag_id_clause = ''

        if equipment_tag_id:
            equipment_tag_id_clause = f" AND dt.equipment_tag_id = '{equipment_tag_id}'"
            time_series_select = f"""
                SELECT 
                    dt.equipment_tag_value_timestamp_utc as time,
                    dt.equipment_tag_value_{preferred_uom} as value
                FROM 
                    {os.getenv("SNOWFLAKE_DATABASE")}
                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                    ."FACT_EQUIPMENT_TAG" dt                  
                WHERE dt.equipment_tag_id = '{equipment_tag_id}'
                AND (dt.equipment_tag_value_timestamp_utc BETWEEN '{date_time_start}' AND '{date_time_end}')
                ORDER BY dt.equipment_tag_value_timestamp_utc
                    """
        else:
            time_series_select = f"""
                SELECT 
                    ft.equipment_tag_value_timestamp_utc as time,
                    ft.equipment_tag_value_{preferred_uom} as value
                FROM 
                    {os.getenv("SNOWFLAKE_DATABASE")}
                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                    ."FACT_EQUIPMENT_TAG" ft    
                LEFT JOIN 
                    {os.getenv("SNOWFLAKE_DATABASE")}
                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                    ."DIM_EQUIPMENT_TAG" dt 
                ON dt.equipment_tag_id = ft.equipment_tag_id                    
                WHERE dt.leading_indicator_id = '{leading_indicator_id}'              
                AND (ft.equipment_tag_value_timestamp_utc BETWEEN '{date_time_start}' AND '{date_time_end}')
                ORDER BY ft.equipment_tag_value_timestamp_utc
                    """

        dim_results = self.wrapper.validate_and_execute(f"""
                SELECT 
                    dt.equipment_tag_id,
                    dt.leading_indicator_id,
                    dt.equipment_tag_name,
                    dt.equipment_tag_display_name,
                    dt.display_high_{preferred_uom} as display_high,
                    dt.display_low_{preferred_uom} as display_low,
                    dt.uom_{preferred_uom} as uom,
                    dt.max_{preferred_uom},
                    dt.min_{preferred_uom}
                FROM 
                    {os.getenv("SNOWFLAKE_DATABASE")}
                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                    .DIM_EQUIPMENT_TAG dt                  
                WHERE dt.leading_indicator_id = '{leading_indicator_id}'
                {equipment_tag_id_clause if equipment_tag_id_clause else ''}
                    """
                                                        )

        time_series_results = self.wrapper.validate_and_execute(time_series_select)
        time_series_results = self.normalize_time_series(time_series_results, date_time_start, date_time_end)

        for equipment_tag in dim_results:
            time_series = self.get_snowflake_equipment_tag_time_series_beta(time_series_results, timezone)

            records.append({
                'equipment_tag_id': equipment_tag[0],
                'leading_indicator_id': equipment_tag[1],
                'equipment_tag_name': equipment_tag[2],
                'equipment_tag_display_name': equipment_tag[3],
                'display_high': self.convert_to_decimal(equipment_tag[4]),
                'display_low': self.convert_to_decimal(equipment_tag[5]),
                'ts_max': time_series[2],
                'ts_min': time_series[1],
                'uom': equipment_tag[6],
                'max_target_value': equipment_tag[7],
                'min_target_value': equipment_tag[8],
                # Pass the entire filtered datase to build the time series array inside the main object
                'time_series': time_series[0]
            })

        return records

    def get_snowflake_equipment_tag_time_series_beta(self, time_series_results, timezone=None):

        records = []

        max = None
        min = None

        if timezone is None:
            timezone = 'UTC'

        last_valid_value = None

        for res in time_series_results:
            if res[1] is None:
                res_value = None
            else:
                res_value = Decimal(round(Decimal(res[1]), 4))
                last_valid_value = res_value
            try:
                if max is None or (res_value is not None and res_value > max):
                    max = res_value
            except DecimalException:
                    log.error(f'Error occurred processing decimal values: max: {max} res_value: {res_value}')
            try:
                if min is None or (res_value is not None and res_value < min):
                    min = res_value
            except DecimalException:
                    log.error(f'Error occurred processing decimal values: min: {min} res_value: {res_value}')
            if res[0] is not None:
                time = helpers.convert_utc_to_local(helpers.get_datetime_obj(res[0]), timezone)
                formatted_date = helpers.format_datetime(time)
                time_epoch = helpers.get_epoch(time, timezone)
            else:
                res_value = last_valid_value
                time = helpers.convert_utc_to_local(helpers.get_datetime_obj(res[2]), timezone)
                formatted_date = helpers.format_datetime(time)
                time_epoch = helpers.get_epoch(time, timezone)
            records.append({
                'time': time,
                'value': res_value,
                'formatted_date': formatted_date,
                'time_epoch': time_epoch,
            })
        
        if max is not None:
            max = Decimal(max)
        if min is not None:
            min = Decimal(min)

        return [records, min, max]