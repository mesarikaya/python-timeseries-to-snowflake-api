import os
from snowflake_wrapper.base import SnowflakeWrapper
from datetime import timedelta, datetime
import concurrent.futures
from .. import helpers
import logging
from decimal import Decimal, DecimalException
from .methods import SnowflakeMethods

log = logging.getLogger(__name__)


class Mtpms(SnowflakeMethods):

    def get_snowflake_mtpm_dim_data(self, plant_technology_id):

        records = []

        results = self.wrapper.validate_and_execute(f"""
                SELECT 
                    m.mtpm_id, 
                    pt.plant_technology_id, 
                    m.plant_id, 
                    m.mtpm_name, 
                    m.mtpm_display_name, 
                    m.area_name, 
                    m.has_big_energy_user_leading_indicator, 
                    m.has_big_water_user_leading_indicator, 
                    m.is_natural_resource_flag, 
                    m.ui_level, 
                    m.uom_metric, 
                    m.uom_imperial, 
                    m.big_energy_user_drilldown_display_text, 
                    m.big_water_user_drilldown_display_text,
                    m.target_type
                FROM 
                    {os.getenv("SNOWFLAKE_DATABASE")}
                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                    .DIM_MTPM m
                LEFT JOIN 
                    {os.getenv("SNOWFLAKE_DATABASE")}
                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                    ."DIM_PLANT_TECHNOLOGY" pt 
                ON m.plant_technology_id = pt.plant_technology_id 
                {f"WHERE m.plant_technology_id='{plant_technology_id}'" if plant_technology_id else ''}
                    """
                                                    )

        for res in results:
            records.append({
                'mtpm_id': res[0],
                'plant_technology_id': res[1],
                'plant_id': res[2],
                'mtpm_name': res[3],
                'mtpm_display_name': res[4],
                'area_name': res[5],
                'has_big_energy_user_leading_indicator': self.convert_boolean_value(res[6]),
                'has_big_water_user_leading_indicator': self.convert_boolean_value(res[7]),
                'is_natural_resource_flag': self.convert_boolean_value(res[8]),
                'ui_level': res[9],
                'uom_metric': res[10],
                'uom_imperial': res[11],
                'big_energy_user_drilldown_display_text': res[12],
                'big_water_user_drilldown_display_text': res[13],
                'target_type': res[14],
            })

        return records

    def get_snowflake_mtpm_target_data_generic(self, plant_technology_id, datetimestart, datetimeend,
                                       preferred_uom=None, mtpm_list=None, ui_level=None, opportunity_flag=None):

        if preferred_uom is not None and mtpm_list is not None:
            time_series_results = None
            if opportunity_flag is None:
                time_series_results = self.get_mtpm_ts_results(datetimestart, datetimeend, preferred_uom, mtpm_list)

            dim_results = self.get_mtpm_dim_results(plant_technology_id, datetimestart, datetimeend, preferred_uom, mtpm_list, ui_level)
            target_results = self.get_mtpm_target_results(preferred_uom, mtpm_list, datetimestart, datetimeend)
            return self.snowflake_build_mtpm_dim_object_preferred_uom(dim_results, time_series_results, target_results)

        else:
            mtpm_query = self.build_mtpm_query(plant_technology_id, datetimestart, datetimeend, mtpm_list, ui_level)
            dim_results = self.wrapper.validate_and_execute(mtpm_query)
            return self.snowflake_build_mtpm_dim_object(dim_results, opportunity_flag)
        return []
    
    def get_mtpm_dim_results(self, plant_technology_id, datetimestart, datetimeend, preferred_uom, mtpm_list, ui_level):
        mtpm_query = self.build_mtpm_query_preferred_uom(plant_technology_id, datetimestart, datetimeend, preferred_uom, mtpm_list, ui_level)
        dim_results = self.wrapper.validate_and_execute(mtpm_query)
        return dim_results

    def get_mtpm_ts_results(self, datetimestart, datetimeend, preferred_uom, mtpm_list):
        time_series_query = self.build_mtpm_time_series_query(datetimestart, datetimeend, preferred_uom, mtpm_list)
        ts_results = self.wrapper.validate_and_execute(time_series_query)
        return ts_results

    def get_mtpm_target_results(self, preferred_uom, mtpm_list, datetimestart, datetimeend):
        target_query = self.build_mtpm_target_query(preferred_uom, mtpm_list, datetimestart, datetimeend)
        target_results = self.wrapper.validate_and_execute(target_query)
        return target_results

    def build_mtpm_query(self, plant_technology_id, datetimestart, datetimeend, mtpm_list=None, ui_level=None):

        snowflake_db_and_schema = f'{os.getenv("SNOWFLAKE_DATABASE")}.{os.getenv("SNOWFLAKE_SCHEMA")}'

        financial_value = """b_opp_cte.opportunity_financial_value opportunity_financial_value_budget,
                pb_opp_cte.opportunity_financial_value opportunity_financial_value_pb,
                lfy_opp_cte.opportunity_financial_value opportunity_financial_value_lfy"""

        preferred_uom_query = 'mtpm_target_value_metric, mtpm_target_value_imperial'
        preferred_uom_t = 't.mtpm_target_value_metric, t.mtpm_target_value_imperial'
        uom = 'uom_imperial, uom_metric'
        uom_low_high = 'display_high_imperial, display_low_imperial, display_high_metric, display_low_metric'
        uom_min_max = 'max_imperial, min_imperial, max_metric, min_metric'
        mtpm_value = 'mtpm_value_imperial, mtpm_value_metric'
        targets_uom = """lfy_cte.mtpm_target_value_metric mtpm_target_value_metric_lfy, 
                        lfy_cte.mtpm_target_value_imperial mtpm_target_value_imperial_lfy,
                        pb_cte.mtpm_target_value_metric mtpm_target_value_metric_pb,
                        pb_cte.mtpm_target_value_imperial mtpm_target_value_imperial_pb,
                        b_cte.mtpm_target_value_metric mtpm_target_value_metric_budget,
                        b_cte.mtpm_target_value_imperial mtpm_target_value_imperial_budget"""

        if mtpm_list is not None:
            mtpm_list_enum = f"""{",".join([f"'{str(elem)}'" for i, elem in enumerate(mtpm_list)])}"""
            mtpm_id_clause = f"""WHERE m.mtpm_id IN ({mtpm_list_enum})"""
            opportunity_where_clause = f"""WHERE mtpm_id IN ({mtpm_list_enum}) AND opportunity_value_timestamp_local BETWEEN '{datetimestart}'
                            AND '{datetimeend}'"""
            cte_subquery = f"""WITH SORTED_MTPM_TARGETS AS (
                            SELECT
                                mtpm_id,
                                mtpm_target_type,
                                mtpm_target_value_timestamp_local,
                                {preferred_uom_query},
                                ROW_NUMBER() OVER (PARTITION BY mtpm_id,mtpm_target_type
                                ORDER BY mtpm_id, mtpm_target_value_timestamp_local desc) AS row_number
                                FROM FACT_MTPM_TARGET
                            WHERE
                                mtpm_id IN ({mtpm_list_enum})
                                AND mtpm_target_value_timestamp_local <= '{datetimeend}'
                                AND (mtpm_target_value_metric IS NOT NULL OR mtpm_target_value_imperial IS NOT NULL)
                        )
                        SELECT row_number as row_id, mtpm_id,
                                mtpm_target_type,
                                {preferred_uom_query}
                        FROM SORTED_MTPM_TARGETS
                        WHERE row_number = 1
                        ORDER BY mtpm_id, mtpm_target_type, mtpm_target_value_timestamp_local"""

        if ui_level is not None:
            mtpm_id_clause = f"""WHERE m.ui_level = '{ui_level}'"""
            opportunity_where_clause = ""
            cte_subquery = f"""SELECT RANK() OVER (ORDER BY mtpm_target_value_timestamp_utc ASC) row_id,
                                t.mtpm_id,
                                mtpm_target_type,
                                {preferred_uom_query}
                        FROM {os.getenv("SNOWFLAKE_DATABASE")}
                            .{os.getenv("SNOWFLAKE_SCHEMA")}
                            ."DIM_MTPM" m
                        LEFT JOIN
                            {os.getenv("SNOWFLAKE_DATABASE")}
                            .{os.getenv("SNOWFLAKE_SCHEMA")}
                            ."FACT_MTPM_TARGET" t  ON m.mtpm_id = t.mtpm_id
                        WHERE m.ui_level = '{ui_level}'
                        AND plant_technology_id = '{plant_technology_id}'
                        AND mtpm_target_value_timestamp_local <= '{datetimeend}'"""

        mtpm_query = f"""WITH cte_target_values(
                        row_id,
                        mtpm_id,
                        mtpm_target_type,
                        {preferred_uom_query}
                        ) AS (
                            {cte_subquery}
                        ),
                        cte_opportunity_values(
                        mtpm_id,
                        opportunity_type,
                        opportunity_financial_value
                    ) AS (
                        SELECT
                            mtpm_id,
                            opportunity_type,
                            SUM(opportunity_financial_value)
                        FROM
                            DIM_OPPORTUNITY op
                            LEFT JOIN FACT_OPPORTUNITY fo ON op.opportunity_id = fo.opportunity_id
                            {opportunity_where_clause}

                        GROUP BY
                            mtpm_id,
                            opportunity_type
                        ORDER BY
                            mtpm_id
                    )
                    SELECT
                        m.mtpm_id,
                        mtpm_name,
                        mtpm_display_name,
                        area_name,
                        has_big_energy_user_leading_indicator,
                        has_big_water_user_leading_indicator,
                        is_natural_resource_flag,
                        ui_level,
                        {targets_uom},
                        {financial_value},
                        (
                            CASE
                                WHEN (SELECT COUNT(*) FROM
                                    {os.getenv("SNOWFLAKE_DATABASE")}
                                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                                    ."DIM_OPPORTUNITY" op
                                    JOIN
                                    {os.getenv("SNOWFLAKE_DATABASE")}
                                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                                    ."FACT_OPPORTUNITY" fop
                                    ON op.opportunity_id = fop.opportunity_id
                                    WHERE op.mtpm_id = m.mtpm_id
                                    AND fop.opportunity_value_timestamp_local BETWEEN '{datetimestart}' AND '{datetimeend}') > 0
                                THEN 1
                                ELSE 0
                            END) has_oportunity,
                        (
                            CASE
                                WHEN (SELECT COUNT(*) FROM
                                    {os.getenv("SNOWFLAKE_DATABASE")}
                                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                                    ."DIM_LEADING_INDICATOR" li
                                    WHERE li.mtpm_id = m.mtpm_id) > 0 
                                THEN 1
                                ELSE 0
                            END) has_leading_indicator,
                        {mtpm_value},
                        mtpm_value_timestamp_utc,
                        mtpm_value_timestamp_local,
                        {uom},
                        {uom_low_high},
                        {uom_min_max},
                        m.target_type
                    FROM
                        {os.getenv("SNOWFLAKE_DATABASE")}
                        .{os.getenv("SNOWFLAKE_SCHEMA")}
                        ."DIM_MTPM" m
                    LEFT JOIN
                        {os.getenv("SNOWFLAKE_DATABASE")}
                        .{os.getenv("SNOWFLAKE_SCHEMA")}
                        ."FACT_MTPM" fm ON m.mtpm_id = fm.mtpm_id AND (fm.mtpm_value_timestamp_local >= '{datetimestart}' AND fm.mtpm_value_timestamp_local <= '{datetimeend}')
                    LEFT JOIN cte_target_values b_cte ON m.mtpm_id = b_cte.mtpm_id AND b_cte.mtpm_target_type = 'Budget'
                    LEFT JOIN cte_target_values lfy_cte ON m.mtpm_id = lfy_cte.mtpm_id AND lfy_cte.mtpm_target_type = 'LFY'
                    LEFT JOIN cte_target_values pb_cte ON m.mtpm_id = pb_cte.mtpm_id AND pb_cte.mtpm_target_type = 'PB'
                    LEFT JOIN cte_opportunity_values b_opp_cte ON m.mtpm_id = b_opp_cte.mtpm_id
                    AND b_opp_cte.opportunity_type = 'OP Budget'
                    LEFT JOIN cte_opportunity_values lfy_opp_cte ON m.mtpm_id = lfy_opp_cte.mtpm_id
                    AND lfy_opp_cte.opportunity_type = 'OP LFY'
                    LEFT JOIN cte_opportunity_values pb_opp_cte ON m.mtpm_id = pb_opp_cte.mtpm_id
                    AND pb_opp_cte.opportunity_type = 'OP PB'
                    {mtpm_id_clause}
                    AND m.plant_technology_id = '{plant_technology_id}'
                    ORDER BY mtpm_value_timestamp_local ASC
                    """
        return mtpm_query

    def build_mtpm_query_preferred_uom(self, plant_technology_id, datetimestart, datetimeend,
                                       preferred_uom, mtpm_list=None, ui_level=None):

        snowflake_db_and_schema = f'{os.getenv("SNOWFLAKE_DATABASE")}.{os.getenv("SNOWFLAKE_SCHEMA")}'

        financial_value = """b_opp_cte.opportunity_financial_value opportunity_financial_value_budget,                        
                pb_opp_cte.opportunity_financial_value opportunity_financial_value_pb,                        
                lfy_opp_cte.opportunity_financial_value opportunity_financial_value_lfy"""

        target_values = f"""lfy_cte.mtpm_target_value_{preferred_uom} mtpm_target_value_{preferred_uom}_lfy,
            pb_cte.mtpm_target_value_{preferred_uom} mtpm_target_value_{preferred_uom}_pb,
            b_cte.mtpm_target_value_{preferred_uom} mtpm_target_value_{preferred_uom}_budget"""

        uom = 'uom_imperial' if preferred_uom == 'imperial' else 'uom_metric'
        uom_low_high = 'display_high_imperial, display_low_imperial' if preferred_uom == 'imperial' else 'display_high_metric, display_low_metric'
        uom_min_max = 'max_imperial, min_imperial' if preferred_uom == 'imperial' else 'max_metric, min_metric'

        if mtpm_list is not None:
            mtpm_list_enum = f"""{",".join([f"'{str(elem)}'" for i, elem in enumerate(mtpm_list)])}"""
            mtpm_id_clause = f"""WHERE m.mtpm_id IN ({mtpm_list_enum})"""
            opportunity_where_clause = f"""WHERE mtpm_id IN ({mtpm_list_enum}) AND opportunity_value_timestamp_local BETWEEN '{datetimestart}'
                            AND '{datetimeend}'"""

        if ui_level is not None:
            mtpm_id_clause = f"""WHERE m.ui_level = '{ui_level}'"""
            opportunity_where_clause = ""

        cte_subquery = f"""WITH SORTED_MTPM_TARGETS AS (
                            SELECT 
                                mtpm_id, 
                                mtpm_target_type,
                                mtpm_target_value_timestamp_local,
                                mtpm_target_value_{preferred_uom}, 
                                ROW_NUMBER() OVER (PARTITION BY mtpm_id,mtpm_target_type 
                                ORDER BY mtpm_id, mtpm_target_value_timestamp_local desc) AS row_number
                                FROM FACT_MTPM_TARGET
                            WHERE 
                                mtpm_id IN ({mtpm_list_enum}) 
                                AND mtpm_target_value_timestamp_local <= '{datetimeend}'
                                AND mtpm_target_value_{preferred_uom} IS NOT NULL
                        ) 
                        SELECT row_number as row_id, mtpm_id, 
                                mtpm_target_type, 
                                mtpm_target_value_{preferred_uom}
                        FROM SORTED_MTPM_TARGETS
                        WHERE row_number = 1
                        ORDER BY mtpm_id, mtpm_target_type, mtpm_target_value_timestamp_local"""

        mtpm_query = f"""WITH cte_target_values(
                        row_id, 
                        mtpm_id, 
                        mtpm_target_type, 
                        mtpm_target_value_{preferred_uom}
                        ) AS (
                            {cte_subquery}   
                        ),
                        cte_opportunity_values(
                        mtpm_id,
                        opportunity_type,
                        opportunity_financial_value
                    ) AS (
                        SELECT
                            mtpm_id,
                            opportunity_type,
                            SUM(opportunity_financial_value)
                        FROM
                            DIM_OPPORTUNITY op
                            LEFT JOIN FACT_OPPORTUNITY fo ON op.opportunity_id = fo.opportunity_id
                            {opportunity_where_clause}
                            
                        GROUP BY
                            mtpm_id,
                            opportunity_type
                        ORDER BY
                            mtpm_id
                    )
                    SELECT 
                        m.mtpm_id,
                        mtpm_name, 
                        mtpm_display_name, 
                        area_name, 
                        has_big_energy_user_leading_indicator, 
                        has_big_water_user_leading_indicator, 
                        is_natural_resource_flag, 
                        ui_level, 
                        {financial_value},
                        {target_values},
                        (
                            CASE  
                                WHEN (SELECT COUNT(*) FROM 
                                    {snowflake_db_and_schema}
                                    ."DIM_OPPORTUNITY" op
                                    JOIN 
                                    {snowflake_db_and_schema}
                                    ."FACT_OPPORTUNITY" fop 
                                    ON op.opportunity_id = fop.opportunity_id 
                                    WHERE op.mtpm_id = m.mtpm_id 
                                    AND fop.opportunity_value_timestamp_local BETWEEN '{datetimestart}' AND '{datetimeend}') > 0 
                                THEN 1 
                                ELSE 0
                            END) has_oportunity,
                        (
                            CASE
                                WHEN (SELECT COUNT(*) FROM 
                                    {snowflake_db_and_schema}
                                    ."DIM_LEADING_INDICATOR" li 
                                    WHERE li.mtpm_id = m.mtpm_id) > 0
                                THEN 1 
                                ELSE 0
                            END) has_leading_indicator,
                        {uom},
                        {uom_low_high},
                        {uom_min_max},
                        m.target_type,
                        m.targets_are_calculated_flag
                    FROM
                        {snowflake_db_and_schema}
                        ."DIM_MTPM" m
                    LEFT JOIN cte_target_values b_cte ON m.mtpm_id = b_cte.mtpm_id AND b_cte.mtpm_target_type = 'Budget'
                    LEFT JOIN cte_target_values lfy_cte ON m.mtpm_id = lfy_cte.mtpm_id AND lfy_cte.mtpm_target_type = 'LFY'
                    LEFT JOIN cte_target_values pb_cte ON m.mtpm_id = pb_cte.mtpm_id AND pb_cte.mtpm_target_type = 'PB'
                    LEFT JOIN cte_opportunity_values b_opp_cte ON m.mtpm_id = b_opp_cte.mtpm_id
                    AND b_opp_cte.opportunity_type = 'OP Budget'
                    LEFT JOIN cte_opportunity_values lfy_opp_cte ON m.mtpm_id = lfy_opp_cte.mtpm_id
                    AND lfy_opp_cte.opportunity_type = 'OP LFY'
                    LEFT JOIN cte_opportunity_values pb_opp_cte ON m.mtpm_id = pb_opp_cte.mtpm_id
                    AND pb_opp_cte.opportunity_type = 'OP PB'
                    {mtpm_id_clause}
                    AND m.plant_technology_id = '{plant_technology_id}'
                    """
        return mtpm_query    

    def build_mtpm_time_series_query(self, datetimestart, datetimeend, preferred_uom, mtpm_id):
        mtpm_list_enum = f"""({",".join([f"'{str(elem)}'" for i, elem in enumerate(mtpm_id)])})"""
        query = f"""WITH cte_target_values(
                        row_id, 
                        mtpm_id, 
                        mtpm_target_type, 
                        mtpm_target_value_{preferred_uom}
                        ) AS (
                            WITH SORTED_MTPM_TARGETS AS (
                            SELECT 
                                mtpm_id, 
                                mtpm_target_type,
                                mtpm_target_value_timestamp_local,
                                mtpm_target_value_{preferred_uom}, 
                                ROW_NUMBER() OVER (PARTITION BY mtpm_id,mtpm_target_type 
                                ORDER BY mtpm_id, mtpm_target_value_timestamp_local desc) AS row_number
                                FROM FACT_MTPM_TARGET
                            WHERE 
                                mtpm_id in {mtpm_list_enum}
                                AND mtpm_target_value_timestamp_local <= '{datetimeend}'
                                AND (mtpm_target_value_{preferred_uom} IS NOT NULL)
                        ) 
                        SELECT row_number as row_id, mtpm_id, 
                                mtpm_target_type, 
                                mtpm_target_value_{preferred_uom}
                        FROM SORTED_MTPM_TARGETS
                        WHERE row_number = 1
                        ORDER BY mtpm_id, mtpm_target_type, mtpm_target_value_timestamp_local   
                        )
                        SELECT 
                        fm.mtpm_value_{preferred_uom},
                        fm.mtpm_value_timestamp_utc,
                        lfy_cte.mtpm_target_value_{preferred_uom} mtpm_target_value_{preferred_uom}_lfy, 
                        pb_cte.mtpm_target_value_{preferred_uom} mtpm_target_value_{preferred_uom}_pb, 
                        b_cte.mtpm_target_value_{preferred_uom} mtpm_target_value_{preferred_uom}_budget
                     FROM 
                        {os.getenv("SNOWFLAKE_DATABASE")}
                        .{os.getenv("SNOWFLAKE_SCHEMA")}
                        ."FACT_MTPM" fm
                    LEFT JOIN cte_target_values b_cte ON fm.mtpm_id = b_cte.mtpm_id AND b_cte.mtpm_target_type = 'Budget'
                    LEFT JOIN cte_target_values lfy_cte ON fm.mtpm_id = lfy_cte.mtpm_id AND lfy_cte.mtpm_target_type = 'LFY'
                    LEFT JOIN cte_target_values pb_cte ON fm.mtpm_id = pb_cte.mtpm_id AND pb_cte.mtpm_target_type = 'PB'
                    WHERE fm.mtpm_id in {mtpm_list_enum}  
                    AND fm.mtpm_value_timestamp_local BETWEEN '{datetimestart}' AND '{datetimeend}'
                    ORDER BY fm.mtpm_value_timestamp_local ASC"""
        return query

    def build_mtpm_target_query(self, preferred_uom, mtpm_id, datetimestart, datetimeend):
        mtpm_list_enum = f"""({",".join([f"'{str(elem)}'" for i, elem in enumerate(mtpm_id)])})"""
        query = f"""SELECT DISTINCT TOP 300 fmt.mtpm_target_value_timestamp_utc, fmt.mtpm_target_value_{preferred_uom}, fmt.mtpm_target_type
                    FROM 
                        {os.getenv("SNOWFLAKE_DATABASE")}
                        .{os.getenv("SNOWFLAKE_SCHEMA")}
                        ."FACT_MTPM_TARGET" fmt
                WHERE fmt.mtpm_id in {mtpm_list_enum}
                AND fmt.mtpm_target_value_{preferred_uom} IS NOT NULL
                AND fmt.mtpm_target_value_timestamp_utc >= '{datetimestart}'
                AND fmt.mtpm_target_value_timestamp_utc <= '{datetimeend}'
                AND fmt.mtpm_target_value_{preferred_uom} != 0
                ORDER BY fmt.mtpm_target_value_timestamp_utc ASC"""
        return query

    def snowflake_build_mtpm_dim_object(self, results, opportunity_flag=None):

        records = []

        # Extract unique mtpm ids from resultset and loop them
        mtpm_ids = set(map(lambda a: a[0], results))

        for mtpm in mtpm_ids:
            # Filter the dataset by each mtpm ID, and take only the first row for header/dim values
            filtered_mtpms = list(filter(lambda r: r[0] == mtpm, results))

            record = {
                'mtpm_id': filtered_mtpms[0][0],
                'mtpm_name': filtered_mtpms[0][1],
                'mtpm_display_name': filtered_mtpms[0][2],
                'area_name': filtered_mtpms[0][3],
                'has_big_energy_user_leading_indicator': self.convert_boolean_value(filtered_mtpms[0][4]),
                'has_big_water_user_leading_indicator': self.convert_boolean_value(filtered_mtpms[0][5]),
                'is_natural_resource_flag': self.convert_boolean_value(filtered_mtpms[0][6]),
                'ui_level': filtered_mtpms[0][7],
                'mtpm_target_value_metric_lfy': filtered_mtpms[0][8],
                'mtpm_target_value_imperial_lfy': filtered_mtpms[0][9],
                'mtpm_target_value_metric_pb': filtered_mtpms[0][10],
                'mtpm_target_value_imperial_pb': filtered_mtpms[0][11],
                'mtpm_target_value_metric_budget': filtered_mtpms[0][12],
                'mtpm_target_value_imperial_budget': filtered_mtpms[0][13],
                'opportunity_financial_value_budget': filtered_mtpms[0][14],
                'opportunity_financial_value_pb': filtered_mtpms[0][15],
                'opportunity_financial_value_lfy': filtered_mtpms[0][16],
                'has_oportunity': self.convert_boolean_value(filtered_mtpms[0][17]),
                'has_leading_indicator': self.convert_boolean_value(filtered_mtpms[0][18]),
                'uom_imperial': filtered_mtpms[0][23],
                'uom_metric': filtered_mtpms[0][24],
                'display_high_imperial': filtered_mtpms[0][25],
                'display_low_imperial': filtered_mtpms[0][26],
                'display_high_metric': filtered_mtpms[0][27],
                'display_low_metric': filtered_mtpms[0][28],
                'max_imperial': filtered_mtpms[0][29],
                'min_imperial': filtered_mtpms[0][30],
                'max_metric': filtered_mtpms[0][31],
                'min_metric': filtered_mtpms[0][32],
                'target_type': filtered_mtpms[0][33],
            }

            if opportunity_flag is None:
                # Pass the entire filtered datase to build the time series array inside the main object
                record['time_series'] = self.get_snowflake_mtpm_time_series(filtered_mtpms)

            records.append(record)

        return records

    def snowflake_build_mtpm_dim_object_preferred_uom(self, dim_results, time_series_results, target_results=None):
        records = []

        for mtpm in dim_results:
            record = {
                'mtpm_id': mtpm[0],
                'mtpm_name': mtpm[1],
                'mtpm_display_name': mtpm[2],
                'area_name': mtpm[3],
                'has_big_energy_user_leading_indicator': self.convert_boolean_value(mtpm[4]),
                'has_big_water_user_leading_indicator': self.convert_boolean_value(mtpm[5]),
                'is_natural_resource_flag': self.convert_boolean_value(mtpm[6]),
                'ui_level': mtpm[7],
                'opportunity_financial_value_budget': mtpm[8],
                'opportunity_financial_value_pb': mtpm[9],
                'opportunity_financial_value_lfy': mtpm[10],
                'mtpm_target_value_lfy': mtpm[11],
                'mtpm_target_value_pb': mtpm[12],
                'mtpm_target_value_budget': mtpm[13],
                'has_oportunity': self.convert_boolean_value(mtpm[14]),
                'has_leading_indicator': self.convert_boolean_value(mtpm[15]),
                f'uom': mtpm[16],
                f'display_high': self.convert_to_decimal(mtpm[17], 2),
                f'display_low': self.convert_to_decimal(mtpm[18], 2),
                f'max': self.convert_to_decimal(mtpm[19]),
                f'min': self.convert_to_decimal(mtpm[20]),
                f'target_type': mtpm[21],
            }

            if time_series_results is not None:
                # Pass the entire filtered datase to build the time series array inside the main object
                time_series_data = self.get_mtpm_time_series_preferred_uom(time_series_results, target_results, mtpm[11], mtpm[12], mtpm[13], self.convert_boolean_value(mtpm[22]))
                record['time_series'] = time_series_data[0]
                record['ts_min'] = time_series_data[1]
                record['ts_max'] = time_series_data[2]

            records.append(record)

        return records

    def get_mtpm_time_series_preferred_uom(self, time_series_results, target_results=None, lfy_dim=None, pb_dim=None, budget_dim=None, dynamic_targets=False):
        records = []

        max = None
        min = None
        lfy_vals = None
        pb_vals = None
        bdgt_vals = None

        if target_results:
            lfy_vals = [x for x in target_results if (len(x) > 2 and x[2].lower() == 'lfy')]
            pb_vals = [x for x in target_results if (len(x) > 2 and x[2].lower() == 'pb')]
            bdgt_vals = [x for x in target_results if (len(x) > 2 and x[2].lower() == 'budget')]

        for res in time_series_results:
            time = None
            formatted_date = ''
            lfy = None
            pb = None
            bdgt = None

            if res[1] is not None:
                time = res[1]
                formatted_date = helpers.format_datetime(time)

            # Round value to 4 decimals
            value = res[0]
            if value is not None:
                value = round(value, 4)

            if max is None or (value is not None and value > max):
                max = value
            if min is None or (value is not None and value < min):
                min = value

            if dynamic_targets:
                if target_results:
                    series_time = helpers.get_datetime_obj(time)
                    for val in lfy_vals:
                        target_time = helpers.get_datetime_obj(val[0])
                        if target_time <= series_time:
                            lfy = val[1]
                        if target_time > series_time:
                            break
                    for val in pb_vals:
                        target_time = helpers.get_datetime_obj(val[0])
                        if target_time <= series_time:
                            pb = val[1]
                        if target_time > series_time:
                            break
                    for val in bdgt_vals:
                        target_time = helpers.get_datetime_obj(val[0])
                        if target_time <= series_time:
                            bdgt = val[1]
                        if target_time > series_time:
                            break
            if not dynamic_targets or len(target_results) < 1:
                # populate targets from dim query
                if lfy is None:
                    lfy = lfy_dim
                if pb is None:
                    pb = pb_dim
                if bdgt is None:
                    bdgt = budget_dim

                    
            # Send empty values in case of nulls for frontend use
            records.append({
                'value': value,
                'date': time,
                'date_formatted': formatted_date,
                'epoch_local': helpers.get_epoch(time),
                'target_value_lfy': lfy,    
                'target_value_pb': pb,    
                'target_value_budget': bdgt
            })

        return [records, min, max]

    def get_snowflake_mtpm_time_series(self, filtered_mtpms):

        records = []

        for res in filtered_mtpms:
            # Send empty values in case of nulls for frontend use
            records.append({
                'mtpm_value_metric': self.handle_nulls(res[20]),
                'mtpm_value_imperial': self.handle_nulls(res[19]),
                'mtpm_value_timestamp_utc': helpers.trim_date_string(self.handle_nulls(res[21])),
                'mtpm_value_timestamp_local': helpers.trim_date_string(self.handle_nulls(res[22])),
                'epoch_local': helpers.get_epoch(self.handle_nulls(res[22])),
            })

        return records