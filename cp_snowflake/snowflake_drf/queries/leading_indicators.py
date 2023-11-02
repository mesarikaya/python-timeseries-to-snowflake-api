import os
from snowflake_wrapper.base import SnowflakeWrapper
from datetime import timedelta, datetime
import concurrent.futures
from .. import helpers
import logging
from decimal import Decimal, DecimalException
from .methods import SnowflakeMethods

log = logging.getLogger(__name__)


class LeadingIndicators(SnowflakeMethods):
    

    def get_snowflake_leading_indicator_dim_data(self, mtpm_id, leading_indicator_id=''):

        records = []

        if leading_indicator_id:
            leading_indicator_id = f"AND leading_indicator_id = '{leading_indicator_id}'"

        results = self.wrapper.validate_and_execute(f"""
                SELECT 
                    leading_indicator_id,
                    leading_indicator_name,
                    leading_indicator_display_name,
                    is_big_energy_user,
                    is_big_water_user,
                    corrective_action,
                    corrective_action_input_language,
                    cmo,
                    display_high_metric,
                    display_low_metric,
                    display_high_imperial,
                    display_low_imperial,
                    max_metric,
                    min_metric,
                    max_imperial,
                    min_imperial,
                    uom_metric,
                    uom_imperial,
                    uom_inside_envelope,
                    pi_vision_display_url                
                FROM 
                    {os.getenv("SNOWFLAKE_DATABASE")}
                    .{os.getenv("SNOWFLAKE_SCHEMA")}
                    .DIM_LEADING_INDICATOR                    
                WHERE mtpm_id = '{mtpm_id}'
                {leading_indicator_id if leading_indicator_id else ''}
                    """
                                                    )

        for leading_indicator in results:
            records.append({
                'leading_indicator_id': leading_indicator[0],
                'leading_indicator_name': leading_indicator[1],
                'leading_indicator_display_name': leading_indicator[2],
                'is_big_energy_user': self.convert_boolean_value(leading_indicator[3]),
                'is_big_water_user': self.convert_boolean_value(leading_indicator[4]),
                'corrective_action': leading_indicator[5],
                'corrective_action_input_language': leading_indicator[6],
                'cmo': leading_indicator[7],
                'display_high_metric': leading_indicator[8],
                'display_low_metric': leading_indicator[9],
                'display_high_imperial': leading_indicator[10],
                'display_low_imperial': leading_indicator[11],
                'max_metric': leading_indicator[12],
                'min_metric': leading_indicator[13],
                'max_imperial': leading_indicator[14],
                'min_imperial': leading_indicator[15],
                'uom_metric': leading_indicator[16],
                'uom_imperial': leading_indicator[17],
                'uom_inside_envelope': leading_indicator[18],
                'pi_vision_display_url': leading_indicator[19],
            })

        return records

    def get_snowflake_leading_indicator_metric_data(self, leading_indicator_id, mtpm_id, date_time_start,
                                                    date_time_end):

        records = []

        snowflake_db_and_schema = f'{os.getenv("SNOWFLAKE_DATABASE")}.{os.getenv("SNOWFLAKE_SCHEMA")}'

        start_date = helpers.get_datetime_obj(date_time_start)
        end_date = helpers.get_datetime_obj(date_time_end)

        results = self.get_snowflake_leading_indicator_metric(start_date, end_date, snowflake_db_and_schema,
                                                              mtpm_id, leading_indicator_id)

        leading_indicator = results[0]

        records.append({
            'leading_indicator_id': leading_indicator[0],
            'leading_indicator_name': leading_indicator[1],
            'leading_indicator_display_name': leading_indicator[2],
            'is_big_energy_user': self.convert_boolean_value(leading_indicator[3]),
            'is_big_water_user': self.convert_boolean_value(leading_indicator[4]),
            'corrective_action': leading_indicator[5],
            'corrective_action_input_language': leading_indicator[6],
            'cmo': leading_indicator[7],
            'display_high_metric': leading_indicator[8],
            'display_low_metric': leading_indicator[9],
            'display_high_imperial': leading_indicator[10],
            'display_low_imperial': leading_indicator[11],
            'max_metric': leading_indicator[12],
            'min_metric': leading_indicator[13],
            'max_imperial': leading_indicator[14],
            'min_imperial': leading_indicator[15],
            'uom_metric': leading_indicator[16],
            'uom_imperial': leading_indicator[17],
            'uom_inside_envelope': leading_indicator[18],
            'pi_vision_display_url': leading_indicator[24],
            'has_equipment_tags': leading_indicator[23],
            'leading_indicator_inside_envelope_last1h_value': leading_indicator[19],
            'leading_indicator_inside_envelope_last8h_value': leading_indicator[20],
            'leading_indicator_inside_envelope_last12h_value': leading_indicator[21],
            'leading_indicator_inside_envelope_last24h_value': leading_indicator[22],
            # Pass the entire filtered datase to build the time series array inside the main object
            'time_series': self.get_snowflake_leading_indicator_time_series(results[1])
        })

        return records

    def get_snowflake_leading_indicator_time_series(self, filtered_indicators):

        records = []

        for res in filtered_indicators:
            records.append({
                'leading_indicator_value_metric': res[0],
                'leading_indicator_value_imperial': res[1],
                'leading_indicator_value_timestamp_utc': res[2],
                'leading_indicator_value_timestamp_local': res[3],
                'is_leading_indicator_substituted_flag': self.convert_boolean_value(res[4])
            })

        return records

    def get_snowflake_leading_indicator_metric(self, date_time_start, date_time_end, snowflake_db_and_schema,
                                               mtpm_id,
                                               leading_indicator_id):

        leading_indicator_id_filter = ''
        time_series_filter = ''
        li_where_query = ''

        if not leading_indicator_id:
            leading_indicator_id = self.get_leading_indicator_id(mtpm_id, snowflake_db_and_schema)

        leading_indicator_id_filter = f"AND l.leading_indicator_id = '{leading_indicator_id}'"
        time_series_filter = f"fl.leading_indicator_id = '{leading_indicator_id}' AND "
        li_where_query = f"WHERE l.leading_indicator_id = '{leading_indicator_id}'"

        dim_payload = self.get_leading_indicator_metric_dim_data(date_time_end, snowflake_db_and_schema,
                                                                 time_series_filter, li_where_query)
        time_series_payload = self.get_leading_indicator_metric_time_series_data(date_time_start, date_time_end,
                                                                                 snowflake_db_and_schema,
                                                                                 time_series_filter)
        return [dim_payload, time_series_payload]

    def get_leading_indicator_id(self, mtpm_id, snowflake_db_and_schema):

        select_query = f"""SELECT leading_indicator_id FROM {snowflake_db_and_schema}.DIM_LEADING_INDICATOR
            WHERE mtpm_id = {mtpm_id}"""
        results = self.wrapper.validate_and_execute(select_query)

        return results[0][0]

    def get_leading_indicator_metric_time_series_data(self, date_time_start, date_time_end,
                                                      snowflake_db_and_schema, time_series_filter):

        time_series_query = f"""SELECT
                fl.leading_indicator_value_metric,
                fl.leading_indicator_value_imperial,
                fl.leading_indicator_value_timestamp_utc,
                fl.leading_indicator_value_timestamp_local,
                fl.is_leading_indicator_substituted_flag
            FROM
                {snowflake_db_and_schema}.FACT_LEADING_INDICATOR fl
            WHERE
                {time_series_filter}
                (
                    leading_indicator_value_timestamp_utc >= '{date_time_start}'
                    AND leading_indicator_value_timestamp_utc <= '{date_time_end}'
                )
            ORDER BY
                leading_indicator_value_timestamp_utc"""

        time_series_payload = self.wrapper.validate_and_execute(time_series_query)

        time_series_payload = self.normalize_time_series(time_series_payload, date_time_start, date_time_end, 2)

        return time_series_payload

    def get_leading_indicator_metric_dim_data(self, date_time_end, snowflake_db_and_schema, time_series_filter,
                                              li_where_query):

        li_dim_query = f"""SELECT
                l.leading_indicator_id,
                l.leading_indicator_name,
                l.leading_indicator_display_name,
                l.is_big_energy_user,
                l.is_big_water_user,
                l.corrective_action,
                l.corrective_action_input_language,
                l.cmo,
                l.display_high_metric,
                l.display_low_metric,
                l.display_high_imperial,
                l.display_low_imperial,
                l.max_metric,
                l.min_metric,
                l.max_imperial,
                l.min_imperial,
                l.uom_metric,
                l.uom_imperial,
                l.uom_inside_envelope,
                (SELECT TOP 1 leading_indicator_inside_envelope_last1h_value
            
            FROM
                {snowflake_db_and_schema}.FACT_LEADING_INDICATOR_TARGET_HEALTH fl
            WHERE
                {time_series_filter}
                leading_indicator_value_timestamp_utc <= '{date_time_end}'
                AND leading_indicator_inside_envelope_last1h_value IS NOT NULL
            ORDER BY
                leading_indicator_value_timestamp_utc desc) as leading_indicator_inside_enevelope_last1h_value,
            (SELECT TOP 1 leading_indicator_inside_envelope_last8h_value
            
            FROM
                {snowflake_db_and_schema}.FACT_LEADING_INDICATOR_TARGET_HEALTH fl
            WHERE
                {time_series_filter}
                leading_indicator_value_timestamp_utc <= '{date_time_end}'
                AND leading_indicator_inside_envelope_last8h_value IS NOT NULL
            ORDER BY
                leading_indicator_value_timestamp_utc desc) as leading_indicator_inside_envelope_last8h_value,
            (SELECT TOP 1 leading_indicator_inside_envelope_last12h_value
            
            FROM
                {snowflake_db_and_schema}.FACT_LEADING_INDICATOR_TARGET_HEALTH fl
            WHERE
                {time_series_filter}
                leading_indicator_value_timestamp_utc <= '{date_time_end}'
                AND leading_indicator_inside_envelope_last12h_value IS NOT NULL
            ORDER BY
                leading_indicator_value_timestamp_utc desc) as leading_indicator_inside_envelope_last12h_value,
            (SELECT TOP 1 leading_indicator_inside_envelope_last24h_value
            
            FROM
                {snowflake_db_and_schema}.FACT_LEADING_INDICATOR_TARGET_HEALTH fl
            WHERE
                {time_series_filter}
                leading_indicator_value_timestamp_utc <= '{date_time_end}'
                AND leading_indicator_inside_envelope_last24h_value IS NOT NULL
            ORDER BY
                leading_indicator_value_timestamp_utc desc) as leading_indicator_inside_envelope_last24h_value,
                CASE
                    WHEN EXISTS (
                        SELECT
                            det.equipment_tag_id
                        FROM
                            DIM_EQUIPMENT_TAG det
                        WHERE
                            det.leading_indicator_id = l.leading_indicator_id
                    ) THEN 'true'
                    ELSE 'false'
                END as has_equipment_tags,
            pi_vision_display_url
            FROM
                {snowflake_db_and_schema}.DIM_LEADING_INDICATOR l
            {li_where_query}"""

        dim_payload = self.wrapper.validate_and_execute(li_dim_query)

        return dim_payload[0]

    def get_snowflake_leading_indicator_metric_data_beta(self, leading_indicator_id, mtpm_id, date_time_start,
                                                         date_time_end, preferred_uom, timezone):

        records = []

        snowflake_db_and_schema = f'{os.getenv("SNOWFLAKE_DATABASE")}.{os.getenv("SNOWFLAKE_SCHEMA")}'

        start_date = helpers.get_datetime_obj(date_time_start)
        end_date = helpers.get_datetime_obj(date_time_end)

        results = self.get_snowflake_leading_indicator_metric_beta(start_date, end_date, snowflake_db_and_schema,
                                                                   mtpm_id, leading_indicator_id, preferred_uom)

        leading_indicator = results[0]

        time_series_data = self.get_snowflake_leading_indicator_time_series_and_sum(results[1], timezone)

        records.append({
            'leading_indicator_id': leading_indicator[0],
            'leading_indicator_name': leading_indicator[1],
            'leading_indicator_display_name': leading_indicator[2],
            'is_big_energy_user': self.convert_boolean_value(leading_indicator[3]),
            'is_big_water_user': self.convert_boolean_value(leading_indicator[4]),
            'corrective_action': leading_indicator[5],
            'corrective_action_input_language': leading_indicator[6],
            'cmo': leading_indicator[7],
            'display_high': self.convert_to_decimal(leading_indicator[8]),
            'display_low': self.convert_to_decimal(leading_indicator[9]),
            'ts_min': time_series_data[1],
            'ts_max': time_series_data[2],
            'uom': leading_indicator[10],
            'uom_inside_envelope': leading_indicator[11],
            'pi_vision_display_url': leading_indicator[17],
            'max_target_value': leading_indicator[18],
            'min_target_value': leading_indicator[19],
            'has_equipment_tags': leading_indicator[16],
            'l1h': leading_indicator[12],
            'l8h': leading_indicator[13],
            'l12h': leading_indicator[14],
            'l24h': leading_indicator[15],
            # Pass the entire filtered datase to build the time series array inside the main object
            'time_series': time_series_data[0]
        })

        return records

    def get_snowflake_leading_indicator_time_series_and_sum(self, filtered_indicators, timezone=None):

        records = []

        max = None
        min = None

        timezone = 'UTC' if timezone is None else timezone
        last_valid_value = None

        for res in filtered_indicators:
            if res[1] is None:
                res_value = None
            else:
                res_value = Decimal(round(Decimal(res[1]), 4))
                last_valid_value = res_value
            if max is None or (res_value is not None and res_value > max):
                max = res_value
            if min is None or (res_value is not None and res_value < min):
                min = res_value
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
                'time_epoch': time_epoch
            })

        if max is not None:
            max = Decimal(max)
        if min is not None:
            min = Decimal(min)

        return [records, min, max]

    def get_snowflake_leading_indicator_metric_beta(self, date_time_start, date_time_end, snowflake_db_and_schema,
                                                    mtpm_id, leading_indicator_id, preferred_uom):

        leading_indicator_id_filter = ''
        time_series_filter = ''
        li_where_query = ''

        if not leading_indicator_id:
            leading_indicator_id = self.get_leading_indicator_id(mtpm_id, snowflake_db_and_schema)

        leading_indicator_id_filter = f"AND l.leading_indicator_id = '{leading_indicator_id}'"
        time_series_filter = f"fl.leading_indicator_id = '{leading_indicator_id}' AND "
        li_where_query = f"WHERE l.leading_indicator_id = '{leading_indicator_id}'"

        dim_payload = self.get_leading_indicator_metric_dim_data_beta(date_time_end, snowflake_db_and_schema,
                                                                      time_series_filter, li_where_query, preferred_uom)
        time_series_payload = self.get_leading_indicator_metric_time_series_data_beta(date_time_start, date_time_end,
                                                                                      snowflake_db_and_schema,
                                                                                      time_series_filter, preferred_uom)
        return [dim_payload, time_series_payload]

    def get_leading_indicator_metric_time_series_data_beta(self, date_time_start, date_time_end,
                                                           snowflake_db_and_schema, time_series_filter, preferred_uom):

        time_series_query = f"""SELECT
                fl.leading_indicator_value_timestamp_utc as time,
                fl.leading_indicator_value_{preferred_uom} as value
            FROM
                {snowflake_db_and_schema}.FACT_LEADING_INDICATOR fl
            WHERE
                {time_series_filter}
                (
                    leading_indicator_value_timestamp_utc >= '{date_time_start}'
                    AND leading_indicator_value_timestamp_utc <= '{date_time_end}'
                )
            ORDER BY
                leading_indicator_value_timestamp_utc"""

        time_series_payload = self.wrapper.validate_and_execute(time_series_query)

        time_series_payload = self.normalize_time_series(time_series_payload, date_time_start, date_time_end)

        return time_series_payload

    def get_leading_indicator_metric_dim_data_beta(self, date_time_end, snowflake_db_and_schema,
                                                   time_series_filter, li_where_query, preferred_uom):

        li_dim_query = f"""SELECT
                l.leading_indicator_id,
                l.leading_indicator_name,
                l.leading_indicator_display_name,
                l.is_big_energy_user,
                l.is_big_water_user,
                l.corrective_action,
                l.corrective_action_input_language,
                l.cmo,
                l.display_high_{preferred_uom} as display_high,
                l.display_low_{preferred_uom} as display_low,
                l.uom_{preferred_uom} as uom,
                l.uom_inside_envelope,
                (SELECT TOP 1 leading_indicator_inside_envelope_last1h_value
            
            FROM
                {snowflake_db_and_schema}.FACT_LEADING_INDICATOR_TARGET_HEALTH fl
            WHERE
                {time_series_filter}
                leading_indicator_value_timestamp_utc <= '{date_time_end}'
                AND leading_indicator_inside_envelope_last1h_value IS NOT NULL
            ORDER BY
                leading_indicator_value_timestamp_utc desc) as leading_indicator_inside_enevelope_last1h_value,
            (SELECT TOP 1 leading_indicator_inside_envelope_last8h_value
            
            FROM
                {snowflake_db_and_schema}.FACT_LEADING_INDICATOR_TARGET_HEALTH fl
            WHERE
                {time_series_filter}
                leading_indicator_value_timestamp_utc <= '{date_time_end}'
                AND leading_indicator_inside_envelope_last8h_value IS NOT NULL
            ORDER BY
                leading_indicator_value_timestamp_utc desc) as leading_indicator_inside_envelope_last8h_value,
            (SELECT TOP 1 leading_indicator_inside_envelope_last12h_value
            
            FROM
                {snowflake_db_and_schema}.FACT_LEADING_INDICATOR_TARGET_HEALTH fl
            WHERE
                {time_series_filter}
                leading_indicator_value_timestamp_utc <= '{date_time_end}'
                AND leading_indicator_inside_envelope_last12h_value IS NOT NULL
            ORDER BY
                leading_indicator_value_timestamp_utc desc) as leading_indicator_inside_envelope_last12h_value,
            (SELECT TOP 1 leading_indicator_inside_envelope_last24h_value

            FROM
                {snowflake_db_and_schema}.FACT_LEADING_INDICATOR_TARGET_HEALTH fl
            WHERE
                {time_series_filter}
                leading_indicator_value_timestamp_utc <= '{date_time_end}'
                AND leading_indicator_inside_envelope_last24h_value IS NOT NULL
            ORDER BY
                leading_indicator_value_timestamp_utc desc) as leading_indicator_inside_envelope_last24h_value,
                CASE
                    WHEN EXISTS (
                        SELECT
                            det.equipment_tag_id
                        FROM
                            DIM_EQUIPMENT_TAG det
                        WHERE
                            det.leading_indicator_id = l.leading_indicator_id
                    ) THEN 'true'
                    ELSE 'false'
                END as has_equipment_tags,
            pi_vision_display_url,
            l.max_{preferred_uom},
            l.min_{preferred_uom}
            FROM
                {snowflake_db_and_schema}.DIM_LEADING_INDICATOR l
            {li_where_query}"""

        dim_payload = self.wrapper.validate_and_execute(li_dim_query)

        return dim_payload[0]

    def perform_utc_conversion(self, local_datetime):
        # Snowflake servers are in Pacific timezone thus CDP_DATETIME_UPDATED is in US/Pacific time.
        utc_datetime_updated = helpers.convert_local_to_utc(helpers.get_datetime_obj(local_datetime), 'US/Pacific')
        return utc_datetime_updated

    def get_snowflake_leading_indicator_summary(self, mtpm_list, datetimestart, datetimeend, preferred_uom, top):
        start_date = helpers.get_datetime_obj(datetimestart)
        end_date = helpers.get_datetime_obj(datetimeend)
        mtpm_list_enum = f"""{",".join([f"'{str(elem)}'" for i, elem in enumerate(mtpm_list)])}"""
        snowflake_db_and_schema = f'{os.getenv("SNOWFLAKE_DATABASE")}.{os.getenv("SNOWFLAKE_SCHEMA")}'
        dim_query = self.get_li_summary_dim_query(mtpm_list_enum, preferred_uom, snowflake_db_and_schema, datetimestart, datetimeend)
        time_query = self.get_li_summary_query(mtpm_list_enum, start_date, end_date, snowflake_db_and_schema)
        dim_data = self.execute_dim_query(dim_query, top)
        time_data = self.execute_summary_query(time_query, dim_data, top)
        return time_data
        

    def execute_dim_query(self, query, top):
        dim_results = self.wrapper.validate_and_execute(query)
        if not top:
            return dim_results
        dim_data = {}
        for result in dim_results:
            dim_data[result[0]] = {
                "leading_indicator_id": result[0],
                "leading_indicator_display_name": result[1],
                "max_target_value": self.convert_to_decimal(result[2]),
                "min_target_value": self.convert_to_decimal(result[3]),
                "uom": result[4],
                "uom_inside_envelope": result[5],
                "mtpm_id": result[6],
                "mtpm_display_name": result[7],
                "last_refresh_timestamp": self.perform_utc_conversion(result[8]),
                "value": self.convert_to_decimal(result[9])
            }
        return dim_data

    def execute_summary_query(self, query, dim_data, top):
        summary_results = self.wrapper.validate_and_execute(query)
        
        records = []

        # If we've received a 'top' value, we'll have the dim data already in a dictionary for us
        # and we want the order of the results determined by the target val query. 
        # Otherwise, we want to put the dim data into a dict and combine with the summary data.
        if top:
            # if total size of list happens to be less than the top value, make that value be the lookup value in the range
            if len(summary_results) < top:
                top = len(summary_results)
            
            # remove nulls
            summary_results = filter(lambda x: x[1] is not None and x[2] is not None and x[3] is not None and x[4] is not None, summary_results)

            # sort by name in addition to values
            summary_results = sorted(summary_results, key=lambda x:(self.convert_to_decimal(x[1]),self.convert_to_decimal(x[2]),
                                                                    self.convert_to_decimal(x[3]),self.convert_to_decimal(x[4]),x[5].lower() if x[5] else ''))

            for i in range(int(top)):
                # TODO: Changed | concatenation since we're not in Python 3.9 yet
                dim_dict = dict(dim_data[summary_results[i][0]])
                dim_dict.update({
                    "l24h": self.convert_to_decimal(summary_results[i][1]),
                    "l12h": self.convert_to_decimal(summary_results[i][2]),
                    "l8h": self.convert_to_decimal(summary_results[i][3]),
                    "l1h": self.convert_to_decimal(summary_results[i][4]),
                })
                records.append(dim_dict)
        else:
            summary_dict = {}
            for res in summary_results:
                summary_dict[res[0]] = {
                    "l24h": self.convert_to_decimal(res[1]),
                    "l12h": self.convert_to_decimal(res[2]),
                    "l8h": self.convert_to_decimal(res[3]),
                    "l1h": self.convert_to_decimal(res[4]),
                }         
            for result in dim_data:
                dim_dict = dict({
                        "leading_indicator_id": result[0],
                        "leading_indicator_display_name": result[1],
                        "max_target_value": self.convert_to_decimal(result[2]),
                        "min_target_value": self.convert_to_decimal(result[3]),
                        "uom": result[4],
                        "uom_inside_envelope": result[5],
                        "mtpm_id": result[6],
                        "mtpm_display_name": result[7],
                        "last_refresh_timestamp": self.perform_utc_conversion(result[8]),
                        "value": self.convert_to_decimal(result[9])})
                dim_dict.update(summary_dict[result[0]] if result[0] in summary_dict else {"l24h": None,"l12h": None, "l8h": None,"l1h": None})
                records.append(dim_dict)
        return records
    
    def get_li_summary_query(self, mtpm_list, datetimestart, datetimeend, snowflake_db_and_schema):
        summary_query = f"""
                        SELECT
                            di.leading_indicator_id,
                            lih24h.leading_indicator_inside_envelope_last24h_value p_24h,
                            lih12h.leading_indicator_inside_envelope_last12h_value p_12h,
                            lih8h.leading_indicator_inside_envelope_last8h_value p_8h,
                            lih1h.leading_indicator_inside_envelope_last1h_value p_1h,
                            di.leading_indicator_display_name
                                FROM {snowflake_db_and_schema}."DIM_LEADING_INDICATOR" di
                                LEFT JOIN (SELECT leading_indicator_id, leading_indicator_inside_envelope_last1h_value, leading_indicator_value_timestamp_utc, ROW_NUMBER() OVER (PARTITION BY leading_indicator_id ORDER BY leading_indicator_value_timestamp_utc DESC) row_id
                                FROM {snowflake_db_and_schema}."FACT_LEADING_INDICATOR_TARGET_HEALTH"
                                WHERE leading_indicator_value_timestamp_utc BETWEEN '{datetimestart}' AND '{datetimeend}'
                                AND leading_indicator_inside_envelope_last1h_value IS NOT NULL) lih1h
                                ON lih1h.leading_indicator_id = di.leading_indicator_id AND lih1h.row_id = 1
                                LEFT JOIN (SELECT leading_indicator_id, leading_indicator_inside_envelope_last8h_value, leading_indicator_value_timestamp_utc, ROW_NUMBER() OVER (PARTITION BY leading_indicator_id ORDER BY leading_indicator_value_timestamp_utc DESC) row_id
                                FROM {snowflake_db_and_schema}."FACT_LEADING_INDICATOR_TARGET_HEALTH"
                                WHERE leading_indicator_value_timestamp_utc BETWEEN '{datetimestart}' AND '{datetimeend}' 
                                AND leading_indicator_inside_envelope_last8h_value IS NOT NULL) lih8h
                                ON lih8h.leading_indicator_id = di.leading_indicator_id AND lih8h.row_id = 1
                                LEFT JOIN (SELECT leading_indicator_id, leading_indicator_inside_envelope_last12h_value, leading_indicator_value_timestamp_utc, ROW_NUMBER() OVER (PARTITION BY leading_indicator_id ORDER BY leading_indicator_value_timestamp_utc DESC) row_id
                                FROM {snowflake_db_and_schema}."FACT_LEADING_INDICATOR_TARGET_HEALTH"
                                WHERE leading_indicator_value_timestamp_utc BETWEEN '{datetimestart}' AND '{datetimeend}'
                                AND leading_indicator_inside_envelope_last12h_value IS NOT NULL) lih12h
                                ON lih12h.leading_indicator_id = di.leading_indicator_id AND lih12h.row_id = 1
                                LEFT JOIN (SELECT leading_indicator_id, leading_indicator_inside_envelope_last24h_value, leading_indicator_value_timestamp_utc, ROW_NUMBER() OVER (PARTITION BY leading_indicator_id ORDER BY leading_indicator_value_timestamp_utc DESC) row_id
                                FROM {snowflake_db_and_schema}."FACT_LEADING_INDICATOR_TARGET_HEALTH"
                                WHERE leading_indicator_value_timestamp_utc BETWEEN '{datetimestart}' AND '{datetimeend}'
                                AND leading_indicator_inside_envelope_last24h_value IS NOT NULL) lih24h
                                ON lih24h.leading_indicator_id = di.leading_indicator_id AND lih24h.row_id = 1
                                WHERE di.leading_indicator_id in (
                                    SELECT l.leading_indicator_id 
                                    FROM {snowflake_db_and_schema}."DIM_LEADING_INDICATOR" l 
                                    INNER JOIN {snowflake_db_and_schema}."DIM_MTPM" m ON m.mtpm_id = l.mtpm_id 
                                    WHERE l.mtpm_id IN ({mtpm_list}))
                                ORDER BY p_24h, p_12h, p_8h, p_1h ASC
                        """
        return summary_query

    def get_li_summary_dim_query(self, mtpm_list, preferred_uom, snowflake_db_and_schema, datetimestart, datetimeend):

        if preferred_uom is None or preferred_uom not in ('metric', 'imperial'):
            preferred_uom = 'metric'

        dim_query = f"""WITH cte_recent_vals AS (
                        SELECT fl.leading_indicator_id, fl.leading_indicator_value_{preferred_uom}
                        FROM {snowflake_db_and_schema}."FACT_LEADING_INDICATOR" fl
                        INNER JOIN (SELECT leading_indicator_id, max(leading_indicator_value_timestamp_utc) AS dtc 
                            FROM {snowflake_db_and_schema}."FACT_LEADING_INDICATOR" 
                            WHERE leading_indicator_value_timestamp_utc <= '{datetimeend}'
                            GROUP BY leading_indicator_id) fli 
                        ON fli.leading_indicator_id = fl.leading_indicator_id and fl.leading_indicator_value_timestamp_utc = dtc
                        WHERE fl.leading_indicator_id in 
                            (SELECT l.leading_indicator_id from DIM_LEADING_INDICATOR l 
                                INNER JOIN DIM_MTPM m ON m.mtpm_id = l.mtpm_id 
                                WHERE l.mtpm_id IN ({mtpm_list})
                            )
                        ),
                        cte_timestamp_vals AS (
                        SELECT fl.leading_indicator_id, fl.cdp_datetime_updated
                        FROM {snowflake_db_and_schema}."FACT_LEADING_INDICATOR" fl
                        INNER JOIN (SELECT leading_indicator_id, max(leading_indicator_value_timestamp_utc) AS dtc 
                            FROM {snowflake_db_and_schema}."FACT_LEADING_INDICATOR" 
                            GROUP BY leading_indicator_id) fli 
                        ON fli.leading_indicator_id = fl.leading_indicator_id and fl.leading_indicator_value_timestamp_utc = dtc
                        WHERE fl.leading_indicator_id in 
                            (SELECT l.leading_indicator_id from DIM_LEADING_INDICATOR l 
                                INNER JOIN DIM_MTPM m ON m.mtpm_id = l.mtpm_id 
                                WHERE l.mtpm_id IN ({mtpm_list}))
                            ) 
                        SELECT l.leading_indicator_id,
                        l.leading_indicator_display_name,
                        l.max_{preferred_uom},
                        l.min_{preferred_uom},
                        l.uom_{preferred_uom},
                        l.uom_inside_envelope,
                        m.mtpm_id,
                        m.mtpm_display_name,
                        ts.cdp_datetime_updated,
                        rv.leading_indicator_value_{preferred_uom}
                    FROM
                        {snowflake_db_and_schema}."DIM_LEADING_INDICATOR" l
                    INNER JOIN {snowflake_db_and_schema}."DIM_MTPM" m 
                    ON m.mtpm_id = l.mtpm_id
                    LEFT JOIN cte_recent_vals rv on rv.leading_indicator_id = l.leading_indicator_id
                    LEFT JOIN cte_timestamp_vals ts on ts.leading_indicator_id = l.leading_indicator_id
                    WHERE l.mtpm_id in ({mtpm_list})
                    ORDER BY mtpm_display_name, leading_indicator_display_name"""
        return dim_query