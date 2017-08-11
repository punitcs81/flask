import os
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import DatabaseError
from collections import defaultdict
import json

from analytics_backend import db
import analytics_backend.utils.date_time as dt
from analytics_backend.models.reference import ParamsAppModel, ConsumerTagsModel, InteractionTypeModel, DateModel
from config import config
import constants

# need to remove this config part
config_name = os.environ.get('PA_CONFIG', 'development')
current_config = config[config_name]


def consumer_summary(business_account_id: int, store_ids: list, date_param_cd=constants.REPORT_DATE_PARAM_6_MONTH):
    date_range = ParamsAppModel.get_date_range(business_account_id, date_param_cd)

    data = __consumer_tags_summary(date_range['report_date'], date_range['lower_bound'], date_range['upper_bound'],
                                   business_account_id)

    if data is None:
        return pd.DataFrame({})

    if store_ids is not None:
        data = data.filter()  # add filter for store id

    # get total count, new customers, churned customers,

    return True


def consumer_activites(business_account_id: int, date_param, report_type, tags: list = [], kpis: list = []):
    # logic for tags and kpi selector will be added later
    date_range = ParamsAppModel.get_date_range(business_account_id, date_param)
    granularity = date_range['ts_granularity']

    if report_type == 'timeseries':
        if granularity == 'DAILY':
            data = __consumer_activites_timeseries_daily(date_range['report_date'], date_range['lower_bound'],
                                                         date_range['upper_bound'], business_account_id)
        elif granularity == 'WEEKLY':
            data = __consumer_activites_timeseries_weekly(date_range['report_date'], date_range['lower_bound'],
                                                          date_range['upper_bound'], business_account_id)
        else:
            raise ValueError("Wrong Granularity is attached to %s param" % date_param)
    else:
        raise ValueError("report type value (%s) is wrong" % report_type)

    if data.empty is True:
        return data
    return (data)


# good 29-june
def consumer_segment_summary(business_account_id, date_param_cd, store_ids: list):
    if date_param_cd not in [r for r, in db.session.query(ParamsAppModel.param_name_cd.distinct()).all()]:
        raise ValueError("Wrong date_param_cd")
    date_range = ParamsAppModel.get_date_range(business_account_id, date_param_cd)

    if date_range:
        lower_bound = date_range['lower_bound']
        upper_bound =date_range['upper_bound']
        prev_lower_bound = date_range['prev_lower_bound']
        prev_upper_bound = date_range['prev_upper_bound']
        report_date = date_range['report_date']
    else:
        raise DatabaseError(
            "ParamAppModel returns no corresponding date range for business_account_id=%s and date param"
            % (business_account_id, date_param_cd))
    summary_df = __consumer_tags_summary(report_date, lower_bound, upper_bound, business_account_id)
    print(summary_df)
    prev_summary_df = __consumer_tags_summary(report_date, prev_lower_bound, prev_upper_bound, business_account_id)

    if store_ids:
        summary_df = summary_df[summary_df['attributed_store_id'].isin(store_ids)]
        print(summary_df)
        prev_summary_df = prev_summary_df[prev_summary_df['attributed_store_id'].isin(store_ids)]
        print(prev_summary_df)
    key_columns = ['gender', 'recency', 'age_group', 'frequency', 'monetary', 'clv_1year_level', 'clv_3year_level',
                   'badge', 'attributed_store_id']
    comb_summary_df = pd.merge(summary_df, prev_summary_df, how='left', on=key_columns, suffixes=['_curr', '_prev'])
    print(comb_summary_df)
    comb_summary_df.fillna(0, inplace=True)
    return comb_summary_df


# good  -29June

def consumer_segment_counts_trends(business_account_id, date_param_cd, store_ids: list, aggregate_level='daily'):
    db_date_format = constants.REPORT_DB_DATE_FORMAT
    if date_param_cd not in [r for r, in db.session.query(ParamsAppModel.param_name_cd.distinct()).all()]:
        raise ValueError("Wrong date_param_cd")
    date_range = ParamsAppModel.get_date_range(business_account_id, date_param_cd)

    if aggregate_level is None:
        aggregate_level_dict = constants.API_DATE_AGGREGATION_LEVEL
        aggregate_level = aggregate_level_dict.get(date_param_cd, 'daily')

    if date_range:
        lower_bound = date_range['lower_bound']
        upper_bound = date_range['upper_bound']
        prev_lower_bound = date_range['prev_lower_bound']
        prev_upper_bound = date_range['prev_upper_bound']
        report_date = date_range['report_date']
    else:
        raise DatabaseError(
            "ParamAppModel returns no corresponding date range for business_account_id=%s and date param"
            % (business_account_id, date_param_cd))
    summary_df = __consumer_tags_counts_trends_dly(report_date, lower_bound, upper_bound, business_account_id,
                                                   db_date_format)
    if summary_df is None:
        return pd.DataFrame({})
    summary_df['date'] = pd.to_datetime(summary_df['date'], format="%Y-%m-%d")

    if store_ids:
        summary_df = summary_df[summary_df['attributed_store_id'].isin(store_ids)]


    # Potential function if there are a lot of trends APIs on date

    all_date_df = pd.DataFrame(DateModel.get_days_between_dates(lower_bound, upper_bound))
    joined_summary_df = pd.merge(all_date_df, summary_df, how='left', left_on='full_date', right_on='date')
    joined_summary_df.fillna(0, inplace=True)
    joined_summary_df['full_date'] = joined_summary_df['full_date'].dt.strftime(db_date_format)
    print(joined_summary_df)


    if aggregate_level == 'daily':
        joined_summary_df.drop(['date', 'attributed_store_id', 'Monday_Date_Of_Week', 'day_of_month'], axis=1,
                               inplace=True)
        joined_summary_df.rename(columns={'full_date': 'date'}, inplace=True)
        final_df = joined_summary_df.groupby('date').sum()
        print(final_df)
    elif aggregate_level == 'weekly':
        joined_summary_df.drop(['date', 'attributed_store_id', 'full_date', 'day_of_month', 'day_of_month'], axis=1,
                               inplace=True)
        joined_summary_df.rename(columns={'Monday_Date_Of_Week': 'date'}, inplace=True)
        final_df = joined_summary_df.groupby('date').sum()
        print(final_df)
    else:
        raise ValueError("aggregate level is wrong : %s" % aggregate_level)
    # final_df.set_index('date',inplace=True) #not required as groupby set the index already

    return final_df


##Loyalty Related
def consumer_loyalty_interaction_summary(business_account_id, date_param_cd, store_ids: list):
    global nested, nested
    if date_param_cd not in [r for r, in db.session.query(ParamsAppModel.param_name_cd.distinct()).all()]:
        raise ValueError("Wrong date_param_cd")
    date_range = ParamsAppModel.get_date_range(business_account_id, date_param_cd)

    if date_range:
        lower_bound = date_range['lower_bound']
        upper_bound = date_range['upper_bound']
        prev_lower_bound = date_range['prev_lower_bound']
        prev_upper_bound = date_range['prev_upper_bound']
        report_date = date_range['report_date']
    else:
        raise DatabaseError(
            "ParamAppModel returns no corresponding date range for business_account_id=%s and date param"
            % (business_account_id, date_param_cd))

    interaction_summary_df = _consumer_loyalty_interactions_summary(report_date, lower_bound, upper_bound,
                                                                    business_account_id,
                                                                    constants.REPORT_DB_DATE_FORMAT)
    prev_interaction_summary_df = _consumer_loyalty_interactions_summary(report_date, prev_lower_bound,
                                                                         prev_upper_bound,
                                                                         business_account_id,
                                                                         constants.REPORT_DB_DATE_FORMAT)
    if store_ids and (interaction_summary_df.empty is False):
        interaction_summary_df = interaction_summary_df[interaction_summary_df['store_id'].isin(store_ids)]
    if store_ids and (prev_interaction_summary_df.empty is False):
        prev_interaction_summary_df = prev_interaction_summary_df[
            prev_interaction_summary_df['store_id'].isin(store_ids)]
    key_columns = ['store_id', 'interaction_type', 'loyalty_currency', 'currency', 'time_zone',
                   'interaction_type_category']

    if interaction_summary_df.empty is False and prev_interaction_summary_df.empty is False:
        comb_interaction_df = pd.merge(interaction_summary_df, prev_interaction_summary_df, how='left', on=key_columns,
                                       suffixes=['', '_prev'])
    elif interaction_summary_df.empty is False and prev_interaction_summary_df.empty is True:
        comb_interaction_df = interaction_summary_df
        comb_interaction_df['total_value_prev'] = 0
        comb_interaction_df['number_of_events_prev'] = 0
        comb_interaction_df['distinct_consumer_events_prev'] = 0
    else:
        comb_interaction_df = pd.DataFrame(None)
    comb_interaction_df.fillna(0, inplace=True)
    comb_interaction_df.drop(['store_id', 'currency', 'time_zone'], axis=1, inplace=True)
    # comb_interaction_df.groupby(by=['loyalty_currency','interaction_type_category','interaction_type']).sum()
    # comb_interaction_df.set_index(['loyalty_currency','interaction_type_category','interaction_type'],inplace=True)
    # print(comb_interaction_df.index)

    # results = defaultdict(lambda: defaultdict(dict))
    # for row in comb_interaction_df.itertuples():
    #     print(row)
    #     for i, key in enumerate(row.Index):
    #         print(i,key)
    #         if i == 0:
    #             nested = results[key]
    #         elif i == len(row.Index) - 1:
    #             nested [key] = row
    #         else:
    #             nested = nested[key]
    #         print(nested)

    # result=(comb_interaction_df.groupby(by=['loyalty_currency','interaction_type'],as_index=False).sum()\
    #     .apply(lambda x: x [['total_value','total_value_prev','number_of_events','number_of_events_prev','distinct_consumer_events','distinct_consumer_events_prev']])\
    #     .rename(columns={0:"some_col"}).to_json(orient='index'))
    #
    # print('Hey\n',json.dumps(json.loads(result),indent=2,sort_keys=True))

    # json_dict = {}
    # json_dict['group_list'] = []
    # for grp, grp_data in df.groupby('Group'):
    #     grp_dict = {}
    #     grp_dict['group'] = grp
    #     for cat, cat_data in grp_data.groupby('Category'):
    #         grp_dict['category_list'] = []
    #         cat_dict = {}
    #         cat_dict['category'] = cat
    #         cat_dict['p_list'] = []
    #         for p, p_data in cat_data.groupby('P'):
    #             p_data = p_data.drop(['Category', 'Group'], axis=1).set_index('P')
    #             for d in p_data.to_dict(orient='records'):
    #                 cat_dict['p_list'].append({'p': p, 'date_list': [d]})
    #         grp_dict['category_list'].append(cat_dict)
    #     json_dict['group_list'].append(grp_dict)
    # json_out = dumps(json_dict)
    # parsed = json.loads(json_out)
    return comb_interaction_df

def consumer_visit(business_account_id, store_ids, date_param_cd):

        if date_param_cd not in [r for r, in db.session.query(ParamsAppModel.param_name_cd.distinct()).all()]:
            raise ValueError("Wrong date_param_cd")
        date_range = ParamsAppModel.get_date_range(business_account_id, date_param_cd)

        if date_range:
            lower_bound = date_range['lower_bound']
            upper_bound = date_range['upper_bound']
            prev_lower_bound = date_range['prev_lower_bound']
            prev_upper_bound = date_range['prev_upper_bound']
            report_date = date_range['report_date']
            print(lower_bound)
            print(upper_bound)
        else:
            raise DatabaseError(
                "ParamAppModel returns no corresponding date range for business_account_id=%s and date param"
                % (business_account_id, date_param_cd))


        output = __consumer_visit(lower_bound, upper_bound, business_account_id, store_ids)

        #return {"message": "Please use POST method to filter with tag_names"},404
        return output


########################################################################################################################
# Protected methods
########################################################################################################################
##good
def __consumer_tags_summary(report_date, from_date, to_date, business_account_id: int):
    # Dates are converted to MySQL DB date (as per config param). This is to avoid sending %d kind of string
    # for SQLAlchemy to handle
    print(to_date,from_date)
    if not dt.validate_date_format(report_date, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % report_date,
                         current_config.REPORT_DB_DATE_FORMAT)
    if not dt.validate_date_format(from_date, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % report_date,
                         current_config.REPORT_DB_DATE_FORMAT)
    if not dt.validate_date_format(to_date, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % report_date,
                         current_config.REPORT_DB_DATE_FORMAT)
    arguments = locals()

    sql = """select gender, recency,coalesce(age_group,'UNDEFINED') age_group, frequency, monetary, clv_1year_level,clv_3year_level,
                   badge , attributed_store_id, 
                   count(distinct(consumer_id)) count,
                   SUM(
                        CASE 
                        WHEN (registered_on BETWEEN \'{from_date}\' AND \'{to_date}\') THEN 1 
                        else 0 
                        end ) new_consumer_count
                   from pika_dm.dim_business_consumer
                   where business_account_id = {business_account_id}
 				  and (\'{report_date}\' between start_date AND end_date )
 				  and (registered_on between \'{from_date}\' and \'{to_date}\') 
                    group by gender, recency,age_group, frequency, monetary, clv_1year_level,clv_3year_level,
               badge,attributed_store_id"""

    formated_sql = sql.format(**arguments)
    print(formated_sql)
    try:
        results = db.engine.execute(formated_sql, ())
        result_set = results.fetchall()
    except Exception as e:
        raise e

    df = pd.DataFrame(result_set)
    if df.empty is True:
        return None
    df.columns = results.keys()

    return df


# Good
def __consumer_tags_counts_trends_dly(report_date, from_date, to_date, business_account_id: int, db_date_format):
    # Dates are converted to MySQL DB date (as per config param). This is to avoid sending %d kind of string
    # for SQLAlchemy to handle

    if not dt.validate_date_format(report_date, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % report_date,
                         current_config.REPORT_DB_DATE_FORMAT)
    if not dt.validate_date_format(from_date, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % report_date,
                         current_config.REPORT_DB_DATE_FORMAT)
    if not dt.validate_date_format(to_date, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % report_date,
                         current_config.REPORT_DB_DATE_FORMAT)
    arguments = locals()

    # sql = """select gender, recency,coalesce(age_group,'UNDEFINED') age_group,
    #                 DATE_FORMAT(registered_on,\'%Y-%m-%d\') registered_on
    #                 , frequency, monetary, clv_1year_level,clv_3year_level,
    #                badge , attributed_store_id,
    #                count(distinct(consumer_id)) total_consumer_count,
    #                SUM(
    #                     CASE
    #                     WHEN (registered_on BETWEEN \'{from_date}\' AND \'{to_date}\') THEN 1
    #                     else 0
    #                     end ) new_consumer_count
    #                from dim_business_consumer
    #                where business_account_id = {business_account_id}
    # 			  and (\'{report_date}\' between sys_start_date AND sys_end_date )
    # 			  and (registered_on between \'{from_date}\' and \'{to_date}\')
    #                 group by gender, recency,age_group, registered_on, frequency, monetary, clv_1year_level,
    #                 clv_3year_level,badge,attributed_store_id"""
    #

    sql = """ select 
            store_id attributed_store_id,
            DATE_FORMAT(interaction_date,\'%Y-%m-%d\') 'date', 
            interaction_checkin_count checkin, 
            interaction_purchase_count purchase, 
            interaction_registration_count registration,
            consumer_new_count new_consumers,
            consumer_total_count existing_consumers,
            consumer_platinum_count platinum,
            consumer_high_risk_count high_risk,
            interaction_redeem_count redemption,
            time_zone
            from 
            pika_dm.agg_buss_store_stats_daily
            where business_account_id = {business_account_id}
            and (interaction_date between '2014-05-24' and '2014-12-31') """

    formated_sql = sql.format(**arguments)
    print(formated_sql)
    try:
        results = db.engine.execute(text(formated_sql), ())
        result_set = results.fetchall()
    except Exception as e:
        raise e

    df = pd.DataFrame(result_set)
    if df.empty is True:
        return None
    df.columns = results.keys()
    return df


def _consumer_loyalty_interactions_summary(report_date, from_date, to_date, business_account_id: int, db_date_format):
    # Dates are converted to MySQL DB date (as per config param). This is to avoid sending %d kind of string
    # for SQLAlchemy to handle

    if not dt.validate_date_format(report_date, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % report_date,
                         current_config.REPORT_DB_DATE_FORMAT)
    if not dt.validate_date_format(from_date, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % report_date,
                         current_config.REPORT_DB_DATE_FORMAT)
    if not dt.validate_date_format(to_date, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % report_date,
                         current_config.REPORT_DB_DATE_FORMAT)
    arguments = locals()

    sql = """select 
          store_id 
          ,interaction_type
          ,loyalty_currency 
          ,currency
          ,time_zone
          ,interaction_type_category
          ,sum(currency_value) total_value
          ,count( *) number_of_events 
          , count(distinct business_consumer_id ) distinct_consumer_events
        from pika_dm.rt_consumer_events
        where business_account_id ={business_account_id}
        and interaction_date between \'{from_date}\' and \'{to_date}\'
        
        group by
          store_id 
          ,interaction_type
          ,loyalty_currency
          ,currency
          ,time_zone
          ,interaction_type_category
          """

    formated_sql = sql.format(**arguments)
    print(formated_sql)
    try:
        results = db.engine.execute(text(formated_sql), ())
        result_set = results.fetchall()
    except Exception as e:
        raise e

    df = pd.DataFrame(result_set)
    if df.empty is True:
        return pd.DataFrame(None)
    df.columns = results.keys()
    return df


def __consumer_activites_summary(report_date, from_date, to_date, business_account_id: int):
    pass


def __consumer_activites_timeseries_daily(report_date, from_date, to_date, business_account_id: int,
                                          granularity='daily'):
    if not dt.validate_date_format(report_date, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % report_date,
                         current_config.REPORT_DB_DATE_FORMAT)
    if not dt.validate_date_format(from_date, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % report_date,
                         current_config.REPORT_DB_DATE_FORMAT)
    if not dt.validate_date_format(to_date, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % report_date,
                         current_config.REPORT_DB_DATE_FORMAT)
    arguments = locals()

    # revenue, points_earned, visits(unique member counts)
    sql = """select 
                  BC.gender,
                  BC.recency,
                  BC.frequency,
                  BC.monetary,
                  BC.clv_1year_level,
                  BC.clv_3year_level,
                  CE.interaction_type,
                  CE.interaction_type_category,
                  SUM(BC.lifetime_spent_type1) total_lifetime_spent,
                  AVG(BC.avg_spent_per_trnx_type1) avg_spent_per_transaction,
                  COUNT(distinct CE.consumer_id) consumer_count

                  from 
                       rt_consumer_events CE
                       inner join
                       dim_business_consumer BC
                       on CE.business_consumer_s_key =BC.s_key

                  where CE.business_account_id=14
                        and CE.store_id =63
                        and CE.interaction_date BETWEEN '2013-01-01' AND '2018-12-31'
                  group by 
                  BC.gender,
                  BC.recency,
                  BC.frequency,
                  BC.monetary,
                  BC.clv_1year_level,
                  BC.clv_3year_level,
                  CE.interaction_type,
                  CE.interaction_type_category   
              """
    formated_sql = sql.format(**arguments)

    # get the left table for series
    all_date_df = pd.DataFrame(DateModel.get_days_between_dates(from_date, to_date))

    # get all tags - series in this case
    all_tags_df = pd.DataFrame(ConsumerTagsModel.get_tags())

    # all KPI list
    all_kpi_df = pd.DataFrame(InteractionTypeModel.get_all_combination())

    # get real data
    try:
        results = db.engine.execute(formated_sql, ())
        result_set = results.fetchall()
    except Exception as e:
        raise e

    data_df = pd.DataFrame(result_set)

    all_tags_data = pd.merge()


def __consumer_activites_timeseries_weekly(report_date, from_date, to_date, business_account_id: int,
                                           granularity='daily'):
    try:
        results = db.engine.execute(formated_sql, ())
        result_set = results.fetchall()
    except Exception as e:
        raise e

    df = pd.DataFrame(result_set)
    if df.empty is False:
        df.columns = results.keys()
    return df
    pass






#
# def __consumer_growth(from_date, to_date, business_account_id, store_ids):
#     #return {"message": "Please use POST method to filter with tag_names inside"}, 404
#     arguments = locals()
#
#     print( from_date, to_date, business_account_id,  store_ids)
#     campaign_dict = dict()
#
#     campaign_dict['api_name'] = 'consumer growth'
#
#
#
#     report_duration_dict = dict()
#
#     start_date = from_date
#     end_date = to_date
#     report_duration_dict['start_date'] = start_date
#     report_duration_dict['end_date'] = end_date
#
#     campaign_dict['report_duration'] = report_duration_dict
#     campaign_dict['campaign_ids'] = campaign_id
#
#     sub_json = {}
#     sub_list = []
#
#     sql = """select t1.response_date,t1.response,sum(t1.consumer_count) as count
#                from pika_dm.agg_campaign_performance_daily t1
#                join pika_dm.rel_campaign_business_store t2
#                on t1.campaign_id  = t2.campaign_id
#                and t1.business_account_id = t2.business_account_id
#                and t1.store_id = t2.store_id
#                where t1.business_account_id = {business_account_id}
#                and response in ('CONVERTED','DELIVERED')"""
#
#     if store_ids is None:
#         a = 'and t2.campaign_id in (' + ','.join(map(str, campaign_id)) + ') group by response_date,response'
#
#         s = sql + a
#
#     else:
#
#         # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
#         s = sql + 'and t2.campaign_id in (' + ','.join(map(str, campaign_id)) + ') and t1.store_id in (' + ','.join(
#             map(str, store_ids)) + ') group by  response_date,response'  # %(store_ids)
#
#     formatted_sql = s.format(**arguments)
#     print(formatted_sql)
#
#     try:
#         results = db.engine.execute(formatted_sql)
#         result_set = results.fetchall()
#         print(result_set)
#     except Exception as e:
#         raise e
#
#     # for data in result_set:
#     #     sub_json = {'date':str(data['response_date']), 'response': str(data['response']), 'count': str(data['count'])}
#     #     print(sub_json)
#     #     sub_list.append(sub_json.copy())
#     #     print(sub_list)
#
#     converted_dict = {}
#     delivered_dict = {}
#     converted_list = []
#     delivered_list = []
#     for data in result_set:
#         print(data)
#
#         if data['response'] == 'CONVERTED':
#             converted_dict = {'date': str(data['response_date']), 'count': str(data['count'])}
#             converted_list.append(converted_dict.copy())
#             print(converted_list)
#
#         else:
#             delivered_dict = {'date': str(data['response_date']), 'count': str(data['count'])}
#             delivered_list.append(delivered_dict.copy())
#             print(delivered_list)
#
#     print(converted_list)
#     print()
#     print(delivered_list)
#
#     campaign_dict['data'] = {'CONVERTED': converted_list, 'DELIVERED': delivered_list}
#
#     output_result = json.dumps(campaign_dict)
#     loaded_json = json.loads(output_result)
#
#     return loaded_json


def __consumer_visit(from_date, to_date, business_account_id, store_ids):

    #return {"message": "Please use POST method to filter with tag_names inside"}, 404
        arguments = locals()

        print( from_date, to_date, business_account_id,  store_ids)
        campaign_dict = dict()

        campaign_dict['api_name'] = 'consumer visit'



        report_duration_dict = dict()

        start_date = from_date
        end_date = to_date
        report_duration_dict['start_date'] = start_date
        report_duration_dict['end_date'] = end_date

        campaign_dict['report_duration'] = report_duration_dict

        sub_json = {}
        sub_list = []

        sql = """select count( t1.business_consumer_id) as count,t2.badge as badge,t1.interaction_date 
                from
                pika_dm.rt_consumer_events t1
                join pika_dm.dim_business_consumer t2
                on t1.business_consumer_id = t2.consumer_id
                join pika_dm.dim_business_store_consumer t3
                on t1.store_id = t3.store_id
                and t1.business_consumer_id = t3.business_consumer_id
                where t1.interaction_date between \'{from_date}\' and \'{to_date}\'
                and t1.business_account_id = {business_account_id}
                and t2.end_date = '9999-12-31 00:00:00'
                and t3.end_date = '9999-12-31 00:00:00'"""

        if store_ids is None:
            a = 'group by t2.badge,t1.interaction_date'

            s = sql + a

        else:

            # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
            s = sql + ' and t1.store_id in (' + ','.join(map(str, store_ids)) + ') group by t2.badge,t1.interaction_date'  # %(store_ids)

        formatted_sql = s.format(**arguments)
        print(formatted_sql)

        try:
            results = db.engine.execute(formatted_sql)
            result_set = results.fetchall()
            print(result_set)
        except Exception as e:
            raise e

        # for data in result_set:
        #     sub_json = {'date':str(data['response_date']), 'response': str(data['response']), 'count': str(data['count'])}
        #     print(sub_json)
        #     sub_list.append(sub_json.copy())
        #     print(sub_list)

        for data in result_set:
            #print(data)
            print(data['interaction_date'])
            sub_json={'bagde':data['badge'],'count':data['count'],'date':data['interaction_date']}
            sub_list.append(sub_json.copy())
            #print(sub_list)
        campaign_dict['data'] =sub_list

        output_result = json.dumps(campaign_dict)
        loaded_json = json.loads(output_result)

        return loaded_json