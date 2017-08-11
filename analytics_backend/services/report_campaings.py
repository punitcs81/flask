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
import logging

logger = logging.getLogger(__name__)
# need to remove this config part
config_name = os.environ.get('PA_CONFIG', 'development')
current_config = config[config_name]


def campaign_summary(business_account_id, store_ids, date_param_cd):

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

    # feedback_df  = __feedback_tags_summary(date_range['lower_bound'], date_range['upper_bound'], business_account_id)

    # feedback_df1 = __feedback_till_date(business_account_id)

    # feedback_df2 = __campaign_till_date(business_account_id)

    logger.info('calling campaign summary protected method')
    campaign_json = __campaign_summary(lower_bound, upper_bound, business_account_id, store_ids)

    # 1st block- data param
    # get data from database (one / many cals - you will get one/many data frames
    # (optional) join and transform all data as necessary - may be 1+ df
    # (otional) where you will construct dict for complex json structure
    #       - use to_json   - manual dict construction (specific rules)


    # key_columns = ['business_account_id']
    # comb_summary_df = pd.merge(feedback_df, feedback_df1, how='right', on=key_columns)
    #
    # j = (
    # comb_summary_df.groupby(['business_account_id','active_campaign'], as_index=False)
    #     .apply(lambda x: x[['channel', 'reach']].to_dict('r'))
    #     # .apply(lambda x: x[['sum(feedback_2)', 'rating2']].to_dict('d'))
    #
    #     .reset_index()
    #     .rename(columns={0: 'channel'})
    #
    #     # .rename(columns={1: 'Detareiled_feedback'})
    #     .to_json(orient='records'))


    return campaign_json

    #return {"message": "there is no get request for the API post inside"}, 404




def RTcampaign_summary(business_account_id, date_param_cd, store_ids):
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

    # rt_channel_df = __rt_summary_channel(report_date, lower_bound, upper_bound, business_account_id)

    # rt_goal_df = __rt_summary_goal(report_date, lower_bound, upper_bound, business_account_id)

    # rt_summary_df = __rt_summary(report_date, lower_bound, upper_bound, business_account_id)

    rt_summary = __rt_campaign_summary(report_date, lower_bound, upper_bound, business_account_id,
                                       date_param_cd, store_ids)

    # def dtj(df):
    #     drec = dict()
    #     ncols = df.values.shape[1]
    #     for line in df.values:
    #         d = drec
    #         for j, col in enumerate(line[:-1]):
    #             if not col in d.keys():
    #                 if j != ncols - 2:
    #                     d[col] = {}
    #                     d = d[col]
    #                 else:
    #                     d[col] = line[-1]
    #             else:
    #                 if j != ncols - 2:
    #                     d = d[col]
    #     print(type(drec))
    #     return drec
    #
    # rt_channel_json = dtj(rt_channel_df)
    # rt_goal_json = dtj(rt_goal_df)
    #
    #
    # final_dict={}
    # final_dict['channels']=(rt_channel_json)
    # final_dict['goals']=rt_goal_json
    #
    # rt_dict= rt_summary_df.to_json(orient='records')
    # #rt_dict.append(final_dict)
    #
    # print(type(rt_dict))
    # #print(rt_dict)
    #
    # return rt_dict
    #
    # ab=rt_summary.to_dict(orient='records')
    #
    # ab.append(drec)
    # print(ab)


    return rt_summary
    # return {"message": "there is no get request for the API"}, 404


def campaign_trend(business_account_id, store_ids, date_param_cd, campaign_id):
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

    campaign_data = __campaign_trend(report_date, lower_bound, upper_bound, business_account_id, store_ids,
                                     date_param_cd, campaign_id)

    return campaign_data
    # return {"message": "there is no get request for the API"}, 404


def rt_campaign_trend(business_account_id, date_param_cd, campaign_id, store_ids):

    print(business_account_id,date_param_cd,campaign_id,store_ids)
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

    campaign_data = __rt_campaign_trend(report_date, lower_bound, upper_bound, business_account_id,
                                        date_param_cd, campaign_id, store_ids)

    return campaign_data
    #return {"message": "there is no get request for the API"}, 404

def campaign_performance(business_account_id, date_param_cd,  store_ids):

    print(business_account_id,date_param_cd,store_ids)
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

    campaign_data = __campaign_performance(report_date, lower_bound, upper_bound, business_account_id,
                                        date_param_cd, store_ids)

    return campaign_data
################################# PROTECTED METHODS  #####################################

# def __feedback_tags_summary(from_date, to_date, business_account_id: int):
#     arguments = locals()
#
#
#     sql ="""select business_account_id,count(*) as active_campaign
# from pika_dm.dim_campaign
# where sys_end_date='9999-12-31 00:00:00' and business_account_id=14"""
#     #formated_sql = sql.format(**arguments)
#
#     try:
#         results = db.engine.execute(sql)
#         result_set = results.fetchall()
#     except Exception as e:
#         raise e
#
#     df = pd.DataFrame(result_set)
#     if df.empty is True:
#         return None
#     df.columns = results.keys()
#     print(df)
#     return  df
#
#     #
#     # sql = """select channel, response
#     # from pika_dm.rt_consumer_campaign_response
#     # where business_account_id=14  and response_date
#     # between '2017-06-01' and '2017-07-31'
#     # """
#     #
#     # try:
#     #     results = db.engine.execute(sql)
#     #     result_set = results.fetchall()
#     # except Exception as e:
#     #     raise e
#     #
#     # df = pd.DataFrame(result_set)
#     # if df.empty is True:
#     #     return None
#     # df.columns = results.keys()
#     # print(df)
#
#     # a = df.groupby(['channel', 'response']).size()
#     # if a.empty is True:
#     #     return None
#     # a.columns = results.keys()
#
#
#
#
#
# def __feedback_till_date(business_account_id: int):
#     arguments = locals()
#
#     sql = """select business_account_id,channel,count(*) as reach,response
# from pika_dm.rt_consumer_campaign_response
# where business_account_id=14 and channel in ('SMS','NOTIFICATION') and response in ('DELIVERED','CONVERTED') and response_date
# between '2017-06-01' and '2017-07-31'
# group by channel,response"""
#     formated_sql = sql.format(**arguments)
#
#     try:
#         results = db.engine.execute(sql)
#         result_set = results.fetchall()
#     except Exception as e:
#         raise e
#
#     df = pd.DataFrame(result_set)
#     if df.empty is True:
#         return None
#     df.columns = results.keys()
#     print(df)
#     return df
#
#
# def __campaign_till_date(business_account_id: int):
#     arguments = locals()
#
#     sql = """select goal_name,count(t3.response),t3.response
# from pika_dm.dim_campaign t1
# join pika_dm.ref_campaign_goal t2
# on t1.campaign_goal_id=t2.id
# join pika_dm.rt_consumer_campaign_response t3
# on t1.campaign_id=t3.campaign_id
# where sys_end_date='9999-12-31 00:00:00'  and start_date between '2017-06-01'
# and '2017-07-31'
# and t3.response in ('DELIVERED','CONVERTED')
# group by
# t2.goal_name,t3.response"""
#     formated_sql = sql.format(**arguments)
#
#     try:
#         results = db.engine.execute(sql)
#         result_set = results.fetchall()
#     except Exception as e:
#         raise e
#
#     df = pd.DataFrame(result_set)
#     if df.empty is True:
#         return None
#     df.columns = results.keys()
#     print(df)
#     return df
#
#



################################   CAMPAIGN SUMMARY ###################

def __campaign_summary(from_date, to_date, business_account_id, store_ids):

    print(from_date,to_date,business_account_id,store_ids)
    arguments = locals()

    campaign_dict = {}

    ##########################  channel sub json

    channel_dict = {}
    channel_list = []

    sql_string = """select t1.channel,count(*) as count,t1.response
                    from pika_dm.rt_consumer_campaign_response t1
                    join pika_dm.dim_business_account t2
                    on t1.business_account_id=t2. business_account_id 
                    and t1.business_account_s_key = t2.s_key           
                    where t1.business_account_id ={business_account_id}
                    and t2.end_date = '9999-12-31 00:00:00'            
                    and channel in ('SMS','NOTIFICATION') 
                    and response in ('DELIVERED','CONVERTED') 
                    and response_date between \'{from_date}\' and \'{to_date}\'"""

             #group by channel,response"""

    if store_ids is None:
        sql = sql_string + 'group by channel,response'

    else:
        # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
        sql = sql_string + 'and t1.store_id in (' + ','.join(map(str,store_ids)) + ') group by channel,response'  # %(store_ids)


    formatted_sql = sql.format(**arguments)

    print(formatted_sql)
    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
        raise e

    for data in result_set:
        if data['response'] == 'CONVERTED':
            channel_dict["channel"] = data['channel']
            channel_dict["converted"] = data['count']

        elif data['response'] == 'DELIVERED':

            channel_dict['channel'] = data['channel']
            channel_dict['reach'] = data['count']
            channel_list.append(channel_dict.copy())

    campaign_dict['channel'] = channel_list

    ######################  campaign json  - active campaign

    # if store_ids is None:
    #     sql = """select count(*) as active_campaign
    #              from pika_dm.dim_campaign t1
    #              join pika_dm.dim_business_account t2
    #              on t1.business_account_id=t2.business_account_id
    #              and t1.business_account_s_key = t2.s_key
    #              where t1.end_date='9999-12-31 00:00:00'
    #              and t2.end_date='9999-12-31 00:00:00'
    #              and t1.business_account_id={business_account_id}
    #              and t1.start_date between \'{from_date}\' and \'{to_date}\'
    #              and t1.isActive =1
    #              and t2.isActive =1"""

    # else:
    sql = """select count(*) as active_campaign
                from pika_dm.dim_campaign t1
                join 
                (select business_account_id, max(s_key) s_key from pika_dm.dim_business_account group by business_account_id ) t2
                on t1.business_account_s_key=t2.s_key
                join (select business_account_s_key, campaign_s_key, store_id,max(store_s_key) store_s_key from pika_dm.rel_campaign_business_store 
                                group by business_account_s_key, campaign_s_key, store_id) t3
                on t1.business_account_s_key = t3.business_account_s_key and t1.s_key=t3.campaign_s_key 
                where t1.business_account_id={business_account_id}
                and t1.isActive =1
                and t1.end_date = '9999-12-31 00:00:00'
                and t1.start_date between \'{from_date}\' and \'{to_date}\'"""

    if store_ids is None:
        s = sql

    else:

        # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
        s = sql + 'and t3.store_id in (' + ','.join(map(str,store_ids)) + ')'  # %(store_ids)


    formatted_sql = s.format(**arguments)
    print(formatted_sql)
    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()

    except Exception as e:
        raise e

    for data in result_set:
        active_campaign = data['active_campaign']

    campaign_dict['active_campaign'] = active_campaign

    ###################### campaign data - campaign

    sql = """select count(*) as campaigns
             from pika_dm.dim_campaign
             where end_date between \'{from_date}\' and \'{to_date}\'
             and business_account_id={business_account_id}
             and start_date between \'{from_date}\' and \'{to_date}\' """

    formatted_sql = sql.format(**arguments)

    print(formatted_sql)
    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()

    except Exception as e:
        raise e

    for data in result_set:
        campaigns = data['campaigns']

    campaign_dict['campaigns'] = campaigns

    ######################campaign data - campaign till date

    sql = """select count(*) as campaign_till_date 
            from  pika_dm.dim_campaign t1
            join (select business_account_s_key, business_account_id, store_id,max(store_s_key) store_s_key from pika_dm.rel_campaign_business_store 
                            group by business_account_s_key, business_account_id, store_id) t2
            on t1.business_account_id = t2.business_account_id
            and t1.business_account_s_key = t2.business_account_s_key
            where t2.business_account_id ={business_account_id}"""

    if store_ids is None:
        s = sql

    else:
        # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
        s = sql + ' and t2.store_id in (' + ','.join(map(str,store_ids)) + ')'  # %(store_ids)


    formatted_sql = s.format(**arguments)
    print(formatted_sql)

    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
    except Exception as e:
        raise e

    for data in result_set:
        campaign_till_date = data['campaign_till_date']

    campaign_dict['campaign_till_date'] = campaign_till_date

    ######################  camapign data - reach, converted

    dlist = []

    sql = """select count(*) as count
             from pika_dm.rt_consumer_campaign_response
             where response in ('DELIVERED','CONVERTED')
             and business_account_id={business_account_id}
             
             and response_date between \'{from_date}\' and \'{to_date}\' """

    if store_ids is None:
        s = sql +'  group by response'

    else:
        # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
        s = sql + ' and store_id in (' + ','.join(map(str, store_ids)) + ') group by response'  # %(store_ids)

    formatted_sql = s.format(**arguments)
    print(formatted_sql)

    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
    except Exception as e:
        raise e

    for data in result_set:
        print(data)
        dlist.append(data['count'])
    print(dlist)
    converted = dlist[0]
    delivered = dlist[1]

    campaign_dict['converted'] = converted
    campaign_dict['reached'] = delivered

    ######################### goal sub json

    goal_list = []
    goal_dict = {}

    sql = """select t1.campaign_goal_name as goal_name ,count(t3.response) as count,t3.response as response
             from pika_dm.dim_campaign t1             
             join pika_dm.rt_consumer_campaign_response t3
             on t1.campaign_id=t3.campaign_id
             and t3.business_account_id = t1.business_account_id
             and t1.business_account_s_key = t3.business_account_s_key
             join (select store_id,max(s_key) s_key from pika_dm.dim_store group by store_id) t2
             on t2.store_id = t3.store_id
             and t2.s_key = t3.store_s_key
             where t1.end_date='9999-12-31 00:00:00'
             and t1.start_date between \'{from_date}\' and \'{to_date}\'
             and t3.response in ('DELIVERED','CONVERTED')
             and t3.business_account_id = {business_account_id}"""



    if store_ids is None:

        a = ' group by t1.campaign_goal_name,t3.response'
        s = sql +a

    else:

        # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
        s = sql + ' and t3.store_id in (' + ','.join(map(str,store_ids)) + ') group by t1.campaign_goal_name,t3.response'  # %(store_ids)

    formatted_sql = s.format(**arguments)
    print(formatted_sql)
    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
        raise e

    for data in result_set:
        if data['response'] == 'CONVERTED':
            goal_dict['goal'] = data['goal_name']
            goal_dict['converted'] = data['count']

        elif data['response'] == 'DELIVERED':
            goal_dict['goal'] = data['goal_name']
            goal_dict['reach'] = data['count']
            goal_list.append(goal_dict.copy())

    campaign_dict['goal'] = goal_list

    output_result = json.dumps(campaign_dict)
    loaded_json = json.loads(output_result)

    return loaded_json


#################################### RT API
#
# def __rt_summary_channel(report_date, from_date, to_date, business_account_id):
#     arguments = locals()
#
#     sql = """select channel,response,count(*) as reach
#         from pika_dm.rt_consumer_campaign_response
#         where business_account_id={business_account_id} and response_date
#         between '2017-06-01' and '2017-07-31'
#         group by channel,response"""
#
#     formated_sql = sql.format(**arguments)
#
#     try:
#         results = db.engine.execute(text(formated_sql), ())
#         result_set = results.fetchall()
#     except Exception as e:
#         raise e
#
#     df = pd.DataFrame(result_set)
#     if df.empty is True:
#         return pd.DataFrame(None)
#     df.columns = results.keys()
#     print(df)
#     return df
#
#
#
#
# #b=df[['channel','response','campaign_id']].groupby(['channel','response']).agg(['count'])
#
#
# def __rt_summary_goal(report_date, from_date, to_date, business_account_id):
#     arguments = locals()
#
#     sql = """select goal_name,t3.response,count(t3.response)
#         from pika_dm.dim_campaign t1
#         join pika_dm.ref_campaign_goal t2
#         on t1.campaign_goal_id=t2.id
#         join pika_dm.rt_consumer_campaign_response t3
#         on t1.campaign_id=t3.campaign_id
#         where sys_end_date='9999-12-31 00:00:00'  and start_date between '2017-06-01'
#         and '2017-07-31'
#         and t3.response in ('DELIVERED','CONVERTED')
#         group by
#         t2.goal_name,t3.response"""
#
#     formated_sql = sql.format(**arguments)
#
#     try:
#         results = db.engine.execute(text(formated_sql), ())
#         result_set = results.fetchall()
#     except Exception as e:
#         raise e
#
#     df = pd.DataFrame(result_set)
#     if df.empty is True:
#         return pd.DataFrame(None)
#     df.columns = results.keys()
#     print(df)
#     return df
#
#
#
#
#
# def __rt_summary(report_date, from_date, to_date, business_account_id):
#     arguments = locals()
#
#     sql = """select count(*) as active_campaign
#             from pika_dm.dim_campaign
#             where sys_end_date='9999-12-31 00:00:00' and business_account_id=14 and start_date between '2017-06-01'
#         and '2017-07-31' """
#
#
#     formated_sql = sql.format(**arguments)
#
#     try:
#         results = db.engine.execute(text(formated_sql), ())
#         result_set = results.fetchall()
#     except Exception as e:
#         raise e
#
#     df = pd.DataFrame(result_set)
#     if df.empty is True:
#         return pd.DataFrame(None)
#     df.columns = results.keys()
#     print(df)
#     #return df
#
#
#     sql = """select count(*) as campaign_till_date from  pika_dm.dim_campaign"""
#
#     try:
#         results = db.engine.execute(sql)
#         result_set1 = results.fetchall()
#     except Exception as e:
#         raise e
#
#     df['campaign_till_date']=result_set1[0]
#     print(df)
#
#     return  df
#
#
#
#     # sql = """select response,count(*)
#     # from pika_dm.rt_consumer_campaign_response
#     # where response in ('DELIVERED','CONVERTED')
#     # group by response"""
#     #
#     # try:
#     #     results = db.engine.execute(sql)
#     #     result_set2 = results.fetchall()
#     # except Exception as e:
#     #     raise e
#     #
#     # df['campaign_trfeill_date'] = result_set2[1]
#     # print(df)
#     #
#     # return df



################################### RT CAMPAIGN SUMMARY  ########################

def __rt_campaign_summary(report_date, from_date, to_date, business_account_id, date_param_cd, store_ids):
    arguments = locals()


    campaign_dict = {}

    ########################## channel sub json

    channel_list = []
    channel_dict = {}
    channel_data = {}

    sql = """select channel,count(*) as count,response
                 from pika_dm.rt_consumer_campaign_response
                 where business_account_id={business_account_id}  
                 and channel in ('SMS','NOTIFICATION') 
                 and response in ('DELIVERED','CONVERTED') 
                 and response_date between \'{from_date}\' and \'{to_date}\' """
    # group by channel,response"""

    if store_ids is None:
        a = 'group by channel,response'

        s = sql + a

    else:

        # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
        s = sql + 'and store_id in (' + ','.join(map(str, store_ids)) + ') group by channel,response'  # %(store_ids)


    formatted_sql = s.format(**arguments)
    print(formatted_sql)
    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
        raise e

    for data in result_set:
        if data['response'] == 'CONVERTED':
            channel_dict["channel"] = data['channel']
            channel_dict["converted"] = data['count']

        elif data['response'] == 'DELIVERED':

            channel_dict['channel'] = data['channel']
            channel_dict['reach'] = data['count']
            channel_list.append(channel_dict.copy())

    channel_data['data'] = channel_list
    campaign_dict['channels'] = channel_data

    ############## campaign data  - active campaign

    sql = """select count(*) as active_campaign
             from pika_dm.dim_campaign
             where end_date='9999-12-31 00:00:00' and business_account_id={business_account_id} 
             and start_date between \'{from_date}\' and \'{to_date}\' and isActive=1"""

    formatted_sql = sql.format(**arguments)

    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
    except Exception as e:
        raise e

    for data in result_set:
        active_campaign = data['active_campaign']

    campaign_dict['active_campaing'] = active_campaign

    ###################### campaign data - campaign

    sql = """select count(*) as campaigns
                     from pika_dm.dim_campaign
                     where end_date between \'{from_date}\' and \'{to_date}\'
                     and business_account_id={business_account_id} 
                     and start_date between \'{from_date}\' and \'{to_date}\' """

    formatted_sql = sql.format(**arguments)

    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
        raise e

    for data in result_set:
        campaigns = data['campaigns']

    campaign_dict['campaigns'] = campaigns

    ########################## campaign data - campaign till date


    sql = """select count(distinct campaign_id) as campaign_till_date from  pika_dm.dim_campaign"""

    try:
        results = db.engine.execute(sql)
        result_set = results.fetchall()
    except Exception as e:
        raise e

    for data in result_set:
        campaign_till_date = data['campaign_till_date']

    campaign_dict['campaign_till_date'] = campaign_till_date

    ####################### camaping data - converted , reach

    data_list = []

    sql = """select count(*) as count
             from pika_dm.rt_consumer_campaign_response
             where response in ('DELIVERED','CONVERTED')
             group by response"""

    try:
        results = db.engine.execute(sql)
        result_set = results.fetchall()
    except Exception as e:
        raise e

    for data in result_set:
        print(data)
        data_list.append(data['count'])
    print(data_list)
    converted = data_list[0]
    delivered = data_list[1]

    campaign_dict['converted'] = converted
    campaign_dict['reached'] = delivered

    ################# goal sub json

    goal_data = {}
    goal_list = []
    goal_dict = {}

    sql = """select goal_name,count(t3.response) as count,t3.response as response
             from pika_dm.dim_campaign t1
             join pika_dm.ref_campaign_goal t2
             on t1.campaign_goal_id=t2.campaign_goal_id
             join pika_dm.rt_consumer_campaign_response t3
             on t1.campaign_id=t3.campaign_id
             where end_date='9999-12-31 00:00:00'  and start_date between \'{from_date}\' and \'{to_date}\'
             and t3.response in ('DELIVERED','CONVERTED')
             group by t2.goal_name,t3.response"""

    formatted_sql = sql.format(**arguments)

    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
        raise e

    for data in result_set:
        if data['response'] == 'CONVERTED':
            goal_dict['goal'] = data['goal_name']
            goal_dict['converted'] = data['count']

        elif data['response'] == 'DELIVERED':
            goal_dict['goal'] = data['goal_name']
            goal_dict['reach'] = data['count']
            goal_list.append(goal_dict.copy())

    goal_data['data'] = goal_list
    campaign_dict['goals'] = goal_data

    ###################### campaing data

    campaign_dict['type'] = 'Real-Time'

    report_duration_dict = {}

    start_date = from_date
    end_date = to_date
    report_duration_dict['start_date'] = start_date
    report_duration_dict['end_date'] = end_date

    print(report_duration_dict)

    campaign_dict['report_duration'] = report_duration_dict
    campaign_dict['date_param_cd'] = date_param_cd

    output_result = json.dumps(campaign_dict)
    loaded_json = json.loads(output_result)

    return loaded_json


###########################  CAMPAIGN TREND

def __campaign_trend(report_date, from_date, to_date, business_account_id, store_ids, date_param_cd, campaign_id):
    arguments = locals()

    campaign_dict = dict()

    campaign_dict['type'] = 'Batch'
    campaign_dict['date_param_cd'] = date_param_cd
    campaign_dict['aggregation_level'] = 'daily'

    report_duration_dict = dict()

    start_date = from_date
    end_date = to_date
    report_duration_dict['start_date'] = start_date
    report_duration_dict['end_date'] = end_date

    campaign_dict['report_duration'] = report_duration_dict
    campaign_dict['campaign_ids'] = campaign_id

    sub_json = {}
    sub_list = []

    sql = """select t1.response_date,t1.response,sum(t1.consumer_count) as count
             from pika_dm.agg_campaign_performance_daily t1
             join pika_dm.rel_campaign_business_store t2
             on t1.campaign_id  = t2.campaign_id
             and t1.business_account_id = t2.business_account_id
             and t1.store_id = t2.store_id
             where t1.business_account_id = {business_account_id} 
             and response in ('CONVERTED','DELIVERED')"""  # and t1.store_id = %s and t2.campaign_id ={campaign_id}
    # group by response_date,response  """%(store_ids)

    if store_ids is None:
        a = ' and t2.campaign_id in (' + ','.join(map(str, campaign_id)) + ') group by response_date,response'

        s = sql + a

    else:

        # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
        s = sql + ' and t2.campaign_id in (' + ','.join(map(str, campaign_id)) + ') and t1.store_id in (' + ','.join(
            map(str, store_ids)) + ') group by  response_date,response'  # %(store_ids)

    formatted_sql = s.format(**arguments)
    print(formatted_sql)

    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
        raise e

    # for data in result_set:
    #     sub_json = {'date':str(data['response_date']),'response': str(data['response']), 'count': str(data['count'])}
    #     print(sub_json)
    #     sub_list.append(sub_json.copy())
    #     print(sub_list)
    #
    # print(sub_list)

    #campaign_dict['data'] = sub_list

    converted_dict={}
    delivered_dict={}
    converted_list=[]
    delivered_list=[]
    for data in result_set:
        print(data)

        if data['response']=='CONVERTED':
            converted_dict = {'date': str(data['response_date']), 'count': str(data['count'])}
            converted_list.append(converted_dict.copy())
            print(converted_list)

        else:
            delivered_dict = {'date': str(data['response_date']), 'count': str(data['count'])}
            delivered_list.append(delivered_dict.copy())
            print(delivered_list)

    print(converted_list)
    print()
    print(delivered_list)

    campaign_dict['data']={'CONVERTED':converted_list,'DELIVERED':delivered_list}
    output_result = json.dumps(campaign_dict)
    loaded_json = json.loads(output_result)

    return loaded_json


######################  RT CAMPAIGN TRENDS



#####################old code #########################
def __rt_campaign_trend(report_date, from_date, to_date, business_account_id, date_param_cd, campaign_id, store_ids):
    arguments = locals()


    print(report_date, from_date, to_date, business_account_id, date_param_cd, campaign_id, store_ids)
    campaign_dict = dict()

    campaign_dict['type'] = 'Real Time'
    campaign_dict['date_param_cd'] = date_param_cd
    campaign_dict['aggregation_level'] = 'daily'

    report_duration_dict = dict()

    start_date = from_date
    end_date = to_date
    report_duration_dict['start_date'] = start_date
    report_duration_dict['end_date'] = end_date

    campaign_dict['report_duration'] = report_duration_dict
    campaign_dict['campaign_ids'] = campaign_id

    sub_json = {}
    sub_list = []

    sql = """select t1.response_date,t1.response,sum(t1.consumer_count) as count
             from pika_dm.agg_campaign_performance_daily t1
             join pika_dm.rel_campaign_business_store t2
             on t1.campaign_id  = t2.campaign_id
             and t1.business_account_id = t2.business_account_id
             and t1.store_id = t2.store_id
             where t1.business_account_id = {business_account_id}
             and response in ('CONVERTED','DELIVERED')
              and response_date between \'{from_date}\' and \'{to_date}\'"""

    if store_ids is None:
        a = 'and t2.campaign_id in (' + ','.join(map(str,campaign_id)) + ') group by response_date,response'

        s = sql + a

    else:

        # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
        s = sql + 'and t2.campaign_id in (' + ','.join(map(str,campaign_id)) + ') and t1.store_id in (' + ','.join(map(str,store_ids)) + ') group by  response_date,response'  # %(store_ids)

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

    converted_dict = {}
    delivered_dict = {}
    converted_list = []
    delivered_list = []
    for data in result_set:
        print(data)

        if data['response'] == 'CONVERTED':
            converted_dict = {'date': str(data['response_date']), 'count': str(data['count'])}
            converted_list.append(converted_dict.copy())
            print(converted_list)

        else:
            delivered_dict = {'date': str(data['response_date']), 'count': str(data['count'])}
            delivered_list.append(delivered_dict.copy())
            print(delivered_list)

    print(converted_list)
    print()
    print(delivered_list)

    campaign_dict['data'] = {'CONVERTED': converted_list, 'DELIVERED': delivered_list}

    output_result = json.dumps(campaign_dict)
    loaded_json = json.loads(output_result)

    return loaded_json




def __campaign_performance(report_date, from_date, to_date, business_account_id, date_param_cd,  store_ids):
    arguments = locals()


    print(report_date, from_date, to_date, business_account_id, date_param_cd,  store_ids)
    campaign_dict = dict()

    campaign_dict['type'] = 'Real Time'
    campaign_dict['date_param_cd'] = date_param_cd
    campaign_dict['aggregation_level'] = 'daily'

    report_duration_dict = dict()

    start_date = from_date
    end_date = to_date
    report_duration_dict['start_date'] = start_date
    report_duration_dict['end_date'] = end_date

    campaign_dict['report_duration'] = report_duration_dict


    sub_json = {}
    sub_list = []

    sql = """SELECT t1.campaign_id,t2.campaign_name AS campaign_name,CAST(t2.start_date AS DATE) AS start_date,
            (SUM(CASE WHEN t1.response = "CONVERTED" THEN t1.consumer_count ELSE 0 END) /SUM(CASE WHEN t1.response = "DELIVERED" THEN t1.consumer_count ELSE 0 END))*100 AS perf
            FROM pika_dm.agg_campaign_performance_daily t1
            INNER JOIN pika_dm.dim_campaign t2 ON t1.campaign_id=t2.campaign_id
            WHERE t2.end_date='9999-12-31 00:00:00' and t1.business_account_id = {business_account_id} AND response_date between \'{from_date}\' and \'{to_date}\' And response<>'SENT'"""

    if store_ids is None:
        a = 'GROUP BY t1.campaign_id,t2.campaign_name,t2.start_date ORDER BY perf DESC LIMIT 3'

        s = sql + a

    else:

        # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
        s = sql + 'and  t1.store_id in (' + ','.join(map(str,store_ids)) + ') GROUP BY t1.campaign_id,t2.campaign_name,t2.start_date ORDER BY perf DESC LIMIT 3'
    formatted_sql = s.format(**arguments)
    print(formatted_sql)

    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
        raise e

    for data in result_set:
        sub_json = {'campaign_name': str(data['campaign_name']),'start_date': str(data['start_date']),'Performance': str(data['perf'])}
        print(sub_json)
        sub_list.append(sub_json.copy())
        print(sub_list)


    sub_json1 = {}
    sub_list1 = []

    sql = """SELECT t1.campaign_id,t2.campaign_name AS campaign_name,CAST(t2.start_date AS DATE) AS start_date,
                (SUM(CASE WHEN t1.response = "CONVERTED" THEN t1.consumer_count ELSE 0 END) /SUM(CASE WHEN t1.response = "DELIVERED" THEN t1.consumer_count ELSE 0 END))*100 AS perf
                FROM pika_dm.agg_campaign_performance_daily t1
                INNER JOIN pika_dm.dim_campaign t2 ON t1.campaign_id=t2.campaign_id
                WHERE t2.end_date='9999-12-31 00:00:00' and t1.business_account_id = {business_account_id} AND response_date between \'{from_date}\' and \'{to_date}\' And response<>'SENT'"""

    if store_ids is None:
        a = 'GROUP BY t1.campaign_id,t2.campaign_name,t2.start_date ORDER BY perf LIMIT 3'

        s = sql + a

    else:

        # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
        s = sql + 'and  t1.store_id in (' + ','.join(map(str, store_ids)) + ') GROUP BY t1.campaign_id,t2.campaign_name,t2.start_date ORDER BY perf LIMIT 3'
    formatted_sql = s.format(**arguments)
    print(formatted_sql)

    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
        raise e

    for data in result_set:
        sub_json1 = {'campaign_name': str(data['campaign_name']), 'start_date': str(data['start_date']),
                    'Performance': str(data['perf'])}
        print(sub_json1)
        sub_list1.append(sub_json1.copy())
        print(sub_list1)
    campaign_dict['data'] = {'Top Three':sub_list,'Worst Three':sub_list1}

    output_result = json.dumps(campaign_dict)
    loaded_json = json.loads(output_result)

    return loaded_json
