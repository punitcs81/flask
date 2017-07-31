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


def feedbacks_summary(business_account_id, date_param_cd,store_ids):
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
    #return {"message": "there is no get request for the API "}, 404
    print(store_ids)

    feedback_summary = __feedback_summary(lower_bound,upper_bound, business_account_id,store_ids)
    # feedback_df  = __feedback_tags_summary(date_range['lower_bound'], date_range['upper_bound'], business_account_id)
    # feedback_till_date_df = __feedback_till_date( business_account_id)
    # if store_ids:
    #     feedback_df = feedback_df[feedback_df['store_id'].isin(store_ids)]
    #     feedback_till_date_df = feedback_till_date_df[feedback_till_date_df['store_id'].isin(store_ids)]
    # key_columns = ['store_id']
    # comb_summary_df = pd.merge(feedback_df, feedback_till_date_df, how='inner', on=key_columns)
    #
    # def f(x):
    #     return (dict( {k: v for k, v in zip(x.feedback_1, x.rating1)}))
    # BabyDataSet = list(zip('sum(feedback_1)', 'rating1'))
    #
    # def fun(x):
    #         return x[['sum(feedback_1)', 'rating1']],x[['sum(feedback_2)', 'rating2']].to_dict('r')
    # j = (comb_summary_df.groupby(['sum(feedback_total)', 'avg(feedback_avg)', 'feedback_total_till_date'], as_index=False)
    #         #.apply(lambda x: x[['sum(feedback_2)', 'rating2']].to_dict('r'))
    #         #.apply(lambda x: x[['sum(feedback_2)', 'rating2']].to_dict('d'))
    #         .apply(fun)
    #         .reset_index()
    #         .rename(columns={0: 'Detailed_feedback'})
    #
    #
    #         #.rename(columns={1: 'Detareiled_feedback'})
    #         .to_json(orient='records'))
    # comb_summary_df.fillna(0, inplace=True)

    #return feedback_till_date_df
    return feedback_summary




def feedbacks_trend(business_account_id, date_param_cd,store_ids):
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

    feedback_summary = __feedback_trend(lower_bound,upper_bound, business_account_id,date_param_cd,store_ids)

    return feedback_summary





##################################  PROTECTED METHODS  ##############################

#
# def __feedback_tags_summary(from_date, to_date, business_account_id: int):
#     arguments = locals()
#
#     sql = """select sum(feedback_total) , avg(feedback_avg), sum(feedback_1) , sum(feedback_1)/5 as rating1, sum(feedback_2), sum(feedback_2)/5 as rating2 ,
#               sum(feedback_3),sum(feedback_3)/5 as rating3 , sum(feedback_4) , sum(feedback_4)/5 as rating4,sum(feedback_5) , sum(feedback_5)/5 as rating5,
#               store_id from
#               pika_dm.agg_buss_store_stats_daily
#               where interaction_date between \'{from_date}\' and \'{to_date}\' and business_account_id = {business_account_id}
#               group by feedback_total , feedback_avg,feedback_1 ,feedback_2 ,feedback_3 ,feedback_4 ,feedback_5 ,
#               store_id"""
#     formated_sql = sql.format(**arguments)
#
#     try:
#         results = db.engine.execute(formated_sql)
#         result_set = results.fetchall()
#     except Exception as e:
#         raise e
#
#     df = pd.DataFrame(result_set)
#     if df.empty is True:
#         return None
#     df.columns = results.keys()
#     return  df
#
#
#
# def __feedback_till_date(business_account_id: int):
#     arguments = locals()
#
#     sql = """select feedback_total_till_date,store_id from pika_dm.agg_buss_store_stats_daily where interaction_date= '2014-08-24'
#               and business_account_id = {business_account_id}"""
#     formated_sql = sql.format(**arguments)
#
#     try:
#         results = db.engine.execute(formated_sql)
#         result_set = results.fetchall()
#     except Exception as e:
#         raise e
#
#     df = pd.DataFrame(result_set)
#     if df.empty is True:
#         return None
#     df.columns = results.keys()
#     return df




################################ FEEDBACK SUMMARY #################################

def __feedback_summary(from_date, to_date, business_account_id,store_ids):
    arguments = locals()


    print(store_ids)

    #ab = tuple(store_ids)
    # print(ab)
    feedback_dict = {}

    sql = "select sum(feedback_total) as total_feed , sum(feedback_avg)/5 as avg_feed, sum(feedback_1) as feed1 , sum(feedback_1)/5 as rating1, sum(feedback_2) as feed2, sum(feedback_2)/5 as rating2 ," \
          " sum(feedback_3) as feed3,sum(feedback_3)/5 as rating3 , sum(feedback_4) as feed4, sum(feedback_4)/5 as rating4,sum(feedback_5) as feed5, sum(feedback_5)/5 as rating5 " \
          " from pika_dm.agg_buss_store_stats_daily  where interaction_date between '2014-01-01' and '2014-12-31' and business_account_id = 14 "

             #group by feedback_total , feedback_avg,feedback_1 ,feedback_2 ,feedback_3 ,feedback_4 ,feedback_5 """

    if store_ids is None:
        a = "group by feedback_total , feedback_avg,feedback_1 ,feedback_2 ,feedback_3 ,feedback_4 ,feedback_5 "

        s= sql + a

    else:

        #sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
        s = sql + 'and store_id in (' + ','.join(map(str, store_ids)) + ') group by feedback_total , feedback_avg,feedback_1 ,feedback_2 ,feedback_3 ,feedback_4 ,feedback_5'#%(store_ids)
    print(s)

    formatted_sql = s.format(**arguments)

    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
            raise e

    feed = []
    #sub_json={}
    for data in result_set:

        total_feed = str(data['total_feed'])
        avg_feed = str(data['avg_feed'])
        feed1 = str(data['feed1'])
        rating1 = str(data['rating1'])
        feed2 = str(data['feed2'])
        rating2 = str(data['rating2'])
        feed3 = str(data['feed3'])
        rating3 = str(data['rating3'])
        feed4 = str(data['feed4'])
        rating4= str(data['rating4'])
        feed5 = str(data['feed5'])
        rating5 = str(data['rating5'])

        print(total_feed,avg_feed,feed1,rating1)
    sub_json1 = {'rating_level':'1','rating':rating1,'count':feed1}
    sub_json2 = {'rating_level': '2', 'rating': rating2, 'count': feed2}
    sub_json3 = {'rating_level': '3', 'rating': rating3, 'count': feed3}
    sub_json4 = {'rating_level': '4', 'rating': rating4, 'count': feed4}
    sub_json5 = {'rating_level': '5', 'rating': rating5, 'count': feed5}

    print(sub_json1)
    feed.append(sub_json1.copy())
    feed.append(sub_json2.copy())
    feed.append(sub_json3.copy())
    feed.append(sub_json4.copy())
    feed.append(sub_json5.copy())

    feedback_dict['detailed_feedback'] = feed
    feedback_dict['feedback_count'] = total_feed
    feedback_dict['average_feedback_rating'] = avg_feed

    ################## main json

    sql = """select feedback_total_till_date 
             from pika_dm.agg_buss_store_stats_daily 
             where interaction_date= '2014-08-24'
             and business_account_id = {business_account_id}"""
            ##################  interaction date  is the last end date of the date frame ############


    formatted_sql = sql.format(**arguments)

    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
    except Exception as e:
        raise e


    for data in result_set:
        feedback_total_till_date = data['feedback_total_till_date']

    feedback_dict['feedback_total_till_date']=feedback_total_till_date

    r = json.dumps(feedback_dict)
    loaded_r = json.loads(r)

    return loaded_r



############################### FEEDBACK TREND ########################


def __feedback_trend(from_date, to_date, business_account_id,date_param_cd,store_ids):
    arguments = locals()

    feedback_dict = {}

    feedback_dict['date_param_cd'] = date_param_cd
    feedback_dict['aggregation_level'] = 'daily'


    ############### sub json

    sql = "select interaction_date,sum(feedback_total) as total_feed , sum(feedback_avg)/5 as avg_feed, sum(feedback_1) as feed1 , sum(feedback_1)/5 as rating1, sum(feedback_2) as feed2, sum(feedback_2)/5 as rating2 ," \
          " sum(feedback_3) as feed3,sum(feedback_3)/5 as rating3 , sum(feedback_4) as feed4, sum(feedback_4)/5 as rating4,sum(feedback_5) as feed5, sum(feedback_5)/5 as rating5 " \
          " from pika_dm.agg_buss_store_stats_daily  where interaction_date between '2014-01-01' and '2014-12-31' and business_account_id = 14 "

    # group by feedback_total , feedback_avg,feedback_1 ,feedback_2 ,feedback_3 ,feedback_4 ,feedback_5 """

    if store_ids is None:
        a = "group by interaction_date,feedback_total , feedback_avg,feedback_1 ,feedback_2 ,feedback_3 ,feedback_4 ,feedback_5 "

        s = sql + a

    else:

        # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
        s = sql + 'and store_id in (' + ','.join(map(str,store_ids)) + ') group by interaction_date,feedback_total , feedback_avg,feedback_1 ,feedback_2 ,feedback_3 ,feedback_4 ,feedback_5'  # %(store_ids)
    print(s)

    formatted_sql = s.format(**arguments)

    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
        raise e


    detail_feed={}
    date_json = {}

    for data in result_set:
        total_feed = str(data['total_feed'])
        avg_feed = str(data['avg_feed'])
        feed1 = str(data['feed1'])
        rating1 = str(data['rating1'])
        feed2 = str(data['feed2'])
        rating2 = str(data['rating2'])
        feed3 = str(data['feed3'])
        rating3 = str(data['rating3'])
        feed4 = str(data['feed4'])
        rating4 = str(data['rating4'])
        feed5 = str(data['feed5'])
        rating5 = str(data['rating5'])

        print(total_feed, avg_feed, feed1, rating1)


        sub_json1 = {'rating_level': '1', 'rating': rating1, 'count': feed1}
        sub_json2 = {'rating_level': '2', 'rating': rating2, 'count': feed2}
        sub_json3 = {'rating_level': '3', 'rating': rating3, 'count': feed3}
        sub_json4 = {'rating_level': '4', 'rating': rating4, 'count': feed4}
        sub_json5 = {'rating_level': '5', 'rating': rating5, 'count': feed5}

        print(sub_json1)
        feed = []

        feed.append(sub_json1.copy())
        feed.append(sub_json2.copy())
        feed.append(sub_json3.copy())
        feed.append(sub_json4.copy())
        feed.append(sub_json5.copy())

        detail_feed['detailed_feedback_rating'] = feed
        detail_feed['feedback_count'] = total_feed
        detail_feed['average_feedback_rating'] = avg_feed


        #date_json[str(data['interaction_date'])]=detail_feed
        date_json[str(data['interaction_date'])]= {'feedback_count':total_feed,'average_feedback_rating':avg_feed,'detailed_feedback_rating':feed}

    ################## main json

    feedback_dict['trend'] = date_json

    r = json.dumps(feedback_dict)
    loaded_r = json.loads(r)

    return loaded_r