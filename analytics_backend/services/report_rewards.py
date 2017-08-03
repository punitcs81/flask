import os
import pandas as pd
from sqlalchemy.exc import DatabaseError
from analytics_backend import db
import analytics_backend.utils.date_time as dt
from analytics_backend.models.reference import ParamsAppModel
import config
import json

# following block should be added with app
config_name = os.environ.get('PA_CONFIG', 'development')
current_config = config.config[config_name]


def reward_summary(business_account_id, date_param_cd):
    # return {"message": "there is no get request for the API post"}, 404

    if date_param_cd not in [r for r, in db.session.query(ParamsAppModel.param_name_cd.distinct()).all()]:
        raise ValueError("Wrong date_param_cd")
    date_range = ParamsAppModel.get_date_range(business_account_id, date_param_cd)
    #print(date_range)

    data = ParamsAppModel.get_param_data(business_account_id)
    print(data)

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

    print(business_account_id, date_param_cd, lower_bound, upper_bound, prev_lower_bound, prev_upper_bound)

    reward_summary_output = __reward_summary(business_account_id, date_param_cd, lower_bound, upper_bound,
                                             prev_lower_bound, prev_upper_bound)

    return reward_summary_output

def reward_list(business_account_id, date_param_cd, store_id):
    #return {"message": "there is no get request for the API post"}, 404


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

    print(business_account_id, date_param_cd, lower_bound, upper_bound, prev_lower_bound, prev_upper_bound)

    reward_list_output = __reward_list(business_account_id, date_param_cd, lower_bound, upper_bound,
                                             store_id)

    return reward_list_output




def reward_activity_trend(business_account_id, date_param_cd, store_id):
    # return {"message": "there is no get request for the API post"}, 404
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

    print(business_account_id, date_param_cd, store_id, lower_bound, upper_bound)

    reward_json = __reward_activity_trend(business_account_id, lower_bound, upper_bound, date_param_cd, store_id)

    return reward_json




def ref_param(business_account_id):

    param_json = __ref_param(business_account_id)

    return param_json
###################PROTECTED METHODS #########################

def __reward_summary(business_account_id, date_param_cd, lower_bound, upper_bound, prev_lower_bound, prev_upper_bound):
    # Dates are converted to MySQL DB date (as per config param). This is to avoid sending %d kind of string
    # for SQLAlchemy to handle

    if not dt.validate_date_format(lower_bound, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % lower_bound,
                         current_config.REPORT_DB_DATE_FORMAT)
    if not dt.validate_date_format(upper_bound, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % upper_bound,
                         current_config.REPORT_DB_DATE_FORMAT)
    if not dt.validate_date_format(prev_lower_bound, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % prev_lower_bound,
                         current_config.REPORT_DB_DATE_FORMAT)
    if not dt.validate_date_format(prev_upper_bound, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % prev_upper_bound,
                         current_config.REPORT_DB_DATE_FORMAT)

    main_dict={}

    arguments = locals()

    main_dict['api_name']='reward_summary'
    main_dict['date_param_cd'] = date_param_cd


    ###################### current ########################
    sql = """select interaction_date as date,interaction_redeem_count as redemptions,
             interaction_redeem_points as points_redeemed,interaction_purchase_count as purchases,
             interaction_earn_points  as points_earned,interaction_checkin_count  as check_ins ,
             consumer_new_count as new_customers
             from pika_dm.agg_buss_store_stats_daily
             where interaction_date between  '2014-08-01' and '2014-12-31'
             and business_account_id=14 
             group by interaction_date,interaction_redeem_count,interaction_redeem_points, 
             interaction_purchase_count,interaction_earn_points,interaction_checkin_count,consumer_new_count """

    formatted_sql = sql.format(**arguments)

    print(formatted_sql)

    data_dict = {}
    sub_list = []
    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
        raise e

    for data in result_set:
        data_dict =  {'redemptions': str(data['redemptions']),
                                         'points_redeemed': str(data['points_redeemed']),
                                         'purchases': str(data['purchases']),
                                         'points_earned': str(data['points_earned']),
                                         'check_ins': str(data['check_ins']),
                                         'new_customers': str(data['new_customers'])}
        print(data_dict)
        #sub_list.append(data_dict.copy())
        #print(sub_list)

    #print(sub_list)
    main_dict['current'] = data_dict


    ###################### PREVIOUS  #####################################
    sql = """select interaction_date as date,interaction_redeem_count as redemptions,
                 interaction_redeem_points as points_redeemed,interaction_purchase_count as purchases,
                 interaction_earn_points  as points_earned,interaction_checkin_count  as check_ins ,
                 consumer_new_count as new_customers
                 from pika_dm.agg_buss_store_stats_daily
                 where interaction_date between  '2014-08-01' and '2014-12-31'
                 and business_account_id=14 
                 group by interaction_date,interaction_redeem_count,interaction_redeem_points, 
                 interaction_purchase_count,interaction_earn_points,interaction_checkin_count,consumer_new_count """

    formatted_sql = sql.format(**arguments)

    print(formatted_sql)

    data_dict = {}
    sub_list = []
    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
        raise e

    for data in result_set:
        data_dict = {'redemptions': str(data['redemptions']),
                     'points_redeemed': str(data['points_redeemed']),
                     'purchases': str(data['purchases']),
                     'points_earned': str(data['points_earned']),
                     'check_ins': str(data['check_ins']),
                     'new_customers': str(data['new_customers'])}
        print(data_dict)
        # sub_list.append(data_dict.copy())
        # print(sub_list)

    # print(sub_list)
    main_dict['previous'] = data_dict


    output_result = json.dumps(main_dict)
    loaded_json = json.loads(output_result)

    return loaded_json


def __reward_activity_trend(business_account_id, from_date, to_date, date_params_cd, store_ids):
    # return {"message": "there is no get request for the API post"}, 404

    print(business_account_id, from_date, to_date, date_params_cd, store_ids)
    if not dt.validate_date_format(from_date, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % from_date,
                         current_config.REPORT_DB_DATE_FORMAT)
    if not dt.validate_date_format(to_date, current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % to_date,
                         current_config.REPORT_DB_DATE_FORMAT)
    arguments = locals()

    reward_dict = {}

    ################## Main json data ###################

    reward_dict['report_name'] = 'reward_trend'
    reward_dict['date_param_cd'] = date_params_cd
    reward_dict['aggregation_level'] = 'daily'
    reward_dict['store_id'] = store_ids

    report_duration_dict = dict()

    start_date = from_date
    end_date = to_date
    report_duration_dict['start_date'] = start_date
    report_duration_dict['end_date'] = end_date

    reward_dict['report_duration'] = report_duration_dict

    #########  have to use to_date and from date   here

    sql = """select interaction_date as date,interaction_redeem_count as redemptions,
             interaction_redeem_points as points_redeemed,interaction_purchase_count as purchases,
             interaction_earn_points  as points_earned,interaction_checkin_count  as check_ins ,
             consumer_new_count as new_customers
             from pika_dm.agg_buss_store_stats_daily
             where interaction_date between  '2014-08-01' and '2014-12-31'
             and business_account_id=14"""

    if store_ids is None:
        a = ' group by interaction_date,interaction_redeem_count,interaction_redeem_points, ' \
            ' interaction_purchase_count,interaction_earn_points,interaction_checkin_count,consumer_new_count '

        s = sql + a

    else:

        # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
        s = sql + ' and store_id in (' + ','.join( map(str, store_ids)) + ')  group by interaction_date,interaction_redeem_count,interaction_redeem_points, ' \
                                   ' interaction_purchase_count,interaction_earn_points,interaction_checkin_count,consumer_new_count '

    formatted_sql = s.format(**arguments)

    print(formatted_sql)

    data_dict = {}
    sub_list = []
    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
        raise e

    for data in result_set:
        data_dict = {str(data['date']): {'redemptions': str(data['redemptions']),
                                         'points_redeemed': str(data['points_redeemed']),
                                         'purchases': str(data['purchases']),
                                         'points_earned': str(data['points_earned']),
                                         'check_ins': str(data['check_ins']),
                                         'new_customers': str(data['new_customers'])}}
        print(data_dict)
        sub_list.append(data_dict.copy())
        print(sub_list)

    print(sub_list)
    reward_dict['data'] = sub_list
    output_result = json.dumps(reward_dict)
    loaded_json = json.loads(output_result)

    return loaded_json



def __reward_list(business_account_id, from_date, to_date, date_params_cd, store_ids):
    #return {"message": "there is no get request for the API post"}, 404

    print(business_account_id, from_date, to_date, date_params_cd, store_ids)

    arguments = locals()

    reward_dict = {}

    ################## Main json data ###################

    reward_dict['api_name'] = 'reward_list'


    #########  have to use to_date and from date   here

    sql = """select t1.reward_description as gift_name, t3.category_name as product_category, 
              cast(t1.reward_begin_date as date) as start_date,t1.point_value as value,
             t4.total_redeem_count as allocated,t4.total_redeem_points as redeemed
            from  pika_dm.dim_rewards t1
            join pika_dm.dim_products t2
            on
            t1.product_id = t2. product_id
            join pika_dm.dim_product_category t3
            on t2.product_category_id = t3.product_category_id
            join pika_dm.agg_reward_performance_daily t4
            on t1.reward_id = t4.reward_id
            where  t4.business_account_id = 14
           
            and t1.reward_begin_date between '2013-01-01' and '2017-12-31'"""


    if store_ids is None:

        s = sql

    else:

        # sql_query = 'select name from studens where id in (' + ','.join(map(str, l)) + ')'
        s = sql + ' and store_id in (' + ','.join( map(str, store_ids)) + ')  '

    formatted_sql = s.format(**arguments)

    print(formatted_sql)

    data_dict = {}
    sub_list = []
    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
        raise e

    for data in result_set:
        data_dict = {'gift_name': str(data['gift_name']),
                                         'product_category': str(data['product_category']),
                                         'start_date': str(data['start_date']),
                                         'value': str(data['value']),
                                         'allocated': str(data['allocated']),
                                         'redeemed': str(data['redeemed'])}

        print(data_dict)
        sub_list.append(data_dict.copy())
        print(sub_list)

    print(sub_list)
    reward_dict['data'] = sub_list
    output_result = json.dumps(reward_dict)
    loaded_json = json.loads(output_result)

    return loaded_json



def __ref_param(business_account_id):
    arguments = locals()

    main_dict = {}

    ################## Main json data ###################

    main_dict['api_name'] = 'ref_params'

    #########  have to use to_date and from date   here

    sql = """select lower_bound,upper_bound,prev_lower_bound,prev_upper_bound,param_name
            from pika_dm.ref_ParamApp
            where business_account_id = {business_account_id}"""

    formatted_sql = sql.format(**arguments)

    print(formatted_sql)

    data_dict = {}
    sub_list = []
    try:
        results = db.engine.execute(formatted_sql)
        result_set = results.fetchall()
        print(result_set)
    except Exception as e:
        raise e

    for data in result_set:
        data_dict = {'lower_bound': str(data['lower_bound']),'upper_bound': str(data['upper_bound']),
                     'prev_lower_bound': str(data['prev_lower_bound']),
                     'prev_upper_bound': str(data['prev_upper_bound']),
                     'param_name': str(data['param_name'])
                     }

        print(data_dict)
        sub_list.append(data_dict.copy())
        print(sub_list)

    print(sub_list)
    main_dict['data'] = sub_list
    output_result = json.dumps(main_dict)
    loaded_json = json.loads(output_result)

    return loaded_json