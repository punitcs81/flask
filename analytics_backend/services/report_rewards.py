import os
import pandas as pd

from pika_analytics import db
import pika_analytics.utils.date_time as dt
from pika_analytics.models.reference import ParamsAppModel
import config

# following block should be added with app
config_name = os.environ.get('PA_CONFIG', 'development')
current_config=config.config[config_name]



def reward_summary(business_account_id:int, store_ids:list, date_param):
    date_range=ParamsAppModel.get_date_range(14, 'Current Week')
    data = __reward_summary(date_range['report_date'], date_range['lower_bound'], date_range['upper_bound'], business_account_id)
    # print(data)
    if data.empty is True:
        return data
    return (data[data['attributed_store_id'].isin(store_ids)])



def __reward_summary(report_date, from_date, to_date, business_account_id :int, store_id : int = None):
    #Dates are converted to MySQL DB date (as per config param). This is to avoid sending %d kind of string
    # for SQLAlchemy to handle

    if not dt.validate_date_format(report_date,current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s"%report_date, current_config.REPORT_DB_DATE_FORMAT)
    if not dt.validate_date_format(from_date,current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % report_date, current_config.REPORT_DB_DATE_FORMAT)
    if not dt.validate_date_format(to_date,current_config.REPORT_DB_DATE_FORMAT):
        raise ValueError("report_date: %s failed validation. Expected Format is %s" % report_date, current_config.REPORT_DB_DATE_FORMAT)
    arguments = locals()

    sql = """select gender, recency, frequency, monetary, clv_1year_level,clv_3year_level,
                   badge , attributed_store_id, count(distinct(consumer_id)) count
                   from dim_business_consumer
                   where business_account_id = {business_account_id}
 				  and (\'{report_date}\' between eff_start_date AND eff_end_date )
 				  and (registered_on between \'{from_date}\' and \'{to_date}\') 
                    group by gender, recency, frequency, monetary, clv_1year_level,clv_3year_level,
               badge,attributed_store_id"""
    formated_sql = sql.format(**arguments)

    try:
        results = db.engine.execute(formated_sql,())
        result_set = results.fetchall()
    except Exception as e:
        raise e

    df = pd.DataFrame(result_set)
    if df.empty is False:
        df.columns = results.keys()
    return df

