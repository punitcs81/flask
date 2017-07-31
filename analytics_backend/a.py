import pandas as pd
import sqlalchemy
import json
from sqlalchemy import create_engine
import json

def __feedback_tags_summary():

    engine = create_engine('mysql+pymysql://qadb:qadb@104.196.135.227/pika_dm')

#     sql = """select sum(feedback_total_till_date) as feedback_count_till_date
#                            from pika_dm.agg_buss_store_stats_daily where business_account_id = 14 """
#
#     sql1 = """select sum(feedback_total) as feedback_count,avg(feedback_avg) as average_feedback_rating,sum(feedback_1) , sum(feedback_1)/5 ,
# sum(feedback_2) , sum(feedback_2)/5,sum(feedback_3) , sum(feedback_3)/5,sum(feedback_4) as count, sum(feedback_4)/5 ,sum(feedback_5) , sum(feedback_5)/5
#                         from pika_dm.agg_buss_store_stats_daily where interaction_date BETWEEN '2014-08-20' and '2014-12-30' """
#
#
#     for data in engine.execute(sql).fetchone():
#         feedback_count_till_date = str(data)
#
#     for data in engine.execute(sql1).fetchall():
#         feedback_count = str(data.feedback_count)
#         average_feedback_rating = str(data.average_feedback_rating)
#         f1_count = str(data[2])
#         f1_rating = str(data[3])
#         f2_count = str(data[4])
#         f2_rating = str(data[5])
#         f3_count = str(data[6])
#         f3_rating = str(data[7])
#         f4_count = str(data[8])
#         f4_rating = str(data[9])
#         f5_count = str(data[10])
#         f5_rating = str(data[11])
#
#     class rater:
#         def __init__(self, rating_level, count, rating):
#             self.rating_level = rating_level
#             self.count = count
#             self.rating = rating
#
#     r1 = rater(1, f1_count, f1_rating)
#     r2 = rater(2, f2_count, f2_rating)
#     r3 = rater(3, f3_count, f3_rating)
#     r4 = rater(4, f4_count, f4_rating)
#     r5 = rater(5, f5_count, f5_rating)
#
#     raterList = []
#
#     raterList.append(r1)
#     raterList.append(r2)
#     raterList.append(r3)
#     raterList.append(r4)
#     raterList.append(r5)
#
#     class feedback:
#         def __init__(self, feed_count, feed_tilldata, feed_avg, feed_detail):
#             self.feed_count = feed_count
#             self.feed_avg = feed_avg
#             self.feed_tilldate = feed_tilldata
#             self.feed_detail = feed_detail
#
#     f1 = feedback(feedback_count, feedback_count_till_date, average_feedback_rating, raterList)
#
#     def obj_dict(feedback):
#         return feedback.__dict__
#
#     json_string = json.dumps(f1, default=obj_dict)
#
#     print(json_string)





###################################  CAMPAIGN API  ###############################
    sql = """select channel,count(*) as reach,response
    from pika_dm.rt_consumer_campaign_response
    where business_account_id=14 and channel in ('SMS','NOTIFICATION') and response in ('DELIVERED','CONVERTED') and response_date
    between '2017-06-01' and '2017-07-31'
    group by channel,response"""



    l=[]
    results = engine.execute(sql)
    result_set = results.fetchall()

    d={}

    #print(result_set)
    for data in result_set:
        if data[2]=='CONVERTED':
            d["channel"]=data[0]
            d["CONVERTED"]=data[1]
            #l.append(d.copy())
            #print(d)

        elif data[2]=='DELIVERED':

            d['channel'] = data[0]
            d['DELIVERED'] = data[1]
            l.append(d.copy())
            #print(d)

    print(l)

    sql = """select business_account_id,count(*) as active_campaign
    from pika_dm.dim_campaign
    where sys_end_date='9999-12-31 00:00:00' and business_account_id=14 and start_date between '2017-06-01'
and '2017-07-31' """

    sql1 = """select count(*) as campaign_till_date from  pika_dm.dim_campaign"""

    results = engine.execute(sql)
    result_set = results.fetchall()

    d1 = {}
    for data in result_set:
        bid=data[0]
        active_campaing=data[1]

    results = engine.execute(sql1)
    result_set = results.fetchall()

    for data in result_set:
        till_date=data[0]


    sql2="""select count(*)
from pika_dm.rt_consumer_campaign_response
where response in ('DELIVERED','CONVERTED')
group by response"""


    results = engine.execute(sql2)
    result_set = results.fetchall()
    print(result_set)
    dlist=[]
    for data in result_set:
        print(data)
        dlist.append(data[0])
    print(dlist)
    converted=dlist[0]
    delivered=dlist[1]

    print(converted)
    print(delivered)
    d1['campaign_till_date']=till_date
    d1['bid']=bid
    d1['active_campaing']=active_campaing
    d1['channel']=l
    d1['converted']=converted
    d1['reached']=delivered


    sql4="""select goal_name,count(t3.response),t3.response
from pika_dm.dim_campaign t1
join pika_dm.ref_campaign_goal t2
on t1.campaign_goal_id=t2.id
join pika_dm.rt_consumer_campaign_response t3
on t1.campaign_id=t3.campaign_id
where sys_end_date='9999-12-31 00:00:00'  and start_date between '2017-06-01'
and '2017-07-31'
and t3.response in ('DELIVERED','CONVERTED')
group by
t2.goal_name,t3.response"""

    list=[]
    dict={}
    results = engine.execute(sql4)
    result_set = results.fetchall()


    for data in result_set:
            if data[2] == 'CONVERTED':
                dict['goal'] = data[0]
                dict['CONVERTED'] = data[1]
                # l.append(d.copy())
                # print(d)

            elif data[2] == 'DELIVERED':

                dict['goal'] = data[0]
                dict['DELIVERED'] = data[1]
                list.append(dict.copy())

    d1['goal']=list

    #print(d1)






    r = json.dumps(d1)
    loaded_r = json.loads(r)

    print(type(r))  # Output str
    print(type(loaded_r))  # Output dict

    print(loaded_r)

__feedback_tags_summary()



########################END OF CAMPAIGN API #################################

#
#     sql = """select channel, response
# from pika_dm.rt_consumer_campaign_response
# where business_account_id=14  and response_date
# between '2017-06-01' and '2017-07-31'
# """
#
#
#     try:
#         results = engine.execute(sql)
#         result_set = results.fetchall()
#     except Exception as e:
#         raise e
#
#     df = pd.DataFrame(result_set)
#     if df.empty is True:
#         return None
#     df.columns = results.keys()
#     #print(df)
#
#     a=df.groupby(['channel','response']).size()
#     print((a))
#
#     dict={}
#
#     print(dict)
#
#
#
#     '''j1 = (
#         df.groupby(['channel'], as_index=False)
#             .apply(lambda x: x[['count', 'channel']].to_dict('s'))
#             # .apply(lambda x: x[['sum(feedback_2)', 'rating2']].to_dict('d'))
#
#             .reset_index()
#             .rename(columns={0: 'channel'})
#
#             #.rename(columns={1: 'Detareiled_feedback'})
#             .to_json(orient='records'))
#     #print(j1)
#
#     print({k: g[['count', 'response']].to_dict('i') for k, g in df.groupby("channel")})
#
#     sql = """select business_account_id,count(*) as active_campaign
#     from pika_dm.dim_campaign
#     where sys_end_date='9999-12-31 00:00:00' and business_account_id=14"""
#     # formated_sql = sql.format(**arguments)
#
#     try:
#         results = engine.execute(sql)
#         result_set = results.fetchall()
#     except Exception as e:
#         raise e
#
#     df1 = pd.DataFrame(result_set)
#     if df1.empty is True:
#         return None
#     df1.columns = results.keys()
#     print(df1)
#
#
#
#
#     sql2 = """select count(*),response
#      from pika_dm.rt_consumer_campaign_response
#      where response in ('DELIVERED','CONVERTED')
#      group by response"""
#
#     try:
#         results = engine.execute(sql2)
#         result_set = results.fetchall()
#     except Exception as e:
#         raise e
#
#     df2 = pd.DataFrame(result_set)
#     if df2.empty is True:
#         return None
#     df2.columns = results.keys()
#     print(df2)
#
#
#     key_columns = ['business_account_id']
#     comb_summary_df = pd.merge(df, df1, how='right', on=key_columns)
#     print(comb_summary_df)
#
#     comb2= pd.merge(comb_summary_df,df2,how='left')
#     print(comb2)
#     j = (
#     comb_summary_df.groupby(['business_account_id', 'active_campaign'], as_index=False)
#         .apply(lambda x: x[['channel', 'response']].to_dict('r'))
#         # .apply(lambda x: x[['sum(feedback_2)', 'rating2']].to_dict('d'))
#
#         .reset_index()
#         .rename(columns={0: 'channel'})
#
#         # .rename(columns={1: 'Detareiled_feedback'})
#         .to_json(orient='records'))
#     print(j)'''
#
# __feedback_tags_summary()