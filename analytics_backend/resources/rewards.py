import pandas as pd
from webargs import fields, validate
from webargs.flaskparser import use_args, use_kwargs, parser, abort

from flask_restful import Resource
from analytics_backend.services.report_consumers import consumer_summary, consumer_activites

#This file should not have any dependecy with db.
#db should only be in services
#so return values to this file can be either dictionary or pandas
class RewardsSummary(Resource):
    input_args = {'b_id':fields.Int(required=True)}

    # payload schema
    # get header information for content request and negotiaion

    @use_args(input_args)
    def get(self,args):

        try:
            output= consumer_summary(args['b_id'],[63,65],'current_week') #need to make sure of sending correct param
        except Exception as e:
            raise ValueError("Something went wrong")
        return output #dataframe


    def post(self):
        pass

class RewardsList(Resource):
    pass

class RewardsActivitiesTrend (Resource):
    def get(self, args):

        try:
            output = consumer_activites(args['b_id'], [63, 65],
                                      'current_week')  # need to make sure of sending correct param
        except Exception as e:
            raise ValueError("Something went wrong")
        return output  # dataframe

    pass

    def post(self):
        return {"message":"hi there"}
