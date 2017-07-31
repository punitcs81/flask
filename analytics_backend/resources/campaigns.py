import pandas as pd
from webargs import fields, validate
from webargs.flaskparser import use_args, use_kwargs, parser, abort
from analytics_backend.services.report_campaings import campaign_summary,RTcampaign_summary,campaign_trend,rt_campaign_trend
from flask_restful import Resource
from analytics_backend.services.report_consumers import consumer_summary, consumer_activites, consumer_segment_summary, \
    consumer_segment_counts_trends, consumer_loyalty_interaction_summary
from analytics_backend.models.reference import ConsumerTagsModel, InteractionTypeModel, ParamsAppModel
from analytics_backend import db

class CampaignsSummary(Resource):
    input_args = {'business_account_id': fields.Int(required=True),
                  'date_param_cd': fields.Str(required=True, validate=lambda val: val in
                                                                                  [r for r, in db.session.query(
                                                                                      ParamsAppModel.param_name_cd.distinct()).all()]),
                  'store_ids': fields.List(fields.Int(required=True))}

    # payload schema
    # get header information for content request and negotiaion

    @use_args(input_args)
    def post(self,args):
        #return {"message": "there is no get request for the API post"}, 404
        # try:
        #     output = campaign_summary(args['business_account_id'],args['store_ids'],args['date_param_cd']) #need to make sure of sending correct param
        # except Exception as e:
        #     raise ValueError("Something went wrong")
        # return output #dataframe

        try:
            if 'store_ids' in args:
                output = campaign_summary(args['business_account_id'], args['store_ids'], args['date_param_cd'])  # need to make sure of sending correct param
            else:
                output = campaign_summary(args['business_account_id'], None, args['date_param_cd'])

        except Exception as e:
            raise ValueError("Something went wrong")
        return output #dataframe


    def get(self):
        return {"message": "there is no get request for the API"}, 404
        pass


class RTCampaignsSummary(Resource):
    input_args = {'business_account_id': fields.Int(required=True),
                  'store_ids': fields.List(fields.Int(required=True)),
                  'date_param_cd': fields.Str(required=True, validate=lambda val: val in
                                                                                  [r for r, in db.session.query(
                                                                                      ParamsAppModel.param_name_cd.distinct()).all()])
                  }

    @use_args(input_args)
    def post(self,args):
        print(args)
        #return {"message": "there is no get request for the API resource"}, 404

        try:
            if 'store_ids' in args:

                output = RTcampaign_summary(args['business_account_id'], args['date_param_cd'],  args['store_ids'])
            else:
                output = RTcampaign_summary(args['business_account_id'], args['date_param_cd'], None)

        except Exception as e:
            raise ValueError("Something went wrong")
        return output #dataframe




    @use_args(input_args)
    def get(self,args):
        return {"message": "there is no get request for the API"}, 404
        pass



class CampaignsResponseTrend (Resource):
    input_args = {'business_account_id': fields.Int(required=True),
                  'store_ids': fields.List(fields.Int(required=True)),
                  'campaign_id': fields.List(fields.Int(required=True)),
                  'date_param_cd': fields.Str(required=True, validate=lambda val: val in
                                                                                  [r for r, in db.session.query(
                                                                                      ParamsAppModel.param_name_cd.distinct()).all()])

                  }

    @use_args(input_args)
    def post(self, args):
        #return {"message": "hi there post"},,,

        # try:
        #     output = campaign_trend(args['business_account_id'],args['store_ids'],args['date_param_cd'],args['campaign_id'])
        # except Exception as e:
        #     raise ValueError("Something went wrong")
        # return output  # dataframe

        try:
            if 'store_ids' in args:
                output = campaign_trend(args['business_account_id'], args['store_ids'], args['date_param_cd'],
                                        args['campaign_id'])# need to make sure of sending correct param
            else:
                output = campaign_trend(args['business_account_id'], None, args['date_param_cd'],
                                        args['campaign_id'])

        except Exception as e:
            raise ValueError("Something went wrong")
        return output

    def get(self):
        return {"message":"hi there"}
        pass



class RTCampaignsResponseTrend (Resource):
    input_args = {'business_account_id': fields.Int(required=True),
                  'store_ids': fields.List(fields.Int(required=True)),
                  'campaign_id': fields.List(fields.Int(required=True)),
                  'date_param_cd': fields.Str(required=True, validate=lambda val: val in
                                                                                  [r for r, in db.session.query(
                                                                                      ParamsAppModel.param_name_cd.distinct()).all()])

                  }

    @use_args(input_args)
    def post(self, args):

        #return {"message": "hi there post"},,,





        try:
            if 'store_ids' in args:
                output = rt_campaign_trend(args['business_account_id'], args['date_param_cd'],
                                           args['campaign_id'], args['store_ids'])# need to make sure of sending correct param
            else:
                output = rt_campaign_trend(args['business_account_id'], args['date_param_cd'],
                                           args['campaign_id'], None)

        except Exception as e:
            raise ValueError("Something went wrong")
        return output

    def get(self):
        return {"message":"hi there"}
        pass
