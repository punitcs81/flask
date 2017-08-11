import pandas as pd
from webargs import fields, validate
from webargs.flaskparser import use_args, use_kwargs, parser, abort
from analytics_backend.services.report_campaings import campaign_summary,RTcampaign_summary,campaign_trend,rt_campaign_trend,campaign_performance
from flask_restful import Resource
from analytics_backend.services.report_consumers import consumer_summary, consumer_activites, consumer_segment_summary, \
    consumer_segment_counts_trends, consumer_loyalty_interaction_summary
from analytics_backend.models.reference import ConsumerTagsModel, InteractionTypeModel, ParamsAppModel
from analytics_backend import db
import logging
from auth_jwt import validate_roles_and_access
from flask_jwt import jwt_required, current_identity


logger = logging.getLogger(__name__)

class CampaignsSummary(Resource):
    input_args = {'business_account_id': fields.Int(required=False),
                  'date_param_cd': fields.Str(required=False, validate=lambda val: val in
                                                                                  [r for r, in db.session.query(
                                                                                      ParamsAppModel.param_name_cd.distinct()).all()]),
                  'store_ids': fields.List(fields.Int(required=False))}

    # payload schema
    # get header information for content request and negotiaion

    #@jwt_required()
    @use_args(input_args)
    #@validate_roles_and_access
    def post(self,args):
        #return {"message": "there is no get request for the API post"}, 404
        # try:
        #     output = campaign_summary(args['business_account_id'],args['store_ids'],args['date_param_cd']) #need to make sure of sending correct param
        # except Exception as e:
        #     raise ValueError("Something went wrong")
        # return output #dataframe
        logger.info('getting campaign summary')
        try:
            if 'store_ids' in args:
                output = campaign_summary(args['business_account_id'], args['store_ids'], args['date_param_cd'])  # need to make sure of sending correct param
            else:
                output = campaign_summary(args['business_account_id'], None, args['date_param_cd'])

        except Exception as e:
            raise ValueError("Something went wrong")
            logger.error('unable to get campaign summary')
        return output #dataframe


    def get(self):
        logger.info('campaign summary get method')
        return {"message": "there is no get request for the API"}, 404
        pass



class RTCampaignsSummary(Resource):
    input_args = {'business_account_id': fields.Int(required=False),
                  'store_ids': fields.List(fields.Int(required=False)),
                  'date_param_cd': fields.Str(required=False, validate=lambda val: val in
                                                                                  [r for r, in db.session.query(
                                                                                      ParamsAppModel.param_name_cd.distinct()).all()])
                  }

    #@jwt_required()
    @use_args(input_args)
    #   @validate_roles_and_access
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




    #@use_args(input_args)
    def get(self):
        logger.info('RTcampaign summary get method')
        return {"message": "there is no get request for the API"}, 404
        pass



class CampaignsResponseTrend (Resource):
    input_args = {'business_account_id': fields.Int(required=False),
                  'store_ids': fields.List(fields.Int(required=False)),
                  'campaign_id': fields.List(fields.Int(required=False)),
                  'date_param_cd': fields.Str(required=False, validate=lambda val: val in
                                                                                  [r for r, in db.session.query(
                                                                                      ParamsAppModel.param_name_cd.distinct()).all()])

                  }


    #@jwt_required()
    @use_args(input_args)
    #@validate_roles_and_access
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
        logger.info('campaign summary trend get method')
        return {"message":"hi there"}
        pass



class RTCampaignsResponseTrend (Resource):
    input_args = {'business_account_id': fields.Int(required=False),
                  'store_ids': fields.List(fields.Int(required=False)),
                  'campaign_id': fields.List(fields.Int(required=False)),
                  'date_param_cd': fields.Str(required=False, validate=lambda val: val in
                                                                                  [r for r, in db.session.query(
                                                                                      ParamsAppModel.param_name_cd.distinct()).all()])

                  }

    #@jwt_required()
    @use_args(input_args)
    #@validate_roles_and_access
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
        logger.info('RTcampaign summary trend get method')
        return {"message":"hi there"}
        pass



class CampaignPerformance(Resource):
    input_args = {'business_account_id': fields.Int(required=False),
                  'store_ids': fields.List(fields.Int(required=False)),
                  'date_param_cd': fields.Str(required=False, validate=lambda val: val in
                                                                                   [r for r, in db.session.query(
                                                                                       ParamsAppModel.param_name_cd.distinct()).all()])

                  }

    # @jwt_required()
    @use_args(input_args)
    # @validate_roles_and_access
    def post(self, args):

        # return {"message": "hi there post"},,,

        try:
            if 'store_ids' in args:
                output = campaign_performance(args['business_account_id'], args['date_param_cd'],

                                           args['store_ids'])  # need to make sure of sending correct param
            else:
                output = campaign_performance(args['business_account_id'], args['date_param_cd'],
                                          None)

        except Exception as e:
            raise ValueError("Something went wrong")
        return output

    def get(self):
        logger.info('RTcampaign summary trend get method')
        return {"message": "hi there"}
        pass

