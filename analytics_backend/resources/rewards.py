import pandas as pd
from webargs import fields, validate
from webargs.flaskparser import use_args, use_kwargs, parser, abort
from flask_restful import Resource
from analytics_backend import db
from analytics_backend.models.reference import ParamsAppModel
from analytics_backend.services.report_rewards import reward_summary,reward_activity_trend,reward_list,ref_param
from auth_jwt import validate_roles_and_access
from flask_jwt import jwt_required, current_identity


# This file should not have any dependecy with db.
# db should only be in services
# so return values to this file can be either dictionary or pandas
class RewardsSummary(Resource):
    input_args = {
        'business_account_id': fields.Int(required=False),
        'date_param_cd': fields.Str(required=False, validate=lambda val: val in
                                                                        [r for r, in db.session.query(
                                                                            ParamsAppModel.param_name_cd.distinct()).all()]),
    }
    # payload schema
    # get header information for content request and negotiaion

    #@jwt_required()
    @use_args(input_args)
    #@validate_roles_and_access
    def post(self, args):

        try:
            output = reward_summary(args['business_account_id'], args['date_param_cd'])

        except Exception as e:
            raise ValueError("Something went wrong")
        return output


    def get(self):
        return {"message": "there is no get request for the API"}, 404
        pass


class RewardsList(Resource):
    input_args = { 'business_account_id': fields.Int(required=False),
                   'store_ids':fields.List(fields.Int(required=False)),
                   'date_param_cd':fields.Str(required=False , validate = lambda val: val in
                                                                        [r for r, in db.session.query(
                                                                            ParamsAppModel.param_name_cd.distinct()).all()]),
    }


    def get(self):
        return {'message':'test'}
        pass

    #@jwt_required()
    @use_args(input_args)
    #@validate_roles_and_access
    def post(self, args):

        try:
            if 'store_ids' in args:
                output = reward_list(args['business_account_id'], args['date_param_cd'], args['store_ids'])

            else:
                output = reward_list(args['business_account_id'], args['date_param_cd'], None)

        except Exception as e:
            raise ValueError("Something went wrong")
        return output


class RewardsActivitiesTrend(Resource):
    input_args = {
        'business_account_id': fields.Int(required=False),
        'store_ids': fields.List(fields.Int(required=False)),
        'date_param_cd': fields.Str(required=False, validate=lambda val: val in
                                                                        [r for r, in db.session.query(
                                                                            ParamsAppModel.param_name_cd.distinct()).all()]),
    }

    def get(self):
        return {"message": "there is no get request for the API"}, 404
        pass

    #@jwt_required()
    @use_args(input_args)
    #@validate_roles_and_access
    def post(self, args):

        try:
            if 'store_ids' in args:
                output = reward_activity_trend(args['business_account_id'], args['date_param_cd'], args['store_ids'])

            else:
                output = reward_activity_trend(args['business_account_id'], args['date_param_cd'], None)

        except Exception as e:
            raise ValueError("Something went wrong")
        return output


class Ref_params(Resource):
    input_args = {
        'business_account_id': fields.Int(required=False),
         }

    def get(self):
        return {"message": "there is no get request for the API"}, 404
        pass

    @jwt_required()
    @use_args(input_args)
    @validate_roles_and_access
    def post(self, args):

        try:

            output = ref_param(args['business_account_id'])

        except Exception as e:
            raise ValueError("Something went wrong")
        return output