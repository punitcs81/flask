import pandas as pd
from webargs import fields, validate
from webargs.flaskparser import use_args, use_kwargs, parser, abort

from flask_restful import Resource
from analytics_backend.services.report_consumers import consumer_summary, consumer_activites
from analytics_backend.services.reports_feedbacks import feedbacks_summary, feedbacks_trend
from analytics_backend.models.reference import ConsumerTagsModel, InteractionTypeModel, ParamsAppModel
from analytics_backend import db


# This file should not have any dependecy with db.
# db should only be in services
# so return values to this file can be either dictionary or pandas
class FeedbacksSummary(Resource):
    input_args = {'business_account_id': fields.Int(required=True),
                  'store_ids': fields.List(fields.Int(required=True)),
                  'date_param_cd': fields.Str(required=True, validate=lambda val: val in
                                                                                  [r for r, in db.session.query(
                                                                                      ParamsAppModel.param_name_cd.distinct()).all()])
                  }

    # payload schema
    # get header information for content request and negotiaion

    def get(self):
        return {"message": "there is no get request for the API "}, 404

    @use_args(input_args)
    def post(self, args):
        # return {"message": "there is no get request for the API "}, 404
        try:
            if 'store_ids' in args:
                output = feedbacks_summary(args['business_account_id'], args['date_param_cd'],
                                           args['store_ids'])  # need to make sure of sending correct param
            else:
                output = feedbacks_summary(args['business_account_id'], args['date_param_cd'], None)

        except Exception as e:
            raise ValueError("Something went wrong")
        return output  # dataframe


class FeedbacksTrend(Resource):
    input_args = {'business_account_id': fields.Int(required=True),
                  'store_ids': fields.List(fields.Int(required=True)),
                  'date_param_cd': fields.Str(required=True, validate=lambda val: val in
                                                                                  [r for r, in db.session.query(
                                                                                      ParamsAppModel.param_name_cd.distinct()).all()])
                  }

    # payload schema
    # get header information for content request and negotiaion

    def get(self):
        return {"message": "there is no get request for the API "}, 404

    @use_args(input_args)
    def post(self, args):
        # return {"message": "there is no get request for the API "}, 404
        try:
            if 'store_ids' in args:

                output = feedbacks_trend(args['business_account_id'], args['date_param_cd'],
                                         args['store_ids'])  # need to make sure of sending correct param
            else:
                output = feedbacks_trend(args['business_account_id'], args['date_param_cd'], None)

        except Exception as e:
            raise ValueError("Something went wrong")
        return output  # dataframe
