import pandas as pd
from webargs import fields, validate, ValidationError
from webargs.flaskparser import use_args, use_kwargs, parser, abort
from flask_restful import Resource
from flask import json, request, current_app
from flask_jwt import jwt_required, current_identity

from analytics_backend.services.report_consumers import consumer_summary, consumer_activites, consumer_segment_summary, \
    consumer_segment_counts_trends, consumer_loyalty_interaction_summary
from analytics_backend.models.reference import ConsumerTagsModel, InteractionTypeModel, ParamsAppModel
from analytics_backend import db
import constants


# from auth_jwt import pika_jwt_required


###TEST CODE FOR WEB ARGS> NEED TO BE REMOVED

@parser.location_handler('data')
def parse_data(request, name, field):
    return request.data.get(name)


# Now 'data' can be specified as a location
@parser.use_args({'per_page': fields.Int()}, locations=('data',))
def posts(args):
    print('i am in use args')
    return 'displaying {} posts'.format(args['per_page'])


## Common Functions to validate column values

def validate_date_param(val):
    param_list = ParamsAppModel.get_date_param_list()
    if val in param_list:
        return True
    raise ValidationError("Wrong Date Param %s. Please request for  =%s" % (val, param_list))


def validate_tag_name(val):
    consumer_tags = db.session.query(ConsumerTagsModel.tag_name.distinct().label('tag_name'))
    tag_type_list = [row.tag_name for row in consumer_tags.all()]
    if val in tag_type_list:
        return True
    raise ValidationError("Wrong tag type=%s. Please request for  =%s" % (val, tag_type_list))


# This file should not have any dependecy with db.
# db should only be in services
# so return values to this file can be either dictionary or pandas
class ConsumerSummary(Resource):
    input_args = {'business_account_id': fields.Int(required=True)}

    # payload schema
    # get header information for content request and negotiaion

    def get(self):
        return {"message": "there is no get request for the API"}, 404

    @use_args(input_args)
    def post(self, args):


        try:
            output = consumer_summary(args['business_account_id'], [63, 65],
                                      'previous_week')  # need to make sure of sending correct param
        except Exception as e:
            raise ValueError("Something went wrong")
        return output  # dataframe


class ConsumerStoreSummary(Resource):
    input_args = {'business_account_id': fields.Int(required=True)}

    # payload schema
    # get header information for content request and negotiaion

    def get(self):
        return {"message": "there is no get request for the API"}, 404

    @use_args(input_args)
    def post(self, args, store_id):
        try:
            output = consumer_summary(args['business_account_id'], list((store_id,)),
                                      'current week')  # need to make sure of sending correct param
        except Exception as e:
            raise ValueError("Something went wrong")
        return output  # dataframe


class ConsumerSummaryByStore(Resource):
    pass


class ConsumerActivities(Resource):
    def validate_kpi(val):
        kpi_list = ['visits', 'revenue', 'redemption', 'purchase', 'checkins']
        if val in kpi_list:  # will add these in database later
            return True
        raise ValidationError("Wrong KPI %s. Please request for  =%s" % (val, kpi_list))


        # def validate_tag_type(val):
        #     consumer_tags=db.session.query(ConsumerTagsModel.tag_name.distinct().label('tag_name'))
        #     tag_type_list = [row.tag_name for row in consumer_tags.all()]

        # if val in tag_type_list:
        #     return True
        # raise ValidationError("Wrong tag type=%s. Please request for  =%s" % (val, tag_type_list))
        #

    input_args = {'report_type': fields.Str(required=True),
                  'business_account_id': fields.Int(required=True),
                  'tag_types': fields.DelimitedList(fields.Str(validate=validate_tag_name)),
                  'kpi': fields.DelimitedList(fields.Str(validate=validate_kpi)),
                  'date_code': fields.Str(required=True, validate=validate_date_param)}

    @use_args(input_args)
    def post(self, args):
        try:
            output = consumer_activites(args['business_account_id'], "CURRENT_WEEK",
                                        args['report_type'], args['tag_types'], args['kpi'])
        except Exception as e:
            raise ValueError(e)
        return output


# good
class ConsumersSegmentSummary(Resource):
    input_args = {'business_account_id': fields.Int(required=True),
                  'store_ids': fields.List(fields.Int(required=True)),
                  'tag_names': fields.List(fields.Str(validate=lambda val: val in
                                                                           [r for r, in db.session.query(
                                                                               ConsumerTagsModel.tag_name.distinct()).all()])),
                  'date_param_cd': fields.Str(required=True, validate=lambda val: val in
                                                                                  [r for r, in db.session.query(
                                                                                      ParamsAppModel.param_name_cd.distinct()).all()])
                  }

    @use_args(input_args)
    def get(self, args):
        if 'tag_names' in args:
            return {"message": "Please use POST method to filter with tag_names"}, 303
        return {"message": "Test0"}

    @use_args(input_args)
    def post(self, args):
        if 'store_ids' in args:
            summary_df = consumer_segment_summary(args['business_account_id'], args['date_param_cd'], args['store_ids'])
        else:
            summary_df = consumer_segment_summary(args['business_account_id'], args['date_param_cd'], None)
        if not summary_df.empty:
            return summary_df
        elif request.headers['Accept'] == 'text/csv':
            return pd.DataFrame({})
        else:
            return {}


# good 29June

class ConsumersSegmentCountsTrends(Resource):
    input_args = {'business_account_id': fields.Int(required=True),
                  'store_ids': fields.List(fields.Int(required=True)),
                  'date_param_cd': fields.Str(required=True, validate=lambda val: val in
                                                                                  [r for r, in db.session.query(
                                                                                      ParamsAppModel.param_name_cd.distinct()).all()]),
                  'aggregate_level': fields.Str(validate=lambda val: val in ['aggregate_level', 'weekly'])
                  }

    @use_args(input_args)
    def get(self, args):
        if 'tag_names' in args:
            return {"message": "Please use POST method to filter with tag_names"}, 303
        return {"message": "Test0"}

    @use_args(input_args)
    def post(self, args):
        if 'store_ids' in args:
            store_ids = args['store_ids']
        else:
            store_ids = None

        if 'aggregate_level' not in args:
            aggregate_level = None
        else:
            aggregate_level = args['aggregate_level']
        summary_df = consumer_segment_counts_trends(args['business_account_id'], args['date_param_cd'], store_ids,
                                                    aggregate_level=aggregate_level)

        # if data frame is not empty
        return summary_df.to_json(orient='index')


class ConsumerLoyaltyInteractionSummary(Resource):
    input_args = {'business_account_id': fields.Int(required=True),
                  'store_ids': fields.List(fields.Int(required=True)),
                  'date_param_cd': fields.Str(required=True, validate=lambda val: val in
                                                                                  [r for r, in db.session.query(
                                                                                      ParamsAppModel.param_name_cd.distinct()).all()]), }

    @use_args(input_args)
    def get(self, args):
        return {"message": "Please use POST method"}, 303

    @use_args(input_args)
    def post(self, args):
        if 'store_ids' in args:
            store_ids = args['store_ids']
        else:
            store_ids = None

        if 'aggregate_level' not in args:
            aggregate_level = None
        else:
            aggregate_level = args['aggregate_level']

        interaction_df = consumer_loyalty_interaction_summary(args['business_account_id'], args['date_param_cd'],
                                                              store_ids)

        if interaction_df.empty is True:
            return pd.DataFrame({})
        # if data frame is not empty
        interaction_df.reset_index()
        interaction_df.set_index(keys=['loyalty_currency', 'interaction_type_category', 'interaction_type'],
                                 inplace=True)
        print(interaction_df.to_json(orient='records'))
        return interaction_df.to_json(orient='index')


##########Reference Data#########################
# good
class RefConsumersSegmentCombination(Resource):
    input_args = {'business_account_id': fields.Int(required=True),
                  'tag_names': fields.List(fields.Str(validate=lambda val: val in
                                                                           [r for r, in db.session.query(
                                                                               ConsumerTagsModel.tag_name.distinct()).all()]))
                  }

    @use_args(input_args)
    def get(self, args):
        if 'tag_names' in args:
            return {"message": "Please use POST method to filter with tag_names"}, 303
        return ConsumerTagsModel.create_customer_segment_combinations()

    @use_args(input_args)
    def post(self, args):
        if args['tag_names']:
            return ConsumerTagsModel.create_customer_segment_combinations(args['tag_names'])
        return ConsumerTagsModel.create_customer_segment_combinations()


# good

class RefConsumersSegmentList(Resource):
    input_args = {'business_account_id': fields.Int(required=True),
                  'tag_names': fields.List(fields.Str(validate=lambda val: val in
                                                                           [r for r, in db.session.query(
                                                                               ConsumerTagsModel.tag_name.distinct()).all()]))
                  }

    @use_args(input_args)
    def get(self, args):
        if 'tag_names' in args:
            return {"message": "Please use POST method to filter with tag_names"}, 303

        all_tags = pd.DataFrame(ConsumerTagsModel.get_tags(tag_type=constants.TAGS_CONSUMER_SEGMENT_TYPE))
        dict_out = {}
        for i_tag_name in all_tags.tag_name.unique():
            sub_df = all_tags[all_tags.tag_name == i_tag_name]
            values_list = list(sub_df.tag_value.unique())
            dict_node = {"tag_name": i_tag_name, "tag_values": values_list}
            dict_out[i_tag_name] = dict_node
        return dict_out

    @use_args(input_args)
    # @pika_jwt_required
    # @jwt_required()
    def post(self, args):
        print(args)
        all_tags = pd.DataFrame(ConsumerTagsModel.get_tags(tag_type=constants.TAGS_CONSUMER_SEGMENT_TYPE))
        if 'tag_names' in args:
            all_tags = all_tags[all_tags.tag_name.isin(args['tag_names'])]
        dict_out = {}
        for i_tag_name in all_tags.tag_name.unique():
            sub_df = all_tags[all_tags.tag_name == i_tag_name]
            values_list = list(sub_df.tag_value.unique())
            dict_node = {"tag_name": i_tag_name, "tag_values": values_list}
            dict_out[i_tag_name] = dict_node
        return dict_out


# wip
class RefConsumersSegmentFilterStats(Resource):
    condition = {'tag_name': fields.Str(validate=validate_tag_name),
                 'tag_values': fields.DelimitedList(fields.Str())
                 }
    tag_filter_schema = {'conditions': fields.DelimitedList(fields.Nested(condition))}

    input_args = {'business_account_id': fields.Int(required=True),
                  'store_ids': fields.DelimitedList(fields.Int(required=False)),
                  'date_param_cd': fields.Str(validate=validate_date_param),
                  'tag_filter': fields.Nested(tag_filter_schema)
                  }

    @use_args(input_args)
    def get(self, args):
        return {"message": "Try with POST calls. You may feel lucky!"}, 303

    @use_args(input_args)
    def post(self, args):
        ##LOGIC FOR CAMPAIGN STATS - Need to connect with Anupam
        return {"total_count": 1245, "filtered_count": 123, "overlapping_count": 500, "active_campaigns":
            [123, 345]
                }

# @parser.error_handler
# def handle_request_parsing_error(err):
#     """webargs error handler that uses Flask-RESTful's abort function to return
#     a JSON error response to the client.
#     """
#     abort(422, errors=err.messages)
