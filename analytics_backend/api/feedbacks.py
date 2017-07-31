from flask_restful import Api
from flask import make_response
from flask import json, jsonify
import pandas as pd

from analytics_backend.api import report_api as report_blueprint
from analytics_backend.resources.feedbacks import (FeedbacksSummary, FeedbacksTrend)

api = Api(report_blueprint)


@api.representation('application/json')
def output_json(data, code, headers=None):
    if isinstance(data, dict):
        resp = make_response(json.dumps(data), code)
        resp.headers.extend(headers or {})
    elif isinstance(data, pd.DataFrame):
        if data.empty is True:
            resp = make_response(json.dumps({}), code)
        else:
            resp = make_response(data.to_json(orient='records'), code)
        resp.headers.extend(headers or {})
    elif isinstance(data, str):
        data_j = json.loads(data)
        resp = make_response(jsonify(data_j), code)
        resp.headers.extend(headers or {})
    else:
        raise ValueError("JSON content negotiator expects data frame or dictionary objects as data input")
    return resp


@api.representation('text/csv')
def output_csv(data, code, headers=None):
    # some CSV serialized data
    # we have to add a condition to check if input string is in CSV format.
    if isinstance(data, pd.DataFrame) is False:
        raise ValueError("CSV content negotiator expects data frame objects as data input")
    if data.empty is True:
        data = ''
    elif code > 202:
        data = data
    else:
        data = data.to_csv(index_label='row_num')
    resp = make_response(data)  # header is responsibility of the program to put in dataframe
    return resp


api.add_resource(FeedbacksSummary, '/feedbacks-summary')
api.add_resource(FeedbacksTrend, '/feedbacks-trends')
