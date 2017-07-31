from flask_restful import Resource
from webargs import fields, validate, ValidationError
from webargs.flaskparser import use_args, use_kwargs, parser, abort
from celery.result import AsyncResult
from flask import Request,jsonify

# from analytics_backend.utils.flask_parser import use_args
import constants

from analytics_backend import celery_app


class AddNumbers(Resource):
    input_args = {'A': fields.Int(required=True), 'B': fields.Int(required=True)}

    # payload schema
    # get header information for content request and negotiaion


    @use_args(input_args)
    def post(self, args):
        try:
            task = celery_app.send_task('tasks.add', (args['A'], args['B']))
        except Exception as e:
            raise ValueError(e)
        return {"message": "Task Submitted Successfully", "task_id": task.id}, 202


class Fib(Resource):
    input_args = {'num': fields.Int(required=True)}

    # payload schema
    # get header information for content request and negotiaion


    @use_args(input_args)
    def post(self, args):
        try:
            task = celery_app.send_task('tasks.fib', (args['num'],))
        except Exception as e:
            raise ValueError(e)
        return {"message": "Task Submitted Successfully", "task_id": task.id}, 202


class Status(Resource):
    input_args = {'task_id': fields.Str(required=True)}

    # payload schema
    # get header information for content request and negotiaion

    @use_args(input_args)
    def post(self, args):
        res = AsyncResult(args['task_id'], app=celery_app)
        try:
            result = res.status
        except Exception as e:
            result = None
        if result:
            message = "Task Exists"
            status = constants.PIKA_RESPONSE_STATUS_SUCCESS
            task_id = res.id
            task_status = res.status
        else:
            task_id = None
            task_status = None
            message = "Task Not Found"
            status = constants.PIKA_RESPONSE_STATUS_FAILURE

        return {"task_id": task_id,
                "status": status,
                "message": message,
                "task_status": task_status,
                "result": result}


class PushEvents(Resource):
    input_args = {'consumer_id': fields.Int(required=True),
                  'business_account_id': fields.Int(required=True),
                  'store_id': fields.Int(required=True),
                  'campaign_id': fields.Int(required=True),
                  'real_time_event_id': fields.Int(required=True, validate=lambda x: x in constants.PIKA_EVENT_TYPES)}

    # payload schema
    # get header information for content request and negotiaion

    @use_args(input_args)
    def post(self, args):
        try:
            task = celery_app.send_task('tasks.fib', (10,))
        except Exception as e:
            raise ValueError(e)
        return {"status": constants.PIKA_RESPONSE_STATUS_ACCEPTED,
                "message": "Task Submitted Successfully",
                "task_id": task.id}, 202


class PushEvents(Resource):
    input_args = {'consumer_id': fields.Int(required=True),
                  'business_account_id': fields.Int(required=True),
                  'store_id': fields.Int(required=True),
                  'real_time_event_type': fields.Str(required=True)}

    # payload schema
    # get header information for content request and negotiaion

    @use_args(input_args)
    def post(self, args):

        if args['real_time_event_type'] == constants.PIKA_EVENT_TYPE_CHECKIN:

            try:
                celery_arguments = (args['consumer_id'], args['business_account_id'], args['store_id'])
                task = celery_app.send_task(
                    constants.CELERY_EVENT_MODULE_NAME + '.' + constants.CELERY_EVENT_TASK_CHECKIN,
                    celery_arguments, queue=constants.CELERY_EVENT_QUE_NAME)
            except Exception as e:
                raise ValueError(e)
            return {"status": constants.PIKA_RESPONSE_STATUS_ACCEPTED,
                    "message": "Task Submitted Successfully",
                    "task_id": task.id}, 202


        elif args['real_time_event_type'] == constants.PIKA_EVENT_TYPE_PURCHASE:
            return {"status": constants.PIKA_RESPONSE_STATUS_ACCEPTED,
                    "message": constants.PIKA_EVENT_TYPE_PURCHASE + " - Event Not Implemeted Yet ",
                    "task_id": None}, 202

        elif args['real_time_event_type'] == constants.PIKA_EVENT_TYPE_REDEEM:
            return {"status": constants.PIKA_RESPONSE_STATUS_ACCEPTED,
                    "message": constants.PIKA_EVENT_TYPE_REDEEM + " - Event Not Implemeted Yet ",
                    "task_id": None}, 202


        elif args['real_time_event_type'] == constants.PIKA_EVENT_TYPE_REGISTRATION:
            return {"status": constants.PIKA_RESPONSE_STATUS_ACCEPTED,
                    "message": constants.PIKA_EVENT_TYPE_REGISTRATION + " - Event Not Implemeted Yet ",
                    "task_id": None}, 202

        else:
            return {"status": constants.PIKA_RESPONSE_STATUS_FAILURE,
                    "message": "Unknown Event Type  ",
                    "task_id": None}, 400



class Test(Resource):
    def get(self):
        return {'hello': 'world'}
