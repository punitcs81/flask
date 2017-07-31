from webargs import fields, validate, ValidationError
from webargs.flaskparser import use_args, use_kwargs, parser, abort
from sqlalchemy import exc
from flask import current_app

from flask_restful import Resource
from analytics_backend.models.audit import AuditCampaignStatus
from analytics_backend.models.reference import ConsumerTagsModel
from analytics_backend import db
import constants
from analytics_backend.utils import date_time as dt


class CampaignStatus(Resource):
    input_args = {'campaign_id': fields.Int(required=True),
                  'status': fields.Str(30, validate=lambda s: s in constants.PIKA_CAMPAIGN_STATUS_CODES)}

    @use_args(input_args)
    def get(self, args):
        campaign_id = args['campaign_id']
        try:
            result = AuditCampaignStatus.query.filter_by(campaign_id=campaign_id).order_by(
                'sys_updated_date desc').first()
        except Exception as e:
            raise e

        if result is not None:
            return {'campaign_id': campaign_id, 'status': result.status,
                    'header': {'message': 'api returns successfully',
                               'return_code': constants.PIKA_RESPONSE_STATUS_SUCCESS}}
        return {'campaign_id': campaign_id, 'status': None,
                'header': {'message': 'no status found. Either data preparation not started or campaign id is wrong',
                           'return_code': constants.PIKA_RESPONSE_STATUS_SUCCESS}}

    @use_args(input_args)
    def post(self, args):

        campaign_id = args['campaign_id']
        status = args['status']

        if status is None:
            self.get(args)

        try:
            campaign_status = AuditCampaignStatus.query.filter_by(campaign_id=campaign_id).first()

        except Exception as e:
            raise e

        if campaign_status is None:
            return {'campaign_id': campaign_id, 'status': None,
                    'header': {'message': 'Can\'t add a new status through the API. Only status update is possible ',
                               'return_code': constants.PIKA_RESPONSE_STATUS_SUCCESS}}

        campaign_status.status = status
        campaign_status.sys_updated_date = dt.get_current_db_date_time(current_app.config['REPORT_DB_DATETIME_FORMAT'])

        try:
            db.session.commit()
        except:
            return {'campaign_id': campaign_id, 'status': status,
                    'header': {'message': 'Database update failed',
                               'return_code': constants.PIKA_RESPONSE_DB_UPDATE_FAILED}}, 422

        return {'campaign_id': campaign_id, 'status': status,
                'header': {'message': 'New status updated ',
                           'return_code': constants.PIKA_RESPONSE_STATUS_SUCCESS}}
