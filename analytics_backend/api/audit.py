from flask_restful import Api
from flask import make_response
from flask import json
import pandas as pd

from analytics_backend.api import audit_blueprint
from analytics_backend.resources.audit import CampaignStatus

api = Api(audit_blueprint)

api.add_resource(CampaignStatus, '/campaign_status')
