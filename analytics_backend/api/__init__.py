from flask import Blueprint


report_api = Blueprint('report_api',__name__+'report')
# filter_api = Blueprint('filter_api', __name__+"_filter")
# analytics_api = Blueprint('api', __name__+"_analytics")
task_api = Blueprint('realtime',__name__+"realtime")
audit_blueprint = Blueprint('audit',__name__+"audit")

from analytics_backend.api import consumers
from analytics_backend.api import realtime
from analytics_backend.api import audit
from analytics_backend.api import rewards
from analytics_backend.api import feedbacks
from analytics_backend.api import campaigns


