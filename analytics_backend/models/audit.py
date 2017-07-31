from analytics_backend import db
from analytics_backend.utils.date_time import convert_date_format


class AuditCampaignStatus(db.Model):
    __tablename__ = 'aud_campaign_status'

    id = db.Column(db.INT, primary_key=True)
    campaign_id = db.Column(db.BIGINT)
    sys_created_date = db.Column(db.DateTime)
    sys_updated_date = db.Column(db.DateTime)
    status = db.Column(db.String(30))
    business_account_id = db.Column(db.BIGINT)

    def __init__(self, campaign_id):
        self.campaign_id = campaign_id

    def update_status(self, status):
        self.status = status
