import pandas as pd
from sqlalchemy import exc

from analytics_backend import db
from analytics_backend.utils.date_time import convert_date_format
import constants

import config

current_config = config.DevelopmentConfig()


class ParamsAppModel(db.Model):
    __tablename__ = 'ref_ParamApp'

    id = db.Column(db.BIGINT, primary_key=True)
    sys_created_date = db.Column(db.DateTime)
    report_date = db.Column(db.DateTime)
    prev_lower_bound = db.Column(db.DateTime)
    prev_upper_bound = db.Column(db.DateTime)
    lower_bound = db.Column(db.DateTime)
    upper_bound = db.Column(db.DateTime)
    business_account_id = db.Column(db.BIGINT)
    param_name = db.Column(db.String(255))
    param_data_type = db.Column(db.String(255))
    param_name_cd = db.Column(db.String(255))
    ts_granularity = db.Column(db.String(30))

    def __init__(self, param_name):
        self.param_name = param_name

    def __repr__(self):
        return '<ParamAppModel For %r>' % self.param_name

    @classmethod
    def get_date_range(cls, business_account_id, param_name_cd):

        try:
            query = ParamsAppModel.query.filter_by(param_name_cd=param_name_cd, business_account_id=business_account_id,
                                                   param_data_type='date')
            result = query.first()
        except exc.SQLAlchemyError as e:
            return "Database Exception When Redeaing Param App table", e

        return {"lower_bound": convert_date_format(result.lower_bound, current_config.REPORT_DB_DATE_FORMAT,
                                                   current_config.PIKA_AN_DATE_FORMAT),
                "upper_bound": convert_date_format(result.upper_bound, current_config.REPORT_DB_DATE_FORMAT,
                                                   current_config.PIKA_AN_DATE_FORMAT),
                "prev_lower_bound": convert_date_format(result.prev_lower_bound, current_config.REPORT_DB_DATE_FORMAT,
                                                        current_config.PIKA_AN_DATE_FORMAT),
                "prev_upper_bound": convert_date_format(result.prev_upper_bound, current_config.REPORT_DB_DATE_FORMAT,
                                                        current_config.PIKA_AN_DATE_FORMAT),
                "report_date": convert_date_format(str(result.report_date), current_config.REPORT_DB_DATETIME_FORMAT,
                                                   current_config.PIKA_AN_DATE_FORMAT)
                }

    @classmethod
    def get_date_param_list(cls):
        query = db.session.query(ParamsAppModel.param_name_cd.distinct().label('date_param_cd'))
        return [row.date_param_cd for row in query.all()]


    #,row.upper_bound,row.prev_lower_bound,row.prev_upper_bound,row.param_name


    @classmethod
    def get_param_data(cls, business_account_id):

        lower_bound=[]
        try:
            for row in db.session.query(ParamsAppModel).all():

                return (row.lower_bound,row.upper_bound,row.prev_lower_bound,row.prev_upper_bound,row.param_name)
                #lower_bound.append(row.lower_bound)
            #print(lower_bound)
        except exc.SQLAlchemyError as e:
            return "Database Exception When Redeaing Param App table", e

        # return {"lower_bound": convert_date_format(result.lower_bound, current_config.REPORT_DB_DATE_FORMAT,
        #                                            current_config.PIKA_AN_DATE_FORMAT),
        #         "upper_bound": convert_date_format(result.upper_bound, current_config.REPORT_DB_DATE_FORMAT,
        #                                            current_config.PIKA_AN_DATE_FORMAT),
        #         "prev_lower_bound": convert_date_format(result.prev_lower_bound, current_config.REPORT_DB_DATE_FORMAT,
        #                                                 current_config.PIKA_AN_DATE_FORMAT),
        #         "prev_upper_bound": convert_date_format(result.prev_upper_bound, current_config.REPORT_DB_DATE_FORMAT,
        #                                                 current_config.PIKA_AN_DATE_FORMAT),
        #         "report_date": convert_date_format(str(result.report_date), current_config.REPORT_DB_DATETIME_FORMAT,
        #                                            current_config.PIKA_AN_DATE_FORMAT)
        #         }

class ConsumerTagsModel(db.Model):
    __tablename__ = 'ref_consumer_tags'

    s_key = db.Column(db.BIGINT, primary_key=True)
    tag_id = db.Column(db.BIGINT)
    tag_type = db.Column(db.String(255))
    tag_name = db.Column(db.String(255))
    tag_value = db.Column(db.String(255))
    time_zone = db.Column(db.String(255))
    isActive = db.Column(db.INT)
    sys_created_date = db.Column(db.DateTime)

    @classmethod
    def get_tags(cls, tag_type=None):
        try:
            if tag_type is None:
                result = db.session.query(cls.tag_type,
                                          cls.tag_name, cls.tag_value) \
                    .group_by(cls.tag_type, \
                              cls.tag_name, cls.tag_value).all()
            else:

                result = (db.session.query(ConsumerTagsModel.tag_type, cls.tag_name, cls.tag_value)
                          .filter(cls.tag_type == tag_type)) \
                    .group_by(cls.tag_type, \
                              cls.tag_name, cls.tag_value).all()

        except Exception as e:
            raise e
        return result

    @classmethod
    def create_customer_segment_combinations(cls, tag_names: list = None):

        segs_df = pd.DataFrame(cls.get_tags(tag_type=constants.TAGS_CONSUMER_SEGMENT_TYPE))
        if segs_df is None:
            return pd.DataFrame({})

        if tag_names is not None:
            print(segs_df)
            segs_df = segs_df[segs_df.tag_name.isin(tag_names)]
            print(segs_df)

        final_df = pd.DataFrame([1])
        final_df.columns = ('key',)

        for i_tag_name in segs_df.tag_name.unique():
            filter_df = pd.DataFrame(segs_df[segs_df.tag_name == i_tag_name]['tag_value']).copy()
            filter_df.columns = (i_tag_name,)
            filter_df['key'] = ([1] * filter_df.shape[0])
            final_df = pd.merge(final_df, filter_df, how='inner', on='key')
        final_columns = set(final_df.columns)
        final_columns.remove('key')
        return final_df[list(final_columns)]  # Returns DataDrame - Unlike other model objects


class InteractionTypeModel(db.Model):
    __tablename__ = 'ref_interaction_type'

    s_key = db.Column(db.BIGINT, primary_key=True)
    interaction_description = db.Column(db.String(255))
    interaction_type = db.Column(db.String(255))
    interaction_type_category = db.Column(db.String(255))
    time_zone = db.Column(db.String(255))
    isActive = db.Column(db.INT)
    sys_created_date = db.Column(db.DateTime)
    sys_updated_date = db.Column(db.DateTime)

    @classmethod
    def get_all_combination(self):
        result = db.session.query(InteractionTypeModel.interaction_type, InteractionTypeModel.interaction_type_category) \
            .group_by(InteractionTypeModel.interaction_type, InteractionTypeModel.interaction_type_category).all()
        return result


class DateModel(db.Model):
    __tablename__ = 'dim_date'

    s_key = db.Column(db.BIGINT, primary_key=True)
    calendar_quarter = db.Column(db.BIGINT)
    calendar_year = db.Column(db.BIGINT)
    calendar_year_month = db.Column(db.String(255))
    calendar_year_qtr = db.Column(db.String(255))
    date_name = db.Column(db.String(255))
    day_name_of_week = db.Column(db.String(255))
    day_of_month = db.Column(db.BIGINT)
    day_of_week = db.Column(db.BIGINT)
    day_of_year = db.Column(db.BIGINT)
    fiscal_month_of_year = db.Column(db.BIGINT)
    fiscal_quarter = db.Column(db.BIGINT)
    fiscal_year = db.Column(db.BIGINT)
    fiscal_year_month = db.Column(db.String(255))
    fiscal_year_qtr = db.Column(db.String(255))
    full_date = db.Column(db.DateTime)
    is_last_day_of_month = db.Column(db.String(255))
    Monday_Date_Of_Week = db.Column(db.String(255))
    month_name = db.Column(db.String(255))
    month_of_year = db.Column(db.BIGINT)
    Sunday_Date_Of_Week = db.Column(db.String(255))
    week_of_year = db.Column(db.BIGINT)
    weekday_weekend = db.Column(db.String(255))
    timezone = db.Column(db.String(255))

    @classmethod
    def get_days_between_dates(cls, start_date, end_date):
        try:
            date_series = db.session.query(DateModel.full_date, DateModel.Monday_Date_Of_Week,
                                           DateModel.day_of_month).filter(
                DateModel.full_date.between(start_date, end_date))

        except Exception as e:
            raise e

        return date_series.all()
