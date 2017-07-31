"""Database interaction for sqls"""
from sqlalchemy import text

from pika_analytics import db
from .serializers import object_as_dict


def run_generic_sql(sql_string,values =()):
    result = db.engine.execute(sql_string,values)
    return result

        # parse sql string
    # execute sql
    # return if a select query (as of not thats the only scenario)
