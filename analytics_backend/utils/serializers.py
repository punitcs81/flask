from flask_sqlalchemy import inspect

def object_as_dict(obj):
    """can serialize row when queried from db.Model"""
    print(obj)
    print(type(obj))
    return {c.key: getattr(obj, c.key)
            for c in inspect(obj).mapper.column_attrs}


def row2dict(row):
    """can serialize rowproxy object of sqlalchemy. derived column should have alias for clean name"""
    d = {}
    for column in row.keys():
        value = str(getattr(row, column))
        d[column] = str(getattr(row, column))

    return d