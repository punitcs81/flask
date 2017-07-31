from datetime import datetime, timezone


def validate_date_format(date_text, _format=None):
    if _format is None:
        _format = current_config.PIKA_AN_DATE_FORMAT
    try:
        datetime.strptime(date_text,_format)
    except ValueError as e:
        return False
    return True

def convert_date_format(in_date_text,in_format, out_format):

    out_date_text = datetime.strptime(in_date_text,in_format).strftime(out_format)

    return out_date_text

def get_current_db_date_time(_format='%Y-%m-%d %H:%M:%S', _timezone=timezone.utc):
    out_date_text = datetime.now(_timezone).strftime(_format)
    return out_date_text
