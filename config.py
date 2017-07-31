import os
import base64
import datetime

basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    DEBUG = False
    TESTING = False
    SECRET_KEY_OS = os.environ.get('PA_SECRET_KEY',
                                'w4MujE4x')
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'PA_DATABASE_URL', 'mysql+pymysql://qadb:qadb@104.196.135.227/pika_dm')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    REQUEST_STATS_WINDOW = 15
    #//////////DATE FORMATES \\\\\\\\\\\\\\
    REPORT_DB_DATE_FORMAT ='%Y-%m-%d'
    REPORT_DB_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    PIKA_AN_DATE_FORMAT = '%Y-%m-%d'

    #PIKA_AN_LOGGING_DIR = os.environ.get('PA_LOGGING_DIR', './log')

    # CELERY_BROKER_URL = 'amqp://pika:anvaya@104.155.193.196/'
    # CELERY_RESULT_BACKEND = 'db+mysql+pymysql://palas:palas@107.167.186.9/celery'
    # CELERY_DEFAULT_QUEUE='palas-test'

    CELERY_BROKER_URL = 'amqp://pika:anvaya@104.155.193.196:5672//'
    CELERY_RESULT_BACKEND = 'db+mysql+pymysql://pika_dev:anvaya@107.167.186.9/punit'
    CELERY_DEFAULT_QUEUE = 'testing'
    SECRET_KEY = base64.b64decode(SECRET_KEY_OS)
    JWT_VERIFY_CLAIMS= ['signature', 'exp', 'iat']
    JWT_REQUIRED_CLAIMS= ['exp', 'iat']
    JWT_AUTH_HEADER_PREFIX='Bearer'

class DevelopmentConfig(Config):
    CELERY_BROKER_URL = 'amqp://pika:anvaya@104.155.193.196:5672//'
    CELERY_RESULT_BACKEND = 'db+mysql+pymysql://pika_dev:anvaya@107.167.186.9/punit'
    CELERY_DEFAULT_QUEUE='testing'

    DEBUG = True
    # this is to test with an old token provided by Java system
    JWT_LEEWAY=datetime.timedelta(999999999)



class ProductionConfig(Config):
    pass


class TestingConfig(Config):
    TESTING = True
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'PA_DATABASE_URL', 'mysql+pymysql://palas:palas@107.167.186.9/pika_dm')
    CELERY_CONFIG = {'CELERY_ALWAYS_EAGER': True}
    SOCKETIO_MESSAGE_QUEUE = None


config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig
}

