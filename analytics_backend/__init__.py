import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from celery import Celery

from config import config, Config
#from auth_jwt import jwt

# Flask extensions
db = SQLAlchemy()
# socketio = SocketIO()
# celery = Celery(__name__,
#                 broker=os.environ.get('CELERY_BROKER_URL', 'redis://'),
#                 backend=os.environ.get('CELERY_BROKER_URL', 'redis://'))

# Import models so that they are registered with SQLAlchemy

#Celery Initialization
#this has to be moved to flask app factory

celery_app = Celery(__name__, broker=Config.CELERY_BROKER_URL,backend=Config.CELERY_RESULT_BACKEND)



from analytics_backend.models.datamart import *

# # Import celery task so that it is registered with the Celery workers
# from .tasks import run_flask_request  # noqa

# # Import Socket.IO events so that they are registered with Flask-SocketIO
# from . import events  # noqa


def create_app(config_name=None, main=True):
    if config_name is None:
        config_name = os.environ.get('PA_CONFIG', 'development')
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    #jwt.init_app(app)

    # Initialize flask extensions
    db.init_app(app)
    # jwt.init_app(app)

    #Logging Test

    # ////////// **** FOLLOWING KEPT FOR FUTURE **** \\\\\\\\\\\\\\\\\\\\\\\\\\
    if main:
        pass
        # Initialize socketio server and attach it to the message queue, so
        # that everything works even when there are multiple servers or
        # additional processes such as Celery workers wanting to access
        # Socket.IO

        # socketio.init_app(app,
        #                   message_queue=app.config['SOCKETIO_MESSAGE_QUEUE'])
    else:
        pass
        # Initialize socketio to emit events through through the message queue
        # Note that since Celery does not use eventlet, we have to be explicit
        # in setting the async mode to not use it.
    #     socketio.init_app(None,
    #                       message_queue=app.config['SOCKETIO_MESSAGE_QUEUE'],
    #                       async_mode='threading')
    # celery.conf.update(config[config_name].CELERY_CONFIG)


    # Register web application routes
    # from .rtlytics import main as main_blueprint
    # app.register_blueprint(main_blueprint)

    # Register API routes


    from analytics_backend.api import report_api, task_api,audit_blueprint
    app.register_blueprint(report_api, url_prefix = '/reports')
    app.register_blueprint(task_api, url_prefix = '/realtime')
    app.register_blueprint(audit_blueprint,url_prefix='/audit')

    #update celery configuration from app config
    celery_app.conf.update(config)

    return app
