import logging
from flask import Flask, jsonify, request
from flask_restful import reqparse, abort, Resource, Api

app = Flask(__name__)
api = Api(app)


file_handler = logging.FileHandler('/home/anvaya/Desktop/error.log')
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)


@app.route('/hello', methods=['GET'])
def api_hello():
    app.logger.info('informing')
    app.logger.warning('warning')
    app.logger.error('screaming bloody murder!')

    return "check your logs\n"

if __name__ == "__main__":
    app.run(debug=True)


# #
# #
# #
#
#
# from flask import Flask
# import logging
# from logging.handlers import RotatingFileHandler
#
#
# app = Flask(__name__)
#
# @app.route("/log")
# def logTest():
#     app.logger.warning('testing warning log')
#     app.logger.error('testing error log')
#     app.logger.info('testing info log')
#     return "Code Handbook !! Log testing."
#
# if __name__ == "__main__":
#     logHandler = RotatingFileHandler('/home/anvaya/Desktop/error.log', maxBytes=1000, backupCount=1)
#
#     # set the log handler level
#     logHandler.setLevel(logging.INFO)
#
#     # set the app logger level
#     app.logger.setLevel(logging.INFO)
#
#     app.logger.addHandler(logHandler)
#     app.run()
#
#
# """
# A sample Flask-Restful app that logs exceptions and renders them as
# JSON documents.
# """
# from flask import Flask
# from flask_restful import Api, Resource
# from flask_restful.representations.json import output_json
# import logging
#
#
# app = Flask(__name__)
#
#
# class Service(Api):
#     def handle_error(self, e):
#         # Attach the exception to itself so Flask-Restful's error handler
#         # tries to render it.
#         if not hasattr(e, 'data'):
#             e.data = e
#
#         return super(Service, self).handle_error(e)
#
#
# api = Service(app)
#
#
# class Thing(Resource):
#     """A broken thing."""
#     def get(self):
#         raise TypeError('uh oh!')
#
#
# api.add_resource(Thing, '/')
#
#
# @api.representation('application/json')
# def output_json_exception(data, code, *args, **kwargs):
#     """Render exceptions as JSON documents with the exception's message."""
#     if isinstance(data, Exception):
#         data = {'status': code, 'message': str(data)}
#
#     return output_json(data, code, *args, **kwargs)
#
#
# # Logging
# handler = logging.StreamHandler()
# handler.setLevel(logging.INFO)
# app.logger.addHandler(handler)
#
#
# if __name__ == '__main__':
#     app.run()
