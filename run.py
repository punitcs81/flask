import logging
# import sys
from analytics_backend import create_app, db

app = create_app('development')
db.init_app(app)


# if app.config['DEBUG']:
#     @app.before_first_request
#     def create_tables():
#         db.create_all()
#
# app.run(
#     host=app.config.get('HOST', '0.0.0.0'),
#     port=app.config.get('PORT', 5006)
# )
# logger = logging.getLogger("your_package_name")
# logger.setLevel(logging.INFO)
# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# formatter = logging.Formatter(
#     """%(levelname)s in %(module)s [%(pathname)s:%(lineno)d]:\n%(message)s"""
# )
# ch.setFormatter(formatter)
# logger.addHandler(ch)
#
# logging.basicConfig(filename='/home/anvaya/Desktop/error.log', level=logging.DEBUG)
#
#
# file_handler = logging.FileHandler('/home/anvaya/Desktop/error.log')
# app.logger.addHandler(file_handler)
# app.logger.setLevel(logging.INFO)


app.run(
    host='0.0.0.0',
    port=5000
)


#app.run(debug=True)