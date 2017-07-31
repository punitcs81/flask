from flask_restful import Api


from analytics_backend.api import task_api as task_blueprint
from analytics_backend.resources.events import AddNumbers,Status,Fib,PushEvents,Test

api = Api(task_blueprint)

api.add_resource(AddNumbers, '/add')
api.add_resource(Fib, '/fib')
api.add_resource(Status, '/status')
api.add_resource(PushEvents, '/pushevents')
api.add_resource(Test , '/test')



