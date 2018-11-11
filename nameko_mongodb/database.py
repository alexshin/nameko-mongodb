from weakref import WeakKeyDictionary
import datetime

from pymongo import MongoClient
from nameko.extensions import DependencyProvider
from nameko.exceptions import safe_for_serialization


class MongoDatabase(DependencyProvider):

    default_connection_uri = 'mongodb://localhost:27017'

    def __init__(self, result_backend=False):
        self.result_backend = result_backend
        self.logs = WeakKeyDictionary()
        self.client = None
        self.database = None

    @property
    def db(self):
        return self.database
        
    def setup(self):
        config = self.container.config
        db_name = config.get('MONGODB_DB_NAME', self.container.service_name)
        conn_uri = config.get('MONGODB_CONNECTION_URL', self.default_connection_uri)

        self.client = MongoClient(conn_uri)
        self.database = self.client[db_name]

        if config.get('MONGODB_USER'):
            self.db.authenticate(
                config.get('MONGODB_USER'),
                config.get('MONGODB_PASSWORD'),
                source=config.get('MONGODB_AUTHENTICATION_BASE', db_name),
                authMechanism=config.get('MONGODB_AUTH_MECHANISM')
            )

        if self.result_backend:
            self.db.logging.create_index('start', expireAfterSeconds=24*60*60)
            self.db.logging.create_index('call_id')

    def stop(self):
        self.client.close()
        self.client = None

    def get_dependency(self, worker_ctx):
        return self.db
    
    def worker_setup(self, worker_ctx):
        if self.result_backend:
            self.logs[worker_ctx] = datetime.datetime.now()

            service_name = worker_ctx.service_name
            method_name = worker_ctx.entrypoint.method_name
            call_id = worker_ctx.call_id

            self.db.logging.insert_one(
                {
                    'call_id': call_id,
                    'service_name': service_name,
                    'method_name': method_name,
                    'status': 'PENDING',
                    'start': self.logs[worker_ctx]
                }
            )

    def worker_result(self, worker_ctx, result=None, exc_info=None):
        if self.result_backend:
            call_id = worker_ctx.call_id

            if exc_info is None:
                status = 'SUCCESS'
            else:
                status = 'FAILED'

            now = datetime.datetime.now()

            start = self.logs.pop(worker_ctx)

            self.db.logging.update_one(
                {'call_id': call_id},
                {
                    '$set': {
                        'status': status,
                        'end': now,
                        'elapsed': (now - start).seconds,
                        'exception': safe_for_serialization(exc_info) if exc_info is not None else None
                    }
                }
            )
