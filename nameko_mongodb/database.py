from weakref import WeakKeyDictionary
import datetime

from pymongo import MongoClient
from nameko.extensions import DependencyProvider
from nameko.exceptions import safe_for_serialization


class MongoDatabase(DependencyProvider):

    default_connection_uri = 'mongodb://localhost:27017'

    def __init__(self, result_backend=False,
                 on_before_setup=None, on_after_setup=None,
                 on_before_stop=None, on_after_stop=None,
                 on_before_worker_setup=None, on_after_worker_setup=None,
                 on_before_worker_result=None, on_after_worker_result=None):
        self.result_backend = result_backend
        self.logs = WeakKeyDictionary()
        self.client = None
        self.database = None

        # Callbacks
        self.on_before_setup = on_before_setup
        self.on_after_setup = on_after_setup

        self.on_before_stop = on_before_stop
        self.on_after_stop = on_after_stop

        self.on_before_worker_setup = on_before_worker_setup
        self.on_after_worker_setup = on_after_worker_setup

        self.on_before_worker_result = on_before_worker_result
        self.on_after_worker_result = on_after_worker_result

    @property
    def db(self):
        return self.database

    def _run_callback(self, name, **kwargs):
        assert hasattr(self, name), "Callback {} does not exist".format(name)

        callback = getattr(self, name)
        if callback is not None:
            callback(self, **kwargs)
        
    def setup(self):
        self._run_callback('on_before_setup')
        config = self.container.config
        db_name = config.get('MONGODB_DB_NAME', self.container.service_name)
        conn_uri = config.get('MONGODB_CONNECTION_URL', self.default_connection_uri)

        params = {}
        if config.get('MONGODB_USER'):
            if config.get('MONGODB_USER'):
                params['username'] = config.get('MONGODB_USER')
            if config.get('MONGODB_PASSWORD'):
                params['password'] = config.get('MONGODB_PASSWORD')
            if config.get('MONGODB_AUTHENTICATION_BASE'):
                params['authSource'] = config.get('MONGODB_AUTHENTICATION_BASE')
            if config.get('MONGODB_AUTH_MECHANISM'):
                params['authSource'] = config.get('MONGODB_AUTH_MECHANISM')

        self.client = MongoClient(conn_uri, **params)
        self.database = self.client[db_name]

        if self.result_backend:
            self.db.logging.create_index('start', expireAfterSeconds=24*60*60)
            self.db.logging.create_index('call_id')

        self._run_callback('on_after_setup')

    def stop(self):
        self._run_callback('on_before_stop')

        self.client.close()
        self.client = None

        self._run_callback('on_after_stop')

    def get_dependency(self, worker_ctx):
        return self.db
    
    def worker_setup(self, worker_ctx):
        self._run_callback('on_before_worker_setup', worker_ctx=worker_ctx)

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

        self._run_callback('on_after_worker_setup', worker_ctx=worker_ctx)

    def worker_result(self, worker_ctx, result=None, exc_info=None):
        self._run_callback('on_before_worker_result', worker_ctx=worker_ctx, result=result, exc_info=exc_info)

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

        self._run_callback('on_after_worker_result', worker_ctx=worker_ctx, result=result, exc_info=exc_info)
