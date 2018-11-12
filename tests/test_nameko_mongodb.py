import pytest
from mock import Mock
from nameko.testing.services import dummy, entrypoint_hook
from nameko.containers import ServiceContainer, WorkerContext
from pymongo import MongoClient
from pymongo.database import Database

from nameko_mongodb import MongoDatabase


class DummyService(object):
    name = 'dummy_service'

    database = MongoDatabase()

    @dummy
    def insert_one(self, document):
        res = self.database.test_collection.insert_one(document)
        return res

    @dummy
    def find_one(self, query):
        doc = self.database.test_collection.find_one(query)
        return doc
        
    @dummy
    def corrupted_method(self):
        return 1/0


@pytest.fixture
def config(db_url):
    return {
        'MONGODB_CONNECTION_URL': db_url
    }


@pytest.fixture
def container(config):
    return Mock(spec=ServiceContainer, config=config, service_name='dummy_service')


@pytest.fixture
def database(container):
   return MongoDatabase().bind(container, 'database')


def test_setup(database):
    database.setup()
    assert isinstance(database.client, MongoClient)
    assert isinstance(database.database, Database)


def test_stop(database):
    database.setup()
    assert database.client

    database.stop()
    assert database.client is None


def test_get_dependency(database):
    database.setup()

    worker_ctx = Mock(spec=WorkerContext)
    db = database.get_dependency(worker_ctx)
    assert isinstance(db, Database)


def test_end_to_end(db_url, container_factory):
    config = {
        'MONGODB_CONNECTION_URL': db_url
    }

    container = container_factory(DummyService, config)
    container.start()

    with entrypoint_hook(container, 'insert_one') as insert_one:
        insert_one({'toto': 'titi'})

    with entrypoint_hook(container, 'find_one') as find_one:
        doc = find_one({'toto': 'titi'})
        assert doc['toto'] == 'titi'
        
    with entrypoint_hook(container, 'corrupted_method') as corrupted_method:
        try:
            corrupted_method()
        except:
            pass

    client = MongoClient(config['MONGODB_CONNECTION_URL'])
    db = client.dummy_service
    logs = db.logging.find({})

    for r in logs:
        if r['method_name'] == 'find_one':
            assert r['status'] == 'SUCCESS'
        elif r['method_name'] == 'insert_one':
            assert r['status'] == 'SUCCESS'
        elif r['method_name'] == 'corrupted_method':
            assert r['status'] == 'FAILED'
            assert r['exception']
