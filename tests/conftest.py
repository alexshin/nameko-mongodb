import eventlet
eventlet.monkey_patch()

from nameko.containers import ServiceContainer
import pytest


def pytest_addoption(parser):
    parser.addoption(
        '--test-db-url',
        action='store',
        dest='TEST_DB_URL',
        default='mongodb://localhost:27017',
        help=(
            'DB url for testing (e.g. mongodb://mongodb:27017)'
        )
    )


@pytest.fixture
def db_url(request):
    return request.config.getoption("TEST_DB_URL")


@pytest.fixture
def container_factory():

    all_containers = []

    def make_container(service_cls, config):
        container = ServiceContainer(service_cls, config)
        all_containers.append(container)
        return container

    yield make_container

    for c in all_containers:
        try:
            c.stop()
        except:
            pass
