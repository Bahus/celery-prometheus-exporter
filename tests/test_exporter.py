import uuid
from time import time

import celery
import celery.states
from celery.events import Event
import pytest
from prometheus_client import REGISTRY
from redis import ResponseError

from celery_prometheus_exporter.main import (
    WorkerMonitoringThread, MonitorThread, QueueLenghtMonitoringThread
)


@pytest.fixture
def app():
    return celery.Celery(broker='memory://', backend='cache+memory://')


@pytest.fixture(autouse=True)
def cleanup_registry():
    for collector in REGISTRY._names_to_collectors.values():
        collector._metrics = {}


class TestWorkerMonitoringThread:

    @pytest.mark.parametrize(
        'ping_result, workers_count',
        (
                ([], 0),
                ([0], 1),
                ([0, 0], 2),
        )
    )
    def test_workers_count(self, ping_result, workers_count, mocker, app):
        monitor = WorkerMonitoringThread(app=app)

        mock_ping = mocker.patch.object(app.control, 'ping')
        mock_ping.return_value = ping_result

        monitor.update_workers_count()
        assert REGISTRY.get_sample_value('celery_workers') == workers_count


class TestMonitorThread:
    TASK_NAME = 'my_task'

    def assert_task_states(self, states, cnts):
        for state in states:
            assert REGISTRY.get_sample_value('celery_tasks', labels={'state': state}) in cnts
            task_by_name_label = {'state': state, 'name': self.TASK_NAME}
            assert REGISTRY.get_sample_value(
                'celery_tasks_by_name', labels=task_by_name_label) in cnts

    def assert_only_states(self, states):
        self.assert_task_states(celery.states.ALL_STATES - states, {0, None})
        self.assert_task_states(states, {1})

    def test_process_event(self, mocker, app):
        monitor = MonitorThread(app=app)

        task_uuid = uuid.uuid4()
        hostname = 'myhost'
        local_received = time()
        latency_before_started = 123.45
        runtime = 234.5

        event_kwargs = {
            'uuid': task_uuid,
            'name': self.TASK_NAME,
            'hostname': hostname,
        }

        monitor._process_event(Event(
            'task-received', **event_kwargs,
            args='()', kwargs='{}', retries=0, eta=None, clock=0, local_received=local_received)
        )
        self.assert_only_states({celery.states.RECEIVED})

        monitor._process_event(Event(
            'task-started', **event_kwargs,
            clock=1, local_received=local_received + latency_before_started)
        )
        self.assert_only_states({celery.states.STARTED})

        monitor._process_event(Event(
            'task-succeeded', **event_kwargs,
            runtime=runtime, clock=2,
            local_received=local_received + latency_before_started + runtime))
        self.assert_only_states({celery.states.SUCCESS})

    def test_no_received_event(self, app):
        monitor = MonitorThread(app=app)

        monitor._process_event(Event(
            'task-started',
            uuid=uuid.uuid4(), name=self.TASK_NAME, hostname='myhost',
            clock=1, local_received=time())
        )
        self.assert_only_states({celery.states.STARTED})

    def test_exception(self, app):
        monitor = MonitorThread(app=app)

        monitor._process_event(Event(
            'task-failed',
            uuid=uuid.uuid4(), name=self.TASK_NAME, hostname='myhost',
            clock=2, local_received=time(), exception='ZeroDivisionError()')
        )
        exc_count = REGISTRY.get_sample_value(
            'celery_task_exceptions',
            labels={'name': 'unknown', 'exception': 'ZeroDivisionError'}
        )
        assert exc_count == 1


class TestQueueLenghtMonitor:

    def get_queue_len(self, queue):
        return REGISTRY.get_sample_value('celery_queue_lengths', labels={'name': queue})

    def test_queue_length(self, mocker):
        redis_client_mock = mocker.MagicMock()
        redis_client_mock.keys.return_value = [
            b'_kombu.binding.celery',
            b'_kombu.binding.myqueue',
            b'_kombu.binding.sometechincalstuff',
        ]
        redis_client_mock.llen.side_effect = [1, 2, ResponseError(), 3, 4]

        monitor = QueueLenghtMonitoringThread(redis_client_mock)

        monitor.update_lengths()
        assert redis_client_mock.llen.call_count == 3
        assert self.get_queue_len('celery') == 1
        assert self.get_queue_len('myqueue') == 2
        assert self.get_queue_len('sometechincalstuff') is None

        monitor.update_lengths()
        assert redis_client_mock.llen.call_count == 5
        assert self.get_queue_len('celery') == 3
        assert self.get_queue_len('myqueue') == 4
        assert self.get_queue_len('sometechincalstuff') is None
