import argparse

import celery
import celery.states
import celery.events
import collections
import logging
import prometheus_client
import signal
import sys
import threading
import time
import os

import redis
from celery.events.state import Task
from collections import defaultdict
from redis import ResponseError

DEFAULT_BROKER = os.environ.get('BROKER_URL', 'redis://127.0.0.1:6379/0')
DEFAULT_ADDR = os.environ.get('DEFAULT_ADDR', '0.0.0.0:8888')

LOG_FORMAT = '[%(asctime)s] %(name)s:%(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

TASKS = prometheus_client.Gauge(
    'celery_tasks', 'Number of tasks per state',
    ['state']
)
TASKS_NAME = prometheus_client.Gauge(
    'celery_tasks_by_name', 'Number of tasks per state and name',
    ['state', 'name']
)
TASK_DURATIONS = prometheus_client.Histogram(
    'celery_task_durations', 'Task durations by state and name',
    ['state', 'name'],
    buckets=(0.1, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, float('inf'))
)
TASK_EXCEPTIONS = prometheus_client.Counter(
    'celery_task_exceptions', 'Number of exceptions per name',
    ['exception', 'name']
)

WORKERS = prometheus_client.Gauge(
    'celery_workers', 'Number of alive workers'
)

QUEUE_LENGTHS = prometheus_client.Gauge(
    'celery_queue_lengths', 'Lengths of celery queues',
    ['name']
)


class MonitorThread(threading.Thread):
    """
    MonitorThread is the thread that will collect the data that is later
    exposed from Celery using its eventing system.
    """

    def __init__(self, app=None, *args, **kwargs):
        self._app = app
        self.log = logging.getLogger('monitor')
        self._mutex = threading.Lock()
        self._tasks_dict = defaultdict(dict)
        self._known_states = set()
        self._known_states_names = set()
        super().__init__(*args, **kwargs)

    def run(self):  # pragma: no cover
        self._monitor()

    def _process_event(self, evt: dict):
        # Events might come in in parallel
        with self._mutex:
            if celery.events.group_from(evt['type']) == 'task':
                evt_state = evt['type'][5:]
                state = celery.events.state.TASK_EVENT_TO_STATE[evt_state]
                self._collect_tasks(evt, state)

    def _collect_tasks(self, evt: dict, state: str):
        if state in celery.states.READY_STATES:
            self._incr_ready_task(evt, state)
            # delete evt from state so state won't increase in memory
            self._delete_evt_from_state(evt)
        else:
            # add event to list of in-progress tasks
            evt_dict = self._tasks_dict[evt['uuid']]
            evt_dict['state'] = state
            if 'name' in evt:
                evt_dict['name'] = evt['name']
            if 'local_received' in evt:
                evt_dict['started_at'] = evt['local_received']

        self._collect_unready_tasks()

    def _delete_evt_from_state(self, evt: dict):
        self._tasks_dict.pop(evt['uuid'], None)

    def _incr_ready_task(self, evt: dict, state: str):
        TASKS.labels(state=state).inc()

        task_state = self._tasks_dict.get(evt['uuid'], {})
        task_name = task_state.get('name', 'unknown')

        TASKS_NAME.labels(state=state, name=task_name).inc()

        exc = evt.get('exception')
        if exc:
            TASK_EXCEPTIONS.labels(exception=exc.split('(')[0], name=task_name).inc()

        task_started = task_state.get('started_at')
        runtime = evt['local_received'] - task_started if task_started else None
        runtime = evt.get('runtime', runtime)
        if runtime is not None:
            TASK_DURATIONS.labels(state=state, name=task_name).observe(runtime)

    def _collect_unready_tasks(self):
        # count unready tasks by state
        cnt = collections.Counter(t['state'] for t in self._tasks_dict.values())
        self._known_states.update(cnt.elements())
        for task_state in self._known_states:
            TASKS.labels(state=task_state).set(cnt[task_state])

        # count unready tasks by state and name
        cnt = collections.Counter(
            (t['state'], t['name']) for t in self._tasks_dict.values() if 'name' in t)
        self._known_states_names.update(cnt.elements())
        for state, name in self._known_states_names:
            TASKS_NAME.labels(state=state,name=name,).set(cnt[(state, name)])

    def _monitor(self):  # pragma: no cover
        while True:
            try:
                with self._app.connection() as conn:
                    recv = self._app.events.Receiver(conn, handlers={'*': self._process_event})
                    recv.capture(limit=None, timeout=None, wakeup=True)
                    self.log.info('Connected to broker')
            except Exception:
                self.log.exception('Queue connection failed')
                time.sleep(5)


class WorkerMonitoringThread(threading.Thread):
    celery_ping_timeout_seconds = 5
    periodicity_seconds = 5

    def __init__(self, app=None, *args, **kwargs):
        self._app = app
        self.log = logging.getLogger('workers-monitor')
        super().__init__(*args, **kwargs)

    def run(self):  # pragma: no cover
        while True:
            self.update_workers_count()
            time.sleep(self.periodicity_seconds)

    def update_workers_count(self):
        try:
            WORKERS.set(len(self._app.control.ping(
                timeout=self.celery_ping_timeout_seconds)))
        except Exception:  # pragma: no cover
            self.log.exception('Error while pinging workers')


class QueueLenghtMonitoringThread(threading.Thread):
    periodicity_seconds = 5

    def __init__(self, redis_client, *args, **kwargs):
        self.redis_client = redis_client
        self.known_not_queue_names = set()
        self.log = logging.getLogger('lengths-monitor')
        super().__init__(*args, **kwargs)

    def run(self):  # pragma: no cover
        while True:
            self.update_lengths()
            time.sleep(self.periodicity_seconds)

    def update_lengths(self):
        try:
            for name, length in self.get_queue_lengths().items():
                QUEUE_LENGTHS.labels(name=name).set(length)
        except Exception:  # pragma: no cover
            self.log.exception('Error while updating lengths')

    def get_queue_lengths(self):
        result = {}

        queue_keys = self.redis_client.keys('_kombu.binding.*')
        possible_queue_names = [k.rsplit(b'.', 1)[1].decode() for k in queue_keys]

        for name in possible_queue_names:
            if name in self.known_not_queue_names:
                continue

            try:
                queue_len = self.redis_client.llen(name)
                result[name] = queue_len
            except ResponseError:
                self.known_not_queue_names.add(name)

        return result


def start_httpd(addr):  # pragma: no cover
    """
    Starts the exposing HTTPD using the addr provided in a separate
    thread.
    """
    host, port = addr.split(':')
    logging.info('Starting HTTPD on {}:{}'.format(host, port))
    prometheus_client.start_http_server(int(port), host)


def shutdown(signum, frame):  # pragma: no cover
    """
    Shutdown is called if the process receives a TERM signal. This way
    we try to prevent an ugly stacktrace being rendered to the user on
    a normal shutdown.
    """
    logging.info('Shutting down')
    sys.exit(0)


def main():  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--broker', dest='broker', default=DEFAULT_BROKER,
        help='URL to the Celery broker. Defaults to {}'.format(DEFAULT_BROKER))
    parser.add_argument(
        '--addr', dest='addr', default=DEFAULT_ADDR,
        help='Address the HTTPD should listen on. Defaults to {}'.format(
            DEFAULT_ADDR))
    opts = parser.parse_args()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    app = celery.Celery(broker=opts.broker)
    redis_client = redis.Redis.from_url(opts.broker)

    t = MonitorThread(app=app, daemon=True)
    t.start()
    w = WorkerMonitoringThread(app=app, daemon=True)
    w.start()
    q = QueueLenghtMonitoringThread(redis_client=redis_client, daemon=True)
    q.start()
    start_httpd(opts.addr)
    t.join()
    w.join()
    q.join()


if __name__ == '__main__':  # pragma: no cover
    main()
