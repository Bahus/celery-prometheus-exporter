# celery-prometheus-exporter
Exporter for Celery related metrics in
order to get picked up by Prometheus.

So far it provides access to the following metrics:

* ``celery_tasks`` exposes the number of tasks currently known to the queue
  grouped by ``state`` (RECEIVED, STARTED, ...).
* ``celery_tasks_by_name`` exposes the number of tasks currently known to the queue
  grouped by ``name`` and ``state``.
* ``celery_task_durations`` exposes the task execution durations grouped by ``name`` and ``state``.
* ``celery_task_exceptions`` exposes the task failure exceptions durations grouped by ``name`` and ``exception``.
 
* ``celery_workers`` exposes the number of currently probably alive workers

* ``celery_queue_lengths`` exposes the lengths of celery queues


How to use
==========

```bash
  $ python setup.py install
  $ celery-exporter
  [2018-04-06 13:08:35,766] root:INFO: Starting HTTPD on 0.0.0.0:8888
```

Celery workers have to be configured to send task-related events:
http://docs.celeryproject.org/en/latest/userguide/configuration.html#worker-send-task-events.

By default, the HTTPD will listen at ``0.0.0.0:8888``. If you want the HTTPD
to listen to another port, use the ``--addr`` option or the environment variable
``DEFAULT_ADDR``.

By default, this will expect the broker to be available through
``redis://127.0.0.1:6379/0``. You can change it via environment variable
``DEFAULT_BROKER`` or by passing ``--broker`` option.
