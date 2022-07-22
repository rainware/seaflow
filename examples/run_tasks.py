import os
import sys
import django
from celery import Celery
from django.db import connection


def truncate():
    with connection.cursor() as cursor:
        cursor.execute('truncate table seaflow_step_to_step')
        cursor.execute('truncate table seaflow_step_to_task')
        cursor.execute('truncate table seaflow_task')
        cursor.execute('truncate table seaflow_step')
        cursor.execute('truncate table seaflow_task_to_step')
        cursor.execute('truncate table seaflow_task_to_task')
        cursor.execute('truncate table seaflow_log')


def wait_for_task(t):
    while t.model.state == TaskStates.PROCESSING:
        t.reload()


def setup():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
    django.setup()

    # celery app definition
    app = Celery()
    app.config_from_object('django.conf:settings', namespace='CELERY')

    # set celery app for seaflow
    seaflow.set_celery_app(app)
    seaflow.autodiscover_actions(packages=['examples'], related_name='actions', force=True)


if __name__ == '__main__':
    import seaflow
    setup()
    # truncate()
    from seaflow.base import Seaflow, TaskConfig, CallbackConfig, StepConfig, time
    from seaflow.consts import TaskStates, StepStates
    from seaflow.models import Step
    task = Seaflow.create_task(dag_name='Root-x',
                               inputs={'n': 1},
                               context={'creator': 'rainware'},
                               config=TaskConfig(
                                   countdown=1,
                                   callback=CallbackConfig(
                                       is_async=True,
                                       func='examples.callbacks.on_callback'
                                   )
                               ),
                               tasks_config={
                                   'A': TaskConfig(
                                       countdown=1,
                                       callback=CallbackConfig(
                                           is_async=True,
                                           func='examples.callbacks.on_callback'
                                       )
                                   ),
                                   'D': TaskConfig(
                                       countdown=1,
                                       callback=CallbackConfig(
                                           is_async=True,
                                           func='examples.callbacks.on_callback'
                                       )
                                   )
                               },
                               steps_config={
                                   'd1': StepConfig(
                                       countdown=1,
                                       max_retries=2,
                                       retry_countdown=1,
                                       callback=CallbackConfig(
                                           is_async=True,
                                           func='examples.callbacks.on_callback'
                                       )
                                   )
                               })

    task.apply()
    i = 0
    while True:
        if i >= 30:
            sys.exit(1)
        rs = Step.objects.filter(node__name='a4', state=StepStates.PUBLISH).order_by('-id')[:1]
        if rs:
            external_a4 = rs[0]
            Seaflow.dispatch_external_step(external_a4.id)
            Seaflow.finish_external_step(external_a4.id)
            break
        i += 1
        time.sleep(1)
    wait_for_task(task)
    print('succeed.')
