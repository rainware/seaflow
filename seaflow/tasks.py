from . import celery_app
from .utils import get_func


@celery_app.task
def publish_external_step(step_id):
    from .base import SeaflowStep
    SeaflowStep.get(step_id)._publish()


@celery_app.task(bind=True)
def start_carrier_step(self, step_id):
    from .base import SeaflowStep
    SeaflowStep.get(step_id)._carry(self)


@celery_app.task
def apply_root_task(task_id):
    from .base import SeaflowTask
    SeaflowTask.get(task_id).apply(sync=True)


@celery_app.task()
def do_callback(func, event, data):
    """
    :param event: 'TASK/STEP_STATE_<TaskStates/StepStates>
    :param data: json data
    :param func: callback func
    :return:
    """

    get_func(func)(event, data)
