celery_app = None

default_app_config = 'seaflow.apps.SeaflowConfig'

def set_celery_app(app):
    global celery_app
    celery_app = app
    celery_app.autodiscover_tasks(packages=['seaflow'], related_name='tasks')


def autodiscover_actions(*args, **kwargs):
    celery_app.autodiscover_tasks(*args, **kwargs)
