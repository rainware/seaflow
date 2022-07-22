celery_app = None


def set_celery_app(app):
    global celery_app
    celery_app = app
