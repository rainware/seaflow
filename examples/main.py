import os
import django
from celery import Celery
import seaflow


# django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
django.setup()

# celery app definition
app = Celery()
app.config_from_object('django.conf:settings', namespace='CELERY')

# set celery app for seaflow
seaflow.set_celery_app(app)
seaflow.autodiscover_actions(packages=['examples'], related_name='actions', force=True)
