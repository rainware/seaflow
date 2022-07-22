import os
import django
from django.core import management


if __name__ == '__main__':
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
    django.setup()

    # create db tables
    management.call_command('migrate', 'seaflow')
