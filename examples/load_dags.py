import json
import os
import django


if __name__ == '__main__':
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
    django.setup()

    from seaflow.base import Seaflow
    with open('definitions/dags.json') as f:
        [Seaflow.load_dag(d) for d in json.loads(f.read())]

