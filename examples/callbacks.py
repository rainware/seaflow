import json

from seaflow.utils import ComplexJSONEncoder


def on_callback(event, o):
    print('%s %s' % (event, json.dumps(o, cls=ComplexJSONEncoder)))
