import importlib
import json
from copy import deepcopy

import jsonpath_rw as jsonpath

from core.contrib.seaflow.utils import ParamDefinitionException, ParamAdaptException


class ParamType(object):

    def __init__(self, name, value):
        assert isinstance(value, self.TYPES)
        self.name = name
        self.value = value

    @classmethod
    def is_valid(cls, value):
        return isinstance(value, cls.TYPES)


class Number(ParamType):
    TYPES = (int, float)
    DEFAULT = 0

    def __init__(self, name, value=None):
        value = 0 if value is None else value
        super().__init__(name, value)


class String(ParamType):
    TYPES = (str,)

    def __init__(self, name, value=None):
        value = '' if value is None else value
        super().__init__(name, value)


class Boolean(ParamType):
    TYPES = (bool,)

    def __init__(self, name, value=None):
        value = False if value is None else value
        super().__init__(name, value)


class Object(ParamType):
    TYPES = (dict,)

    def __init__(self, name, value=None):
        value = dict() if value is None else value
        super().__init__(name, value)


class Array(ParamType):
    TYPES = (list,)

    def __init__(self, name, value=None):
        value = list() if value is None else value
        super().__init__(name, value)


def get_param_class(cls_name):
    """
    """

    cls = getattr(importlib.import_module(__name__), cls_name)
    assert issubclass(cls, ParamType)
    return cls


class ParamDefinitionItem(object):
    def __init__(self):
        self.type = None
        self.required = None
        self.default = None

    @classmethod
    def from_json(cls, definition_item: dict):
        """
        :param definition_item:
        :return:
        """
        _type = get_param_class(definition_item.get('type'))
        required = definition_item.get('required', True)
        default = definition_item.get('default', None)
        assert issubclass(_type, ParamType)
        if required and default is not None:
            assert _type.is_valid(default)
        r = cls()
        r.type = _type
        r.required = required
        r.default = default

        return r

    def to_json(self):
        return {'type': self.type.__name__, 'required': self.required, 'default': self.default}


class ParamDefinition(object):
    def __init__(self):
        self.dict = {}

    def __len__(self):
        return len(self.dict)

    def add(self, name, item: ParamDefinitionItem):
        assert isinstance(item, ParamDefinitionItem)
        self.dict[name] = item

    def get(self, name):
        return self.dict[name]

    def has(self, name):
        return name in self.dict

    def keys(self):
        return self.dict.keys()

    def dump(self):
        return json.dumps(self.to_json(), ensure_ascii=False)

    @classmethod
    def from_json(cls, definition: dict):
        """
        :param definition: dict, {"paramA": "Number"}
        :return:
        """
        r = cls()
        for name, item in definition.items():
            r.add(name, ParamDefinitionItem.from_json(item))
        return r

    def to_json(self):
        """
        :return:
        """
        d = {k: v.to_json() for k, v in self.dict.items()}
        return d

    def validate(self, params):
        """
        :param params: dict
        :return:
        """
        errors = {}
        for name, item in self.dict.items():
            if not params or params.get(name) is None:
                if item.required:
                    errors[name] = 'missing required field'
            else:
                if not item.type.is_valid(params[name]):
                    errors[name] = 'invalid value, expect %s' % item.type.__name__

        return len(errors) == 0, errors


class ParamAdapter(object):
    def __init__(self):
        self.dict = {}
        self.parser_dict = {}

    def __len__(self):
        return len(self.dict)

    def parse(self, raw):
        return self.adapt(raw)

    def adapt(self, raw):
        """
        :param raw: dict, raw params
        :return:
        """

        d = {}

        raw = raw or {}
        def __recursive(parser, o, key):
            if isinstance(parser, dict):
                o[key] = {}
                for k, v in parser.items():
                    __recursive(v, o[key], k)
            else:
                try:
                    rs = parser.find(raw)
                    if rs:
                        assert len(rs) == 1, 'duplicated hits'
                        o[key] = rs[0].value
                except Exception as e:
                    raise ParamAdaptException(key).with_traceback(e.__traceback__)

        for name, item in self.parser_dict.items():
            __recursive(item, d, name)

        return d


    def dump(self):
        return json.dumps(self.dict, ensure_ascii=False)

    @classmethod
    def from_json(cls, adapter: dict):
        """
        :param adapter: dict, {"paramA": "$.a"}
        :return:
        """
        def __recursive(o):
            for k, v in o.items():
                if isinstance(v, dict):
                    __recursive(v)
                else:
                    o[k] = jsonpath.parse(v)
            return o

        r = cls()
        r.dict = adapter
        r.parser_dict = __recursive(deepcopy(adapter))

        return r

    def to_json(self):
        """
        :return:
        """
        d = {k: v for k, v in self.dict.items()}
        return d


if __name__ == '__main__':
    pd = ParamDefinition.from_json({'a': 'Array', 'b': 'Object', 'c': 'Number'})
    adapter = ParamAdapter.from_json({'a': '$.aa', 'b': '$.bb', 'c': '$.cc'})
    r = adapter.parse({'aa': [1, 2], 'bb': {'x': 1}, 'cc': 12})
    print(r, pd.validate(r))
