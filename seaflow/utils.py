import datetime
import hashlib
import importlib
import json
import random
import threading
import time
from enum import Enum

import jsonpath_rw as jsonpath


def singleton_class(post_init=None):
    def _dec(cls):
        def _init(func):
            def __init(self, *args, **kwargs):
                result = func(self, *args, **kwargs)
                if post_init:
                    post_init(self)
                return result

            return __init

        cls.__init__ = _init(cls.__init__)

        def __new__(_cls, *args, **kwargs):
            if not hasattr(cls, "_instance"):
                with cls._instance_lock:
                    if not hasattr(cls, "_instance"):
                        cls._instance = object.__new__(_cls)
            return cls._instance

        cls._instance_lock = threading.Lock()
        cls.__new__ = __new__
        return cls

    return _dec


class Dict(dict):

    def __setattr__(self, key, value):
        self[key] = value

    def __getattr__(self, name):
        try:
            return self.get(name)
        except:
            raise


class ComplexJSONEncoder(json.JSONEncoder):
    """
    encoder for json dumps
    """

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        elif isinstance(obj, NameComparableEnum):
            return str(obj)
        else:
            return json.JSONEncoder.default(self, obj)


class NameComparableEnum(Enum):

    def __eq__(self, other):
        return self.name == other

    def __str__(self):
        return self.name


class SeaflowException(Exception):

    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


class NotPrintException:
    """
    """


class TimeoutException(SeaflowException, NotPrintException):
    """
    """


class RevokeException(SeaflowException, NotPrintException):
    """
    """


class ExternalActionException(SeaflowException, NotPrintException):
    """
    外部action异常
    """


class ExternalActionFailed(ExternalActionException):
    """
    外部action执行失败
    """

class StateException(SeaflowException):
    """
    状态错误
    """


class ParamException(SeaflowException):
    """
    输入/输出参数错误
    """


class ParamDefinitionException(ParamException):
    """
    输入/输出参数不符合定义
    """

    def __init__(self, *fields):
        super().__init__('invalid fields: %s' % ', '.join(fields))


class ParamAdaptException(ParamException):
    """
    输入/输出参数适配错误
    """

    def __init__(self, *fields):
        super().__init__('failed to adapt fields: %s' % ', '.join(fields))


class SeaflowContext(object):
    """
    """

    def __init__(self, task_id=None, task=None):
        from .models import Task
        self.Task = Task
        self.task = task or self.Task.objects.get(pk=task_id)
        self.context = self.task.context

    def set(self, c):
        """
        局部更新
        :param c: dict
        :return:
        """

        from django_mysql.models.functions import JSONSet
        self.task.update(context=JSONSet('context', {'$.%s' % k: v for k, v in c.items()}), _refresh=False)
        self.reload()

    def get(self):
        """
        :return:
        """
        self.reload()
        return self.context

    def __setitem__(self, key, value):
        self.set({key: value})

    def __getitem__(self, item):
        self.reload()
        return self.context[item]

    def reload(self):
        self.task.refresh_from_db()
        self.context = self.task.context


def tracker_flush(instance, logs):
    instance.update(logs=('%s\n%s' % (instance.logs, logs)).strip())


def get_func(func_name):
    """
    根据func_name反射得到func
    :param func_name:例如apps.api.tasks.request_info
    :return:
    """
    rs = func_name.rsplit('.', 1)
    if len(rs) == 2:
        return getattr(importlib.import_module(rs[0]), rs[1])
    else:
        return eval(func_name)


def generate_identifier(value=None, rand=True):
    _id = hashlib.md5()
    if value:
        _id.update(value.encode())
    if rand:
        _id.update(time.asctime().encode())
        _id.update(str(random.randint(0, 999999999)).encode())
    return _id.hexdigest()


# def jsonpath_to_jsonlogic(o):
#     """
#     {"<": ["$.a", "$.b"]} to {"<": [["var": "a"], ["var", "b"]]}
#     :param o:
#     :return:
#     """
#     o = deepcopy(o)
#
#     def _recursion(_o, parent=None, key=None):
#         """
#         :param _o:
#         :return:
#         """
#         if isinstance(_o, dict):
#             for k, v in _o.items():
#                 _recursion(v, _o, k)
#         elif isinstance(_o, list):
#             for k, v in enumerate(_o):
#                 _recursion(v, _o, k)
#         elif isinstance(_o, str):
#             if _o.startswith('$.'):
#                 parent[key] = ['var', _o[2:]]
#
#     _recursion(o)
#     return o


def del_attrs(o, *attrs):
    for attr in attrs:
        if hasattr(o, attr):
            delattr(o, attr)


# class TimeoutManager(object):
#
#     box = {}
#
#     def register(self, key, expire, callback):
#         self.box[key] = (key, expire, callback)
#
#     def listen(self):
#         pass

class Config(dict):

    def __init__(self, **kwargs):
        for k in kwargs.keys():
            if k in self._keys:
                self[k] = kwargs[k]

    def trim(self):
        for k in list(self.keys()):
            if self[k] is None:
                self.pop(k)
        return self

    @classmethod
    def from_json(cls, o):
        return cls(**o)


class CallbackConfig(Config):
    _keys = ('is_async', 'func')


class TaskConfig(Config):
    _keys = ('countdown', 'max_retries', 'retry_countdown', 'timeout', 'callback')


class StepConfig(Config):
    _keys = ('countdown', 'max_retries', 'retry_countdown', 'timeout', 'callback')


def fission_inputs(inputs, fission_key):
    rs = jsonpath.parse(fission_key).find(inputs)
    assert len(rs) == 1, rs
    return [dict(inputs, **{rs[0].path.fields[0]: x}) for x in rs[0].value]


def iter_inputs(inputs, iter_key):
    """
    :param inputs:
    :param iter_key:
    :return: current_inputs, iter_sequence
    """
    rs = jsonpath.parse(iter_key).find(inputs)
    assert len(rs) == 1, rs
    key = rs[0].path.fields[0]
    sequence = rs[0].value
    return dict(inputs, **{key: sequence[0]}), key, sequence


def merge_outputs_of_tasks(tasks):
    fission_dict = {}
    outputs_list = []
    for t in tasks:
        if t.dag.fissionable:
            if t.dag.id not in fission_dict:
                fission_dict[t.dag.id] = [t.output]
            else:
                fission_dict[t.dag.id].append(t.output)
        else:
            outputs_list.append(t.output)

    for _, v in fission_dict.items():
        outputs_list.append(merge_fission_outputs(*v))

    return merge_outputs(*outputs_list)


def merge_outputs_of_steps(steps):
    fission_dict = {}
    outputs_list = []
    for s in steps:
        if s.node.fissionable:
            if s.node.id not in fission_dict:
                fission_dict[s.node.id] = [s.output]
            else:
                fission_dict[s.node.id].append(s.output)
        else:
            outputs_list.append(s.output)

    for _, v in fission_dict.items():
        outputs_list.append(merge_fission_outputs(*v))

    return merge_outputs(*outputs_list)


def merge_outputs(*outputs):
    """
    相同的key，merge后的值为array
    :param outputs:
    :return:
    """
    result = {}
    for o in outputs:
        for k, v in o.items():
            if k not in result:
                result[k] = [v]
            else:
                result[k].append(v)
    for k, v in result.items():
        if len(v) == 1:
            result[k] = v[0]

    return result


def merge_fission_outputs(*outputs):
    result = {}
    for o in outputs:
        for k, v in o.items():
            if k not in result:
                result[k] = [v]
            else:
                result[k].append(v)

    return result
