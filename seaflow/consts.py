from enum import unique

from .utils import NameComparableEnum


@unique
class TaskStates(NameComparableEnum):
    PENDING = '等待中'
    PROCESSING = '正在执行'
    SLEEP = '休眠'
    RETRY = '等待重试'
    TIMEOUT = '超时'
    SUCCESS = '成功'
    ERROR = '错误'
    REVOKE = '撤销'
    TERMINATE = '终止'


@unique
class StepStates(NameComparableEnum):
    PENDING = '等待中'
    PUBLISH = '已发布' # 外部节点
    PROCESSING = '正在执行'
    SLEEP = '休眠'
    RETRY = '等待重试'
    TIMEOUT = '超时'
    SUCCESS = '成功'
    ERROR = '错误'
    REVOKE = '撤销'
    TERMINATE = '终止'


@unique
class ActionTypes(NameComparableEnum):
    Default = 'default'
    Carrier = 'carrier'
    External = 'external'
