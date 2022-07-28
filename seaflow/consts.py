from enum import unique

from .utils import NameComparableEnum


@unique
class TaskStates(NameComparableEnum):
    PENDING = '等待中'
    PROCESSING = '正在执行'
    SLEEP = '休眠'
    RETRY = '等待重试'
    SKIP = '跳过'
    TIMEOUT = '超时'
    SUCCESS = '成功'
    ERROR = '错误'
    REVOKE = '撤销'
    TERMINATE = '终止'

    @classmethod
    def end_states(cls):
        return [cls.SKIP, cls.SUCCESS,
                cls.TIMEOUT, cls.ERROR,
                cls.REVOKE, cls.TERMINATE]

    @classmethod
    def finish_states(cls):
        return [cls.SKIP, cls.SUCCESS]

    @classmethod
    def fail_states(cls):
        return [cls.TIMEOUT, cls.ERROR]

    @classmethod
    def interrupt_states(cls):
        return [cls.REVOKE, cls.TERMINATE]

    @classmethod
    def sleepable_states(cls):
        return [cls.PENDING, cls.PROCESSING, cls.RETRY]

    @classmethod
    def revocable_states(cls):
        return [cls.PENDING, cls.PROCESSING, cls.RETRY]

    @classmethod
    def terminable_states(cls):
        # revoke状态的task下的可能还有正在执行的step
        return [cls.PENDING, cls.PROCESSING,
                cls.SLEEP, cls.REVOKE, cls.RETRY]


@unique
class StepStates(NameComparableEnum):
    PENDING = '等待中'
    PUBLISH = '已发布'  # 外部节点
    PROCESSING = '正在执行'
    SLEEP = '休眠'
    RETRY = '等待重试'
    SKIP = '跳过'
    TIMEOUT = '超时'
    SUCCESS = '成功'
    ERROR = '错误'
    REVOKE = '撤销'
    TERMINATE = '终止'

    @classmethod
    def end_states(cls):
        return [cls.SKIP, cls.SUCCESS,
                cls.TIMEOUT, cls.ERROR,
                cls.REVOKE, cls.TERMINATE]

    @classmethod
    def finish_states(cls):
        return [cls.SKIP, cls.SUCCESS]

    @classmethod
    def error_states(cls):
        return [cls.TIMEOUT, cls.ERROR]

    @classmethod
    def interrupt_states(cls):
        return [cls.REVOKE, cls.TERMINATE]

    @classmethod
    def revocable_states(cls):
        return [cls.PENDING, cls.RETRY]

    @classmethod
    def terminable_states(cls):
        return [cls.PENDING, cls.PUBLISH,
                cls.PROCESSING, cls.SLEEP,
                cls.RETRY]


@unique
class ActionTypes(NameComparableEnum):
    Default = 'default'
    Carrier = 'carrier'
    External = 'external'
