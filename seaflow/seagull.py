import datetime
import json
import logging
import os
import sys
import time
import traceback

from .models import Log, Task
from .utils import NotPrintException


class Tracker(object):
    """
    logger
    """

    def __init__(self,
                 name='',
                 level=logging.NOTSET,
                 max_buffer_lines=1,
                 auto_flush=True,
                 flush_action=None
                 ):
        self.logger = logging.getLogger(name)
        self.logger.level = level
        self._buf = []
        self.level = level
        self.max_buffer_lines = max_buffer_lines
        self.auto_flush = auto_flush
        self.flush_action = flush_action
        self.ignore_empty_lines = True

    def length(self):
        return len(self._buf)

    def debug(self, message):
        if self.level <= logging.DEBUG:
            self.write(self._format(message, 'DEBUG'))
            self.logger.debug(message)

    def info(self, message):
        if self.level <= logging.INFO:
            self.write(self._format(message, 'INFO'))
            self.logger.info(message)

    def warn(self, message):
        if self.level <= logging.WARNING:
            self.write(self._format(message, 'WARN'))
            self.logger.warning(message)

    def error(self, message):
        if self.level <= logging.ERROR:
            self.write(self._format(message, 'ERROR'))
            self.logger.error(message)

    def flush(self, exec_flush_action=False, **kwargs):
        logs = self._buf.copy()
        self._buf = []
        if exec_flush_action and self.flush_action:
            self.flush_action(logs, **kwargs)
        return logs

    def write(self, content):
        self._buf.append({'ts': time.time() * 1000, 'content': content})
        if self.auto_flush and len(self._buf) >= self.max_buffer_lines + 1:
            self.flush(exec_flush_action=True)

    def _format(self, content, level):
        content = content.strip('\n')
        # co_filename, func_lineno, co_name = self._find_caller()
        # return '[%s] %s %s:%s: [line:%s] %s' % (
        return '[%s] %s: %s' % (
            datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4],
            level,
            # co_filename,
            # co_name,
            # func_lineno,
            content
        )

    def trace_error(self, e=None):
        if e:
            e_type, e_value, traceback_obj = type(e), e, e.__traceback__
        else:
            e_type, e_value, traceback_obj = sys.exc_info()[:3]
        self.logger.exception(e_value)

        lines = ['Type: %s' % e_type, 'Value: %s' % e_value]
        for line in traceback.format_exception(e_type, e_value, traceback_obj)[1:]:
            line = line.rstrip('\n')
            lines.append(line)
        if not isinstance(e, NotPrintException):
            self.error('ErrorStack:')
            for line in lines:
                self.write(line)

        return '\n'.join(lines)

    def _find_caller(self):
        """
        获取调用者信息, 用于记录file func lineno
        :return:
        """

        f = logging.currentframe()

        if f is not None:
            f = f.f_back
        rv = "(unknown file)", 0, "(unknown function)"
        while hasattr(f, "f_code"):
            co = f.f_code
            filename = os.path.normcase(co.co_filename)
            if filename == __file__:
                f = f.f_back
                continue
            rv = (co.co_filename, f.f_lineno, co.co_name)
            break
        return rv


class Seagull(Tracker):
    seagulls = {}

    def __init__(self, ref, *args, **kwargs):
        self.ref = ref
        self.ref_type = 'TASK' if isinstance(self.ref, Task) else 'STEP'
        kwargs['flush_action'] = self.persist
        kwargs['auto_flush'] = True
        super().__init__('seagull', *args, **kwargs)

    def persist(self, logs, merge=False):
        if logs:
            Log.objects.bulk_create([Log(
                ref_type=self.ref_type,
                ref_id=self.ref.id,
                ts=lg['ts'],
                content=lg['content']
            ) for lg in logs])
        if merge:
            self.merge()

    def merge(self):
        logs = []
        ids = []
        for lg in Log.objects.filter(ref_id=self.ref.id, ref_type=self.ref_type):
            logs.append({'ts': lg.ts, 'content': lg.content})
            ids.append(lg.id)
        if logs:
            self.ref.refresh_from_db()
            self.ref.update(_refresh=False, logs=(self.ref.logs + logs))

            # NOTE: 避免一次删除太多数据，引发 DB 报警：每次删除 500 条日志
            cursor = 0
            size = 500
            while cursor < len(ids):
                Log.objects.filter(pk__in=ids[cursor:cursor+size]).delete()
                cursor += size

    @classmethod
    def instance(cls, target, *args, **kwargs):
        identifier = (target.__class__.name, target.id)
        if identifier not in cls.seagulls:
            cls.seagulls[identifier] = cls(target, *args, **kwargs)
        return cls.seagulls[identifier]

    def __del__(self):
        self.flush(True)
