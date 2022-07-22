from django.db import models

# Create your models here.
from .consts import TaskStates, StepStates, ActionTypes


class BaseModel(models.Model):

    def update(self, _refresh=True, **kwargs):
        self.__class__.objects.filter(pk=self.pk).update(**kwargs)
        if _refresh:
            self.refresh_from_db()

    class Meta:
        abstract = True


class Action(BaseModel):
    """
    流程Action
    a1，a2，a3，a4，a5，a6
    """

    id = models.AutoField(primary_key=True)

    name = models.CharField(max_length=64, db_index=True, unique=True)
    title = models.CharField(max_length=64)
    type = models.CharField(max_length=32, choices=[(item.name, item.value) for item in ActionTypes])
    func = models.CharField(max_length=128, unique=True, null=True)
    input_def = models.JSONField(default=dict)
    output_def = models.JSONField(default=dict)

    create_time = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = 'seaflow_action'
        verbose_name = '流程Action'
        verbose_name_plural = verbose_name


class Dag(BaseModel):
    """
    流程图
      -C-
    A--B--E--F
      -D-
    """

    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=64, db_index=True)
    title = models.CharField(max_length=64)
    version = models.PositiveIntegerField(null=True, db_index=True)  # root dag才有version
    latest = models.BooleanField(null=True)

    root = models.ForeignKey('Dag', db_constraint=False, null=True,
                             related_name='descendants', on_delete=models.CASCADE)
    parent = models.ForeignKey('Dag', db_constraint=False, null=True,
                               related_name='children', on_delete=models.CASCADE)
    previous_dags = models.ManyToManyField('Dag', db_constraint=False, related_name='next_dags',
                                           db_table='seaflow_dag_to_dag')
    previous_nodes = models.ManyToManyField('Node', db_constraint=False, related_name='next_dags',
                                            db_table='seaflow_node_to_dag')

    input_adapter = models.JSONField(default=dict)
    output_adapter = models.JSONField(default=dict)

    # 分裂
    fissionable = models.BooleanField('可分裂', default=False)
    fission_config = models.JSONField(default=dict)

    # 迭代
    iterable = models.BooleanField('可迭代', default=False)
    iter_config = models.JSONField(default=dict)  # condition, iter_key

    # 循环
    loopable = models.BooleanField('可循环', default=False)
    loop_config = models.JSONField(default=dict)  # condition, countdown

    max_retries = models.IntegerField(default=0)
    retry_countdown = models.IntegerField(default=0)
    timeout = models.IntegerField(null=True)

    create_time = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        managed = True
        db_table = 'seaflow_dag'
        verbose_name = '流程图'
        verbose_name_plural = verbose_name


class Node(BaseModel):
    """
    流程节点
    n1，n2，n3，n4，n5，n6
    """

    id = models.AutoField(primary_key=True)

    name = models.CharField(max_length=64)
    title = models.CharField(max_length=64)
    action = models.ForeignKey('Action', db_constraint=False, on_delete=models.PROTECT)
    dag = models.ForeignKey('Dag', db_constraint=False, related_name='nodes', on_delete=models.CASCADE)
    root_dag = models.ForeignKey('Dag', db_constraint=False, related_name='all_nodes', on_delete=models.CASCADE)

    previous_nodes = models.ManyToManyField('Node', db_constraint=False, related_name='next_nodes',
                                            db_table='seaflow_node_to_node')
    previous_dags = models.ManyToManyField('Dag', db_constraint=False, related_name='next_nodes',
                                           db_table='seaflow_dag_to_node')

    input_adapter = models.JSONField(default=dict)
    output_adapter = models.JSONField(default=dict)

    action_type = models.CharField(max_length=32,
                                   choices=[(item.name, item.value) for item in ActionTypes])

    # 分裂
    fissionable = models.BooleanField('可分裂', default=False)
    fission_config = models.JSONField(default=dict)

    # 迭代
    iterable = models.BooleanField('可迭代', default=False)
    iter_config = models.JSONField(default=dict)  # condition, iter_key

    # 循环
    loopable = models.BooleanField('可循环', default=False)
    loop_config = models.JSONField(default=dict)  # condition, countdown

    max_retries = models.IntegerField(default=0)
    retry_countdown = models.IntegerField(default=0)
    timeout = models.IntegerField(null=True)

    create_time = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        managed = True
        db_table = 'seaflow_node'
        verbose_name = '流程节点'
        verbose_name_plural = verbose_name


class Task(BaseModel):
    """
    任务
    """

    id = models.AutoField(primary_key=True)

    name = models.CharField(max_length=64)
    title = models.CharField(max_length=64)
    dag = models.ForeignKey('Dag', db_constraint=False, on_delete=models.PROTECT)
    state = models.CharField(max_length=64,
                             choices=[(item.name, item.value) for item in TaskStates])

    root = models.ForeignKey('Task', db_constraint=False, null=True,
                             related_name='descendants', on_delete=models.CASCADE)
    parent = models.ForeignKey('Task', db_constraint=False, null=True,
                               related_name='children', on_delete=models.CASCADE)
    previous_tasks = models.ManyToManyField('Task', db_constraint=False, related_name='next_tasks',
                                            db_table='seaflow_task_to_task')

    previous_steps = models.ManyToManyField('Step', db_constraint=False, related_name='next_tasks',
                                            db_table='seaflow_step_to_task')
    config = models.JSONField('配置', default=dict)
    retries = models.IntegerField('重试次数', default=0)
    # 分裂
    fission_count = models.IntegerField('分裂数', default=1)
    fission_index = models.IntegerField('分裂序号', db_index=True, default=0)

    # 迭代
    iter_index = models.IntegerField('迭代序号', db_index=True, default=0)
    iter_end = models.BooleanField('迭代末端', null=True)

    # 循环
    loop_index = models.IntegerField('循环序号', default=0)

    input = models.JSONField('输入')
    context = models.JSONField('上下文', default=dict)
    output = models.JSONField('输出', default=dict)

    extra = models.JSONField(default=dict)

    error = models.TextField('错误信息', default='')
    logs = models.JSONField('执行日志', default=list)
    logfile = models.CharField('s3日志文件路径', max_length=256, default='')
    log_refetch_time = models.DateTimeField(null=True, db_index=True)

    start_time = models.DateTimeField(null=True)
    end_time = models.DateTimeField(null=True)
    duration = models.FloatField(null=True)

    create_time = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)

    def __str__(self):
        return '%s%s%s' % (
            self.name,
            ' fission-%s' % self.fission_index if self.dag.fissionable else '',
            ' iter-%s' % self.iter_index if self.dag.iterable else '',
        )

    def to_dict(self, with_logs=False):
        d = {
            'id': self.id,
            'name': self.name,
            'title': self.title,
            'dag_id': self.dag_id,
            'dag': {
                'id': self.dag.id,
                'name': self.dag.name,
                'version': self.dag.version
            },
            'state': self.state,
            'state_display': self.get_state_display(),
            'root_id': self.root_id,
            'parent_id': self.parent_id,
            'config': self.config,
            'retries': self.retries,
            'fission_count': self.fission_count,
            'fission_index': self.fission_index,
            'iter_index': self.iter_index,
            'iter_end': self.iter_end,
            'loop_index': self.loop_index,
            'input': self.input,
            'context': self.context,
            'output': self.output,
            'error': self.error,
            'start_time': self.start_time,
            'start_timestamp': self.start_time.timestamp() * 1000 if self.start_time else None,
            'end_time': self.end_time,
            'end_timestamp': self.end_time.timestamp() * 1000 if self.end_time else None,
            'duration': self.duration,
            'create_time': self.create_time,
            'create_timestamp': self.create_time.timestamp() * 1000 if self.create_time else None,
            'update_time': self.update_time,
            'update_timestamp': self.update_time.timestamp() * 1000 if self.update_time else None,
        }

        if with_logs:
            d['logs'] = self.logs

        return d

    def to_json(self, *args, **kwargs):
        d = self.to_dict(*args, **kwargs)
        d.pop('start_time')
        d.pop('end_time')
        d.pop('create_time')
        d.pop('update_time')
        return d

    class Meta:
        managed = True
        db_table = 'seaflow_task'
        verbose_name = '任务'
        verbose_name_plural = verbose_name
        unique_together = ['parent', 'dag', 'fission_index', 'iter_index']


class Step(BaseModel):
    """
    任务步骤
    """

    id = models.AutoField(primary_key=True)
    identifier = models.CharField(max_length=64, db_index=True, null=True)

    name = models.CharField(max_length=64)
    title = models.CharField(max_length=64)
    node = models.ForeignKey('Node', db_constraint=False, on_delete=models.PROTECT)
    root = models.ForeignKey('Task', db_constraint=False, related_name='all_steps', on_delete=models.CASCADE)
    task = models.ForeignKey('Task', db_constraint=False, related_name='steps', on_delete=models.CASCADE)
    state = models.CharField(max_length=64,
                             choices=[(item.name, item.value) for item in StepStates])

    previous_steps = models.ManyToManyField('Step', db_constraint=False, related_name='next_steps',
                                            db_table='seaflow_step_to_step')
    previous_tasks = models.ManyToManyField('Task', db_constraint=False, related_name='next_steps',
                                            db_table='seaflow_task_to_step')
    config = models.JSONField('配置', default=dict)
    retries = models.IntegerField('重试次数', default=0)
    # 分裂
    fission_count = models.IntegerField('分裂数', default=1)
    fission_index = models.IntegerField('分裂序号', db_index=True, default=0)

    # 迭代
    iter_index = models.IntegerField('迭代序号', db_index=True, default=0)
    iter_end = models.BooleanField('迭代末端', null=True)

    # 循环
    loop_index = models.IntegerField('循环序号', default=0)

    input = models.JSONField('输入')
    output = models.JSONField('输出', default=dict)

    extra = models.JSONField(default=dict)

    error = models.TextField('错误信息', default='')
    logs = models.JSONField('执行日志', default=list)
    logfile = models.CharField('s3日志文件路径', max_length=256, default='')
    log_refetch_time = models.DateTimeField(null=True, db_index=True)

    start_time = models.DateTimeField(null=True)
    end_time = models.DateTimeField(null=True)
    duration = models.FloatField(null=True)

    create_time = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)

    def __str__(self):
        return '%s%s%s' % (
            self.name,
            ' fission-%s' % self.fission_index if self.node.fissionable else '',
            ' iter-%s' % self.iter_index if self.node.iterable else '',
        )

    def to_dict(self, with_logs=False):
        d = {
            'id': self.id,
            'name': self.name,
            'title': self.title,
            'node_id': self.node_id,
            'node': {
              'id': self.node.id,
              'name': self.node.name
            },
            'root_id': self.root_id,
            'task_id': self.task_id,
            'state': self.state,
            'state_display': self.get_state_display(),
            'config': self.config,
            'retries': self.retries,
            'fission_count': self.fission_count,
            'fission_index': self.fission_index,
            'iter_index': self.iter_index,
            'iter_end': self.iter_end,
            'loop_index': self.loop_index,
            'input': self.input,
            'output': self.output,
            'error': self.error,
            'start_time': self.start_time,
            'start_timestamp': self.start_time.timestamp() * 1000 if self.start_time else None,
            'end_time': self.end_time,
            'end_timestamp': self.end_time.timestamp() * 1000 if self.end_time else None,
            'duration': self.duration,
            'create_time': self.create_time,
            'create_timestamp': self.create_time.timestamp() * 1000 if self.create_time else None,
            'update_time': self.update_time,
            'update_timestamp': self.update_time.timestamp() * 1000 if self.update_time else None,
        }

        if with_logs:
            d['logs'] = self.logs

        return d

    def to_json(self, *args, **kwargs):
        d = self.to_dict(*args, **kwargs)
        d.pop('start_time')
        d.pop('end_time')
        d.pop('create_time')
        d.pop('update_time')
        return d

    class Meta:
        managed = True
        db_table = 'seaflow_step'
        verbose_name = '任务步骤'
        verbose_name_plural = verbose_name
        unique_together = ['task', 'node', 'fission_index', 'iter_index']


class Log(BaseModel):
    """
    日志表
    """

    id = models.AutoField(primary_key=True)
    ref_type = models.CharField(max_length=16, choices=[('TASK', 'Task'), ('STEP', 'Step')]
                                )
    ref_id = models.IntegerField(db_index=True)
    ts = models.FloatField()
    content = models.TextField()

    class Meta:
        managed = True
        db_table = 'seaflow_log'
        verbose_name = '日志'
        verbose_name_plural = verbose_name
