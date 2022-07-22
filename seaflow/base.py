import functools
import hashlib
import json
import sys
from copy import deepcopy

from celery.result import AsyncResult
from django.db import transaction, models
from django.utils import timezone
from json_logic import jsonLogic
from timeout_decorator import timeout

import core.contrib
from core.contrib.seaflow import celery_app, tasks
from core.contrib.seaflow.consts import ActionTypes, TaskStates, StepStates
from core.contrib.seaflow.models import Action, Dag, Node, Task, Step
from core.contrib.seaflow.params import ParamAdapter, ParamDefinition
from core.contrib.seaflow.seagull import Seagull
from core.contrib.seaflow.tasks import do_callback
from core.contrib.seaflow.utils import SeaflowException, ComplexJSONEncoder, ParamDefinitionException, get_func, \
    merge_outputs, merge_outputs_of_tasks, merge_outputs_of_steps, fission_inputs, iter_inputs, RevokeException, \
    TimeoutException, merge_fission_outputs, TaskConfig, StepConfig, StateException, generate_identifier, \
    SeaflowContext, del_attrs, ExternalActionFailed


class Seaflow(object):

    @classmethod
    def create_task(cls, *args, **kwargs):
        return SeaflowTask.create(*args, **kwargs)

    @classmethod
    def get_task(cls, task_id):
        return SeaflowTask.get(task_id)

    @staticmethod
    @transaction.atomic()
    def load_actions(dsl):
        """
        load actions from dsl
        :param dsl: json dsl
        :return:
        """

        actions = []
        for item in dsl:
            actions.append(Action(
                name=item['name'],
                title=item.get('title', item['name']),
                func=item.get('func'),
                type=item.get('type', ActionTypes.Default),
                input_def=item.get('input_def', {}),
                output_def=item.get('output_def', {}),
            ))
        with transaction.atomic():
            for a in actions:
                a.save()
            # assert len(actions) == len(dsl), 'not all actions are created'
        return actions

    @staticmethod
    @transaction.atomic()
    def load_dag(dsl):
        """
        load dag from dsl
        :param dsl: json dsl
        :return:
        """
        dsl = deepcopy(dsl)
        root_identifier = dsl['identifier']
        d = {
            root_identifier: dsl,
        }

        # 先处理refs
        ref_components = []
        for item in dsl['components']:
            if item.get('ref'):
                assert item['kind'] == 'Dag'
                _ = item['ref'].rsplit('.', 1)
                ref_name, ref_version = _[0], None if len(_) == 1 else _[1]
                ref = Seaflow.dag_dsl(dag_name=ref_name, dag_version=ref_version, root_identifier=item['identifier'])
                ref_components += ref['components']

        dsl['components'] += ref_components

        action_dict = {}
        node_dict = {}
        dag_children_dict = {}
        for item in dsl['components']:
            assert item['identifier'] not in d, 'duplicated identifier %s' % item['identifier']
            d[item['identifier']] = item
            if item['kind'] == 'Dag':
                parent = item.get('parent', root_identifier)
                if parent not in dag_children_dict:
                    dag_children_dict[parent] = [item['identifier']]
                else:
                    dag_children_dict[parent].append(item['identifier'])
            elif item['kind'] == 'Node':
                action_dict[item['action']] = None
                node_dict[item['identifier']] = item

        action_length = len(action_dict)
        action_dict = {a.name: a for a in Action.objects.filter(name__in=action_dict.keys())}
        assert action_length == len(action_dict), 'action not exist'

        def __recursive_create_dags(_identifiers):
            for x in _identifiers:
                dag_item = d[x]
                if x == root_identifier:
                    # root dag
                    version = dag_item.get('version', 1)
                    exists_dags = Dag.objects.filter(name=dag_item['name'], parent=None).order_by('-version')
                    latest_dag = None
                    if exists_dags: latest_dag = exists_dags[0]
                    assert latest_dag.version < version, 'out of version'
                    Dag.objects.filter(name=dag_item['name'], parent=None).update(latest=False)
                    d[x] = Dag.objects.create(
                        name=dag_item['name'],
                        title=dag_item.get('title', dag_item['name']),
                        version=version,
                        latest=True,
                        max_retries=dag_item.get('max_retries', 0),
                        timeout=dag_item.get('timeout'),
                        input_adapter=dag_item.get('input_adapter', {}),
                        output_adapter=dag_item.get('output_adapter', {})
                    )
                else:
                    d[x] = Dag.objects.create(
                        name=dag_item['name'],
                        title=dag_item.get('title', dag_item['name']),
                        root=d[root_identifier],
                        parent=d[dag_item.get('parent', root_identifier)],
                        fissionable=bool(dag_item.get('fission')),
                        fission_config=dag_item.get('fission', {}),
                        iterable=bool(dag_item.get('iter')),
                        iter_config=dag_item.get('iter', {}),
                        loopable=bool(dag_item.get('loop')),
                        loop_config=dag_item.get('loop', {}),
                        max_retries=dag_item.get('max_retries', 0),
                        timeout=dag_item.get('timeout'),
                        input_adapter=dag_item.get('input_adapter', {}),
                        output_adapter=dag_item.get('output_adapter', {})
                    )
                children = dag_children_dict.pop(x, [])
                if children:
                    __recursive_create_dags(children)

        with transaction.atomic():
            __recursive_create_dags([root_identifier])
            for k, node_item in node_dict.items():
                a = action_dict[node_item['action']]
                d[k] = Node.objects.create(
                    name=node_item['name'],
                    title=node_item.get('title', node_item['name']),
                    dag=d[node_item.get('dag', root_identifier)],
                    root_dag=d[root_identifier],
                    fissionable=bool(node_item.get('fission')),
                    fission_config=node_item.get('fission', {}),
                    iterable=bool(node_item.get('iter')),
                    iter_config=node_item.get('iter', {}),
                    loopable=bool(node_item.get('loop')),
                    loop_config=node_item.get('loop', {}),
                    action=a,
                    action_type=a.type,
                    max_retries=node_item.get('max_retries', 0),
                    timeout=node_item.get('timeout'),
                    input_adapter=node_item.get('input_adapter', {}),
                    output_adapter=node_item.get('output_adapter', {})
                )

            # relations
            for item in dsl['components']:
                if item.get('previous_dags'):
                    d[item['identifier']].previous_dags.set([d[x] for x in item['previous_dags']])
                if item.get('previous_nodes'):
                    d[item['identifier']].previous_nodes.set([d[x] for x in item['previous_nodes']])

        return d[root_identifier]

    @staticmethod
    def dump_dag_dsl(*args, **kwargs):
        dsl = Seaflow.dag_dsl(*args, **kwargs)
        return json.dumps(dsl, ensure_ascii=False, indent=2)

    @staticmethod
    def dag_dsl(dag_id=None, dag_name=None, dag_version=None, root_identifier=None):
        def _gen_identifier(s, prefix=root_identifier, use_hash=True):
            identifier = s
            if prefix:
                identifier = str(prefix) + str(s)
            if use_hash:
                return hashlib.md5(identifier.encode()).hexdigest()
            else:
                return identifier

        assert dag_id or dag_name
        try:
            if dag_id:
                dag = Dag.objects.prefetch_related('descendants', 'all_nodes', 'all_nodes__action').get(pk=dag_id)
            else:
                if dag_version:
                    dag = Dag.objects.prefetch_related('descendants', 'all_nodes', 'all_nodes__action').get(name=dag_name,
                                                                                                        version=dag_version)
                else:
                    dag = Dag.objects.prefetch_related('descendants', 'all_nodes', 'all_nodes__action').get(name=dag_name, latest=True)
        except models.ObjectDoesNotExist as e:
            sys.stderr.writelines(['dag not exists, id: %s, name: %s, version: %s' % (dag_id, dag_name, dag_version)])
            raise

        dag_identifiers = {
            dag.id: root_identifier or _gen_identifier(str(dag.id), prefix='root-%s-idx-' % dag.name, use_hash=False)
        }
        node_identifiers = {}
        for d in dag.descendants.all():
            dag_identifiers[d.id] = _gen_identifier(str(d.id), prefix='dag-%s-idx-' % d.name, use_hash=False)
        for n in dag.all_nodes.all():
            node_identifiers[n.id] = _gen_identifier(str(n.id), prefix='node-%s-idx-' % n.name, use_hash=False)
        dsl = {
            'identifier': dag_identifiers[dag.id],
            'name': dag.name,
            'title': dag.title,
            'version': dag.version,
            'latest': dag.latest,
            'input_adapter': dag.input_adapter,
            'output_adapter': dag.output_adapter,
            'components': []
        }

        for d in dag.descendants.all():
            d_data = {
                'identifier': dag_identifiers[d.id],
                'kind': 'Dag',
                'name': d.name,
                'title': d.title,
                'root': dag_identifiers[dag.id],
                'parent': dag_identifiers[d.parent.id],
                'input_adapter': d.input_adapter,
                'output_adapter': d.output_adapter,
                'previous_dags': [dag_identifiers[x.id] for x in d.previous_dags.all()],
                'previous_nodes': [node_identifiers[x.id] for x in d.previous_nodes.all()],
            }
            if d.fissionable: d_data['fission'] = d.fission_config
            if d.iterable: d_data['iter'] = d.iter_config
            if d.loopable: d_data['loop'] = d.loop_config
            dsl['components'].append(d_data)

        for n in dag.all_nodes.all():
            n_data = {
                'identifier': node_identifiers[n.id],
                'kind': 'Node',
                'name': n.name,
                'title': n.title,
                'dag': dag_identifiers[n.dag.id],
                'root_dag': dag_identifiers[dag.id],
                'fission': n.fission_config,
                'action': n.action.name,
                'action_type': n.action_type,
                'max_retries': n.max_retries,
                'input_adapter': n.input_adapter,
                'output_adapter': n.output_adapter,
                'previous_dags': [dag_identifiers[x.id] for x in n.previous_dags.all()],
                'previous_nodes': [node_identifiers[x.id] for x in n.previous_nodes.all()],
            }
            if n.fissionable: n_data['fission'] = n.fission_config
            if n.iterable: n_data['iter'] = n.iter_config
            if n.loopable: n_data['loop'] = n.loop_config
            dsl['components'].append(n_data)

        return dsl

    @classmethod
    def action(cls, *args, **opts):
        """
        :param args:
        :param opts:
        :return:
        """

        def _dec(func):
            @functools.wraps(func)
            def __dec(celery_action, step_id):
                celery_action.func = func
                SeaflowStep.get(step_id)._execute(celery_action)

            return celery_app.task(bind=True)(__dec)

        return _dec

    @classmethod
    def sleep_task(cls, task_id):
        """
        使任务进入睡眠状态
        :param task_id:
        :return:
        """

        SeaflowTask.get(task_id).sleep()

    @classmethod
    def awake_task(cls, task_id):
        """
        唤醒任务
        :param task_id:
        :return:
        """

        SeaflowTask.get(task_id).awake()

    @classmethod
    def revoke_task(cls, task_id):
        """
        撤销任务
        :param task_id:
        :return:
        """

        SeaflowTask.get(task_id).revoke()

    @classmethod
    def terminate_task(cls, task_id):
        """
        终止任务
        :param task_id:
        :return:
        """

        SeaflowTask.get(task_id).terminate()

    @classmethod
    def dispatch_external_step(cls, step_id, identity={}):
        """
        派发外部step
        :param step_id:
        :param identity: dict，订阅者的身份信息
                    name:
                    key:
        :return: identifier
        """
        return SeaflowStep.get(step_id)._dispatch(identity)

    @classmethod
    def finish_external_step(cls, step_id, outputs={}):
        """
        完成外部step
        :param step_id:
        :param outputs:
        :return:
        """
        step = SeaflowStep.get(step_id)
        if step.model.node.action_type != ActionTypes.External:
            raise SeaflowException('step is not external')
        try:
            step.seagull.info('action【%s】 output: %s' % (step.model.node.action.name,
                                                         json.dumps(outputs, cls=ComplexJSONEncoder)))
            step._finish(step._adapt_outputs(outputs))
        finally:
            step.seagull.flush(True)

    @classmethod
    def fail_external_step(cls, step_id, error=None, outputs={}):
        """
        失败外部step
        :param step_id:
        :param error:
        :param outputs:
        :return:
        """
        step = SeaflowStep.get(step_id)
        if step.model.node.action_type != ActionTypes.External:
            raise SeaflowException('step is not external')
        try:
            step.seagull.error('receive error: %s' % error)
            step.seagull.info('action【%s】 output: %s' % (step.model.node.action.name,
                                                         json.dumps(outputs, cls=ComplexJSONEncoder)))
            try:
                outputs = step._adapt_outputs(outputs)
            except ParamDefinitionException:
                outputs = {}
            step._break_off(e=ExternalActionFailed(error) if error else None, outputs=outputs)
        finally:
            step.seagull.flush(True)


class SeaflowTask(object):

    def __init__(self):
        # model
        self.model = None
        self.id = None
        self.name = None
        self.context = None
        self.seagull = None
        self.config = None

        #
        self.parent = None  # SeaflowTask
        self.root = None  # SeaflowTask

    @classmethod
    def create(cls, dag_id=None, dag_name=None, dag_version=None, name=None,
               inputs=None, context=None, config=None, tasks_config=None, steps_config=None):
        """
        :param dag_id:
        :param dag_name:
        :param dag_version:
        :param name:
        :param inputs:
        :param context:
        :param config: TaskConfig
        :param tasks_config: {<dag_name>: TaskConfig}
        :param steps_config: {<node_name>: StepConfig}
        :return: SeaflowTask
        """
        assert dag_id or dag_name
        if dag_id:
            dag = Dag.objects.get(pk=dag_id)
        else:
            if dag_version:
                dag = Dag.objects.get(name=dag_name, version=dag_version)
            else:
                dag = Dag.objects.get(name=dag_name, latest=True)
        assert not dag.root_id, 'assert to be root dag'
        inputs = ParamAdapter.from_json(dag.input_adapter).adapt(inputs or {})
        extra = {'tasks_config': tasks_config or {}, 'steps_config': steps_config or {}}
        t = Task.objects.create(
            name=name or dag.name,
            title=dag.title,
            dag=dag,
            state=TaskStates.PENDING,
            root=None,
            parent=None,
            input=inputs or {},
            context=context or {},
            config=config or {},
            extra=extra,
        )
        r = cls.get(task=t)
        r.seagull.info('task 【%s】 created: %s' % (t.name, t.id))
        r.seagull.flush(True)

        return r

    @classmethod
    def get(cls, task_id=None, task=None):
        """
        :param task_id:
        :param task:
        :return: SeaflowTask
        """
        m = task or Task.objects.select_related('dag', 'parent', 'root').defer('logs').get(pk=task_id)
        r = cls()
        r.model = m
        r.load()
        return r

    def load(self):
        self.id = self.model.id
        self.name = self.model.name
        self.context = (self.model.root or self.model).context
        self.seagull = Seagull.instance(self.model)
        self.config = self.model.config

        if self.model.parent:
            self.parent = self.__class__.get(task=self.model.parent)
        if self.model.root:
            self.root = self.__class__.get(task=self.model.root)

    def reload(self):
        self.model.refresh_from_db()
        self.load()

    def apply(self, sync=False, countdown=None):
        if sync:
            self._apply()
        else:
            core.contrib.seaflow.tasks.apply_root_task.apply_async((self.id,), countdown=countdown)

    def _apply(self):
        self.model.update(start_time=timezone.now(), state=StepStates.PROCESSING)
        self._do_callback('TASK_STATE_%s' % TaskStates.PROCESSING)
        self.seagull.info('task 【%s】 started: %s' % (self.name, self.id))

        # 第一批free node
        nodes = self.model.dag.nodes.filter(previous_dags=None, previous_nodes=None)
        if nodes:
            self.seagull.info('apply nodes...')
            [self._apply_node(n) for n in nodes]

        # 第一批dag
        dags = self.model.dag.children.filter(previous_dags=None, previous_nodes=None)
        if dags:
            self.seagull.info('apply dags...')
            [self._apply_dag(d) for d in dags]

        self.seagull.flush(True)

    def revoke(self):
        self.reload()
        if self.model.state in [TaskStates.PENDING, TaskStates.PROCESSING, TaskStates.RETRY]:
            tnow = timezone.now()
            duration = 0
            if self.model.start_time:
                duration = tnow - self.model.start_time
                duration = float('%s.%s' % (duration.seconds, duration.microseconds))
            self.model.update(
                state=TaskStates.REVOKE,
                end_time=tnow,
                duration=duration
            )
            self._do_callback('TASK_STATE_%s' % TaskStates.REVOKE)
        for t in self.model.descendants.filter(state__in=[TaskStates.PENDING, TaskStates.PROCESSING, TaskStates.RETRY]):
            tnow = timezone.now()
            duration = 0
            if t.start_time:
                duration = tnow - t.start_time
                duration = float('%s.%s' % (duration.seconds, duration.microseconds))
            t.update(
                state=TaskStates.REVOKE,
                end_time=tnow,
                duration=duration
            )

    def terminate(self):
        self.reload()
        if self.model.state in [TaskStates.PENDING, TaskStates.PROCESSING, TaskStates.RETRY, TaskStates.REVOKE]:
            tnow = timezone.now()
            duration = 0
            if self.model.start_time:
                duration = tnow - self.model.start_time
                duration = float('%s.%s' % (duration.seconds, duration.microseconds))
            self.model.update(
                state=TaskStates.TERMINATE,
                end_time=timezone.now(),
                duration=duration
            )
            self._do_callback('TASK_STATE_%s' % TaskStates.TERMINATE)
        for t in self.model.descendants.filter(state__in=[TaskStates.PENDING, TaskStates.PROCESSING,
                                                          TaskStates.RETRY, TaskStates.REVOKE]):
            tnow = timezone.now()
            duration = 0
            if t.start_time:
                duration = tnow - t.start_time
                duration = float('%s.%s' % (duration.seconds, duration.microseconds))
            Seagull.instance(t).flush(True, merge=True)
            t.update(
                state=TaskStates.TERMINATE,
                end_time=tnow,
                duration=duration
            )
        for step in self.model.all_steps.filter(state__in=[StepStates.PENDING,
                                                           StepStates.PUBLISH,
                                                           StepStates.PROCESSING,
                                                           StepStates.SLEEP,
                                                           StepStates.RETRY]):
            tnow = timezone.now()
            duration = 0
            if step.start_time:
                duration = tnow - step.start_time
                duration = float('%s.%s' % (duration.seconds, duration.microseconds))
            Seagull.instance(step).flush(True, merge=True)
            step.update(
                state=StepStates.TERMINATE,
                end_time=timezone.now(),
                duration=duration
            )
            if AsyncResult(step.identifier).state in ['PENDING', 'RECEIVED', 'STARTED', 'RETRY']:
                celery_app.control.revoke(step.identifier, terminate=True)

    def sleep(self):
        self.reload()
        if self.model.state in [TaskStates.PENDING, TaskStates.PROCESSING, TaskStates.RETRY]:
            self.model.update(state=TaskStates.SLEEP)
            self._do_callback('TASK_STATE_%s' % TaskStates.SLEEP)
        for t in self.model.descendants.filter(state__in=[TaskStates.PENDING, TaskStates.PROCESSING, TaskStates.RETRY]):
            t.update(state=TaskStates.SLEEP)
            SeaflowTask.get(task=t)._do_callback('TASK_STATE_%s' % TaskStates.SLEEP)

    def awake(self):

        self.reload()
        if self.model.state != TaskStates.SLEEP:
            return

        self.model.update(state=TaskStates.PROCESSING)
        self._do_callback('TASK_STATE_%s' % TaskStates.PROCESSING)
        for t in self.model.descendants.filter(state=TaskStates.SLEEP):
            t.update(state=TaskStates.PROCESSING)
            SeaflowTask.get(task=t)._do_callback('TASK_STATE_%s' % TaskStates.PROCESSING)

        # steps
        for s in self.model.all_steps.prefetch_related('node').filter(state=TaskStates.SLEEP):
            if s.node.action_type == ActionTypes.Carrier:
                # 旁路节点
                func = core.contrib.seaflow.tasks.start_carrier_step
            elif s.node.action_type == ActionTypes.External:
                # 外部节点
                func = tasks.publish_external_step
            else:
                # 普通节点
                func = get_func(s.node.action.func)
            func.delay(s.id)

    def _apply_dag(self, dag):
        """
        :param dag:子dag
        :return:
        """

        if not self._ready_to_execute_dag(dag):
            return
        self.seagull.info('apply dag 【%s】...' % dag.name)
        # 查找previous steps和previous tasks
        previous_tasks = Task.objects.filter(parent=self.model, dag__in=dag.previous_dags.all(),
                                             state=TaskStates.SUCCESS) \
            .exclude(iter_end=False)
        previous_steps = Step.objects.filter(task=self.model, node__in=dag.previous_nodes.all(),
                                             state=StepStates.SUCCESS) \
            .exclude(iter_end=False)
        if not previous_tasks and not previous_steps:
            # parent dag的第一批子dag
            inputs = self.model.input
        else:
            # inputs
            inputs = merge_outputs(merge_outputs_of_tasks(previous_tasks), merge_outputs_of_steps(previous_steps))

        self.seagull.info('dag 【%s】 raw input: %s'
                          % (dag.name, json.dumps(inputs, ensure_ascii=False)))
        input_adapter = ParamAdapter.from_json(dag.input_adapter)

        def _apply(_inputs,
                   _fissionable=False, _fission_index=0, _fission_count=1,
                   _iterable=False, _iter_index=0, _iter_context=None):

            extra = {}
            if _iter_context: extra['iter_context'] = _iter_context
            t, created = Task.objects.get_or_create(
                defaults=dict(
                    name=dag.name,
                    title=dag.title,
                    state=TaskStates.PROCESSING,
                    fission_count=_fission_count,
                    iter_end=False if _iterable else None,
                    input=_inputs,
                    config=self._generate_task_config(dag),
                    start_time=timezone.now(),
                    root=self.model.root or self.model,
                    extra=extra
                ),
                dag=dag,
                parent=self.model,
                fission_index=_fission_index,
                iter_index=_iter_index
            )
            s_task = SeaflowTask.get(task=t)
            if not created:
                self.seagull.warn('task 【%s】%s%s already exists'
                                  % (t.name,
                                     ' fission-%s' % _fission_index if _fissionable else '',
                                     ' iter-%s' % _iter_index if _iterable else ''
                                     ))
                self.seagull.flush(True)
                # 其他进程中已经创建了这个任务
                return
            t.previous_tasks.set(previous_tasks)
            t.previous_steps.set(previous_steps)

            try:
                s_task.seagull.info('task 【%s】%s%s created: %s'
                                    % (t.name,
                                       ' fission-%s' % _fission_index if _fissionable else '',
                                       ' iter-%s' % _iter_index if _iterable else '',
                                       t.id))

                # 第一批free node
                _nodes = dag.nodes.filter(previous_dags=None, previous_nodes=None)
                if _nodes:
                    s_task.seagull.info('apply nodes...')
                    [s_task._apply_node(n) for n in _nodes]

                # 第一批dag
                _dags = dag.children.filter(previous_dags=None, previous_nodes=None)
                if _dags:
                    s_task.seagull.info('apply dags...')
                    [s_task._apply_dag(d) for d in _dags]
                s_task.seagull.flush(True)
            except Exception as e:
                s_task.seagull.flush(True)
                s_task._break_off(e)

        if dag.fissionable:
            # 分裂
            inputs = fission_inputs(inputs, dag.fission_config['key'])
            fission_count = len(inputs)
            self.seagull.info('dag 【%s】 fission to %s by %s'
                              % (dag.name, fission_count, dag.fission_config['key']))
            for i, ii in enumerate(inputs):
                # adapt
                if dag.iterable:
                    ii, iter_key, iter_sequence = iter_inputs(ii, dag.iter_config['key'])
                    iter_index = 0
                    iter_context = {
                        'key': iter_key,
                        'sequence': iter_sequence
                    }
                    self.seagull.info('dag 【%s】 fission-%s start iter-%s'
                                      % (dag.name, i, iter_index))
                else:
                    iter_index = 0
                    iter_context = None
                ii = input_adapter.adapt(ii)
                self.seagull.info(
                    'dag 【%s】 fission-%s%s input: %s'
                    % (dag.name, i,
                       ' iter-%s' % iter_index if dag.iterable else '',
                       json.dumps(ii, ensure_ascii=False)))
                _apply(ii, True, i, fission_count, dag.iterable, iter_index, iter_context)
        else:
            if dag.iterable:
                inputs, iter_key, iter_sequence = iter_inputs(inputs, dag.iter_config['key'])
                iter_index = 0
                iter_context = {
                    'key': iter_key,
                    'sequence': iter_sequence
                }
                self.seagull.info('dag 【%s】 start iter-%s'
                                  % (dag.name, iter_index))
            else:
                iter_index = 0
                iter_context = None
            inputs = input_adapter.adapt(inputs)
            self.seagull.info('dag 【%s】%s input: %s'
                              % (dag.name,
                                 ' iter-%s' % iter_index if dag.iterable else '',
                                 json.dumps(inputs, ensure_ascii=False)))
            _apply(inputs, False, 0, 1, dag.iterable, iter_index, iter_context)

        self.seagull.flush(True)

    def _apply_node(self, node):
        """apply node, 请注意iter node的非首个step不会在这里处理
        :param node:
        :return:
        """
        ready = self._ready_to_execute_node(node)
        if not ready:
            return
        self.seagull.info('apply node 【%s】...' % node.name)
        # 查找previous steps和previous tasks
        previous_tasks = Task.objects.filter(parent=self.model,
                                             dag__in=node.previous_dags.all(),
                                             state=TaskStates.SUCCESS) \
            .exclude(iter_end=False)
        previous_steps = Step.objects.filter(task=self.model,
                                             node__in=node.previous_nodes.all(),
                                             state=StepStates.SUCCESS). \
            exclude(iter_end=False)
        # 参数
        if not previous_tasks and not previous_steps:
            # dag的第一批node
            inputs = self.model.input
        else:
            # inputs
            inputs = merge_outputs(merge_outputs_of_tasks(previous_tasks), merge_outputs_of_steps(previous_steps))

        self.seagull.info('node 【%s】 raw input: %s' % (node.name, json.dumps(inputs, ensure_ascii=False)))
        input_adapter = ParamAdapter.from_json(node.input_adapter)
        input_def = ParamDefinition.from_json(node.action.input_def)

        def _apply(_inputs,
                   _fissionable=False, _fission_index=0, _fission_count=1,
                   _iterable=False, _iter_index=0, _iter_context=None):

            extra = {}
            if _iter_context: extra['iter_context'] = _iter_context

            s, created = Step.objects.get_or_create(
                defaults=dict(
                    state=StepStates.PENDING,
                    fission_count=_fission_count,
                    iter_end=False if _iterable else None,
                    input=_inputs,
                    config=self._generate_step_config(node),
                    name=node.name,
                    title=node.title,
                    root=self.model.root or self.model,
                    extra=extra
                ),
                node=node,
                task=self.model,
                fission_index=_fission_index,
                iter_index=_iter_index
            )
            s_step = SeaflowStep.get(step=s)
            if not created:
                self.seagull.warn('step 【%s】%s%s already exists'
                                  % (s.name,
                                     ' fission-%s' % _fission_index if _fissionable else '',
                                     ' iter-%s' % _iter_index if _iterable else ''
                                     ))
                self.seagull.flush(True)
                # 其他进程中已经创建了这个任务
                return
            s.previous_tasks.set(previous_tasks)
            s.previous_steps.set(previous_steps)

            try:
                s_step.seagull.info('step 【%s】%s%s created: %s'
                                    % (s.name,
                                       ' fission-%s' % _fission_index if _fissionable else '',
                                       ' iter-%s' % _iter_index if _iterable else '',
                                       s.id))
                s_step.seagull.info('action type: %s' % node.action_type)
                if node.action_type == ActionTypes.Carrier:
                    # 旁路节点
                    func = core.contrib.seaflow.tasks.start_carrier_step
                elif node.action_type == ActionTypes.External:
                    # 外部节点
                    func = tasks.publish_external_step
                else:
                    # 普通节点
                    func = get_func(node.action.func)

                s_step.seagull.flush(True)

                func.apply_async((s.id,), countdown=s.config.get('countdown', 0))
            except Exception as e:
                s_step.seagull.flush(True)
                s_step._break_off(e)

        if node.fissionable:
            # 分裂
            inputs = fission_inputs(inputs, node.fission_config['key'])
            fission_count = len(inputs)
            self.seagull.info('node 【%s】 fission to %s by %s'
                              % (node.name, fission_count, node.fission_config['key']))
            for i, ii in enumerate(inputs):
                if node.iterable:
                    ii, iter_key, iter_sequence = iter_inputs(ii, node.iter_config['key'])
                    iter_index = 0
                    iter_context = {
                        'key': iter_key,
                        'sequence': iter_sequence
                    }
                    self.seagull.info('node 【%s】 fission-%s start iter-%s'
                                      % (node.name, i, iter_index))
                else:
                    iter_index = 0
                    iter_context = None
                # adapt
                if not (node.action_type == ActionTypes.Carrier and not input_adapter):
                    # carrier特殊处理
                    ii = input_adapter.adapt(ii)
                if not (node.action_type == ActionTypes.Carrier and not input_def):
                    # carrier特殊处理
                    valid, errors = input_def.validate(ii)
                    if not valid:
                        self.seagull.error('node 【%s】 fission-%s%s validate '
                                           'input failed: expect %s, got %s'
                                           % (node.name,
                                              i,
                                              ' iter-%s' % iter_index if node.iterable else '',
                                              input_def.dump(),
                                              ii))
                        raise ParamDefinitionException(*errors.keys())

                self.seagull.info(
                    'node 【%s】 fission-%s%s input: %s'
                    % (node.name, i,
                       ' iter-%s' % iter_index if node.iterable else '',
                       json.dumps(ii, ensure_ascii=False)))
                _apply(ii, node.fissionable, i, fission_count, node.iterable, iter_index, iter_context)
        else:
            if node.iterable:
                inputs, iter_key, iter_sequence = iter_inputs(inputs, node.iter_config['key'])
                iter_index = 0
                iter_context = {
                    'key': iter_key,
                    'sequence': iter_sequence
                }
                self.seagull.info('node 【%s】 start iter-%s' % (node.name, iter_index))
            else:
                iter_index = 0
                iter_context = None
            if not (node.action_type == ActionTypes.Carrier and not input_adapter):
                # carrier特殊处理
                inputs = input_adapter.adapt(inputs)
            if not (node.action_type == ActionTypes.Carrier and not input_def):
                # carrier特殊处理
                valid, errors = input_def.validate(inputs)
                if not valid:
                    self.seagull.error('node 【%s】%s validate input failed: expect %s, got %s'
                                       % (node.name,
                                          ' iter-%s' % iter_index if node.iterable else '',
                                          input_def.dump(),
                                          inputs))
                    raise ParamDefinitionException(*errors.keys())

            self.seagull.info('node 【%s】%s input: %s'
                              % (node.name,
                                 ' iter-%s' % iter_index if node.iterable else '',
                                 json.dumps(inputs, ensure_ascii=False)))
            _apply(inputs, node.fissionable, 0, 1, node.iterable, iter_index, iter_context)
        self.seagull.flush(True)

    def _break_off(self, e=None, outputs={}):
        self.reload()
        if self.model.state not in [TaskStates.PENDING, TaskStates.PROCESSING, TaskStates.RETRY]:
            return
        seagull = Seagull.instance(self.model)

        if e:
            error = seagull.trace_error(e)
            if self.model.retries < self.model.config.get('max_retries', 0) and not isinstance(e, RevokeException):
                state = TaskStates.RETRY
            elif isinstance(e, TimeoutException):
                state = TaskStates.TIMEOUT
            elif isinstance(e, RevokeException):
                state = TaskStates.REVOKE
            else:
                state = TaskStates.ERROR
        else:
            seagull.info('output: %s' % json.dumps(outputs, cls=ComplexJSONEncoder))
            error = ""
            if self.model.retries < self.model.config.get('max_retries', 0):
                state = TaskStates.RETRY
            else:
                state = TaskStates.ERROR

        seagull.error('task 【%s】 broken: %s' % (self.model.name, state.value))
        if state == TaskStates.RETRY:
            seagull.info('retry in %ss.' % self.model.dag.retry_countdown)

        seagull.flush(True, merge=True)
        end_time = timezone.now()
        duration = end_time - self.model.start_time
        self.model.update(
            state=state,
            end_time=end_time,
            duration=float('%s.%s' % (duration.seconds, duration.microseconds)),
            error=error,
            output=outputs or {}
        )

        self._do_callback('TASK_STATE_%s' % self.model.state)
        # propagate
        if self.parent:
            self.parent._break_off(e, outputs=outputs)

    def _finish(self, outputs={}):
        self.reload()
        if self.model.state != TaskStates.PROCESSING:
            return

        self.seagull.info('task 【%s】%s%s finished: %s'
                          % (self.model.name,
                             ' fission-%s' % self.model.fission_index if self.model.dag.fissionable else '',
                             ' iter-%s' % self.model.iter_index if self.model.dag.iterable else '',
                             TaskStates.SUCCESS.value))
        self.seagull.info('output: %s' % json.dumps(outputs, cls=ComplexJSONEncoder))

        end_time = timezone.now()
        duration = end_time - self.model.start_time

        self.seagull.flush(True, merge=True)
        self.model.update(
            output=outputs,
            state=TaskStates.SUCCESS,
            end_time=end_time,
            duration=float('%s.%s' % (duration.seconds, duration.microseconds)),
            iter_end=self._is_iter_end()
        )

        self._do_callback('TASK_STATE_%s' % TaskStates.SUCCESS)

        if self.parent:
            # 路在何方
            self._forward()

    def _forward(self):
        try:
            outputs = self.model.output
            self.parent.seagull.info('dag 【%s】%s%s finished'
                                     % (self.model.dag.name,
                                        ' fission-%s' % self.model.fission_index if self.model.dag.fissionable else '',
                                        ' iter-%s' % self.model.iter_index if self.model.dag.iterable else '',
                                        ))
            if self.model.dag.iterable:
                if not self.model.iter_end:
                    # 继续iter
                    self.parent.seagull.info('dag 【%s】%s forward to iter-%s' % (self.model.dag.name,
                                                                                ' fission-%s' % self.model.fission_index
                                                                                if self.model.dag.fissionable else '',
                                                                                self.model.iter_index + 1,
                                                                                ))
                    self.parent._iter_dag(self.model.dag, self.model.fission_index, self.model.iter_index + 1)
                    return
            # 是否是fission
            if self.model.dag.fissionable:
                # 判断兄弟们是否完成
                siblings = Task.objects.filter(parent=self.model.parent, dag=self.model.dag).exclude(iter_end=False)
                if len([s for s in siblings if s.state == TaskStates.SUCCESS]) != self.model.fission_count:
                    self.parent.seagull.flush(True)
                    return
                self.parent.seagull.info('dag 【%s】 all fissions finished' % self.model.name)
                outputs = merge_fission_outputs(*[s.output for s in siblings])

            self.parent.seagull.info(
                'dag 【%s】 output: %s' % (self.model.dag.name, json.dumps(outputs, ensure_ascii=True)))

            # 有没有后继dag和node
            next_dags, next_nodes = self.model.dag.next_dags.all(), self.model.dag.next_nodes.all()
            if next_dags or next_nodes:
                if next_dags:
                    self.parent.seagull.info('apply next dags from 【%s】...' % self.model.dag.name)
                    [self.parent._apply_dag(d) for d in next_dags]
                if next_nodes:
                    self.parent.seagull.info('apply next nodes from 【%s】...' % self.model.dag.name)
                    [self.parent._apply_node(n) for n in next_nodes]
            else:
                # 自己处于尾部, 判断父task是否完成
                parent_finished, parent_outputs = self.parent._is_finished()
                if parent_finished:
                    self.parent._finish(self.parent._adapt_outputs(parent_outputs))
        except Exception as e:
            self.parent._break_off(e)
        finally:
            self.parent.seagull.flush(True)

    def _iter_dag(self, dag, fission_index, iter_index):
        """
        :param fission_index:
        :param iter_index:
        :param dag:
        :return:
        """
        # previous_task
        previous_task = Task.objects.get(parent=self.model, dag=dag, fission_index=fission_index,
                                         iter_index=iter_index - 1,
                                         state=TaskStates.SUCCESS)

        iter_index = previous_task.iter_index + 1
        inputs = previous_task.output
        iter_context = previous_task.extra['iter_context']

        if dag.iter_config.get('key'):
            # iter by key
            inputs[iter_context['key']] = iter_context['sequence'][iter_index]

        inputs = ParamAdapter.from_json(dag.input_adapter).adapt(inputs or {})
        t, created = Task.objects.get_or_create(
            defaults=dict(
                name=dag.name,
                title=dag.title,
                state=TaskStates.PROCESSING,
                fission_count=previous_task.fission_count,
                input=inputs,
                config=self._generate_task_config(dag),
                start_time=timezone.now(),
                root=self.model.root or self.model,
                extra={'iter_context': iter_context}
            ),
            dag=dag,
            parent=self.model,
            fission_index=fission_index,
            iter_index=iter_index
        )
        s_task = SeaflowTask.get(task=t)
        if not created:
            self.seagull.warn('task 【%s】%s%s already exists'
                              % (t.name,
                                 ' fission-%s' % fission_index
                                 if dag.fissionable else '',
                                 ' iter-%s' % iter_index if dag.iterable else ''
                                 ))
            # 其他进程中已经创建了这个任务, 理论上不可能
            self.seagull.flush(True)
            return
        t.previous_tasks.set([previous_task])

        try:
            s_task.seagull.info('task 【%s】%s%s created: %s'
                                % (t.name,
                                   ' fission-%s' % fission_index
                                   if dag.fissionable else '',
                                   ' iter-%s' % iter_index if dag.iterable else '',
                                   t.id))
            # 第一批free node
            _nodes = dag.nodes.filter(previous_dags=None, previous_nodes=None)
            if _nodes:
                s_task.seagull.info('apply nodes...')
                [s_task._apply_node(n) for n in _nodes]

            # 第一批dag
            _dags = dag.children.filter(previous_dags=None, previous_nodes=None)
            if _dags:
                s_task.seagull.info('apply dags...')
                [s_task._apply_dag(d) for d in _dags]
            s_task.seagull.flush(True)
        except Exception as e:
            s_task.seagull.flush(True)
            s_task._break_off(e)

    def _iter_node(self, node, fission_index, iter_index):
        """
        :param fission_index:
        :param iter_index:
        :param node:
        :return:
        """
        # previous_step
        previous_step = Step.objects.get(task=self.model, node=node, fission_index=fission_index,
                                         iter_index=iter_index - 1,
                                         state=StepStates.SUCCESS)

        iter_index = previous_step.iter_index + 1
        inputs = previous_step.output
        iter_context = previous_step.extra['iter_context']

        if node.iter_config.get('key'):
            # iter by key
            inputs[iter_context['key']] = iter_context['sequence'][iter_index]

        inputs = ParamAdapter.from_json(node.input_adapter).adapt(inputs or {})
        s, created = Step.objects.get_or_create(
            defaults=dict(
                state=StepStates.PENDING,
                fission_count=previous_step.fission_count,
                input=inputs,
                config=self._generate_step_config(node),
                name=node.name,
                title=node.title,
                root=self.model.root or self.model,
                extra={'iter_context': iter_context}
            ),
            node=node,
            task=self.model,
            fission_index=fission_index,
            iter_index=iter_index
        )
        s_step = SeaflowStep.get(step=s)
        if not created:
            self.seagull.warn('step 【%s】%s%s already exists'
                              % (s.name,
                                 ' fission-%s' % fission_index
                                 if node.fissionable else '',
                                 ' iter-%s' % iter_index if node.iterable else ''
                                 ))
            # 其他进程中已经创建了这个任务, 理论上不可能
            self.seagull.flush(True)
            return
        s.previous_steps.set([previous_step])

        try:
            s_step.seagull.info('step 【%s】%s%s created: %s'
                                % (s.name,
                                   ' fission-%s' % previous_step.fission_index
                                   if node.fissionable else '',
                                   ' iter-%s' % iter_index if node.iterable else '',
                                   s.id))
            s_step.seagull.info('action type: %s' % node.action_type)
            if node.action_type == ActionTypes.External:
                # 外部节点
                func = core.contrib.seaflow.base.Seaflow.publish_external_step
            elif node.action_type == ActionTypes.Carrier:
                # 旁路节点
                func = core.contrib.seaflow.tasks.start_carrier_step
            else:
                func = get_func(node.action.func)

            s_step.seagull.flush(True)
            func.delay(s.id)
        except Exception as e:
            s_step.seagull.flush(True)
            s_step._break_off(e)

    def _adapt_outputs(self, outputs):
        # outputs = outputs or {}

        output_adapter = ParamAdapter.from_json(self.model.dag.output_adapter)
        outputs = output_adapter.adapt(outputs)

        return outputs

    def _is_iter_end(self):
        if not self.model.dag.iterable:
            return
        iter_context = self.model.extra['iter_context']
        if self.model.dag.iter_config.get('key'):
            return self.model.iter_index == (len(iter_context['sequence']) - 1)

    def _ready_to_execute_node(self, node):
        """
        ready to execute node in task
        :param node:
        :return:
        """
        # previous nodes
        for n in node.previous_nodes.all():
            rs = Step.objects.filter(task=self.model, node=n, state=StepStates.SUCCESS) \
                .exclude(iter_end=False)
            if not rs:
                # previous node还么有已完成的step
                return False
            if len(rs) != rs[0].fission_count:
                # previous node完成的step数没有达到额定分裂数量
                return False
        # previous dags
        for d in node.previous_dags.all():
            rs = Task.objects.filter(parent=self.model, dag=d, state=TaskStates.SUCCESS.name).exclude(iter_end=False)
            if not rs:
                # previous dag还么有已完成的task
                return False
            if len(rs) != rs[0].fission_count:
                # previous dag完成的task数没有达到额定分裂数量
                return False
        return True

    def _ready_to_execute_dag(self, dag):
        """
        ready to execute dag in task
        :param dag:
        :return:
        """

        # previous nodes
        for n in dag.previous_nodes.all():
            rs = Step.objects.filter(task=self.model, node=n, state=StepStates.SUCCESS) \
                .exclude(iter_end=False)
            if not rs:
                # previous node还么有已完成的step
                return False
            if len(rs) != rs[0].fission_count:
                # previous node完成的step数没有达到额定分裂数量
                return False
        # previous dags
        for d in dag.previous_dags.all():
            rs = Task.objects.filter(parent=self.model, dag=d, state=TaskStates.SUCCESS).exclude(iter_end=False)
            if not rs:
                # previous dag还么有已完成的task
                return False
            if len(rs) != rs[0].fission_count:
                # previous dag完成的task数没有达到额定分裂数量
                return False
        return True

    def _is_dag_finished(self, dag):
        """
        判断某个子dag是否执行完毕: dag对应的task全部完成并达到额定分裂数量
        :param dag:
        :return:
            finished: bool
            outputs: dict
        """
        rs = Task.objects.filter(parent=self.model, dag=dag, state=TaskStates.SUCCESS) \
            .exclude(iter_end=False)
        if not rs:
            # 还没有已完成的task
            return False, None
        if len(rs) != rs[0].fission_count:
            # 已完成的task数没有达到额定分裂数量
            return False, None

        if dag.fissionable:
            outputs = merge_fission_outputs(*[r.output for r in rs])
        else:
            outputs = rs[0].output

        return True, outputs

    def _is_node_finished(self, node):
        """
        判断某个node是否执行完毕
        :param node:
        :return:
        """
        rs = Step.objects.filter(task=self.model, node=node,
                                 state=StepStates.SUCCESS) \
            .exclude(iter_end=False)
        if not rs:
            # 还么有已完成的step
            return False, None
        if len(rs) != rs[0].fission_count:
            # 已完成的step数没有达到额定分裂数量
            return False, None

        if node.fissionable:
            outputs = merge_fission_outputs(*[r.output for r in rs])
        else:
            outputs = rs[0].output

        return True, outputs

    def _is_finished(self):
        """
        dag中所有的尾结点(dag/node)都执行完毕
        :return:
            finished: bool
            outputs: dict
        """
        outputs_list = []
        for dag in self.model.dag.children.filter(next_dags=None, next_nodes=None):
            finished, outputs = self._is_dag_finished(dag)
            if not finished:
                return False, None
            outputs_list.append(outputs)
        for node in self.model.dag.nodes.filter(next_dags=None, next_nodes=None):
            finished, outputs = self._is_node_finished(node)
            if not finished:
                return False, None
            outputs_list.append(outputs)

        outputs = merge_outputs(*outputs_list)

        return True, outputs

    def _is_loop_end(self, outputs):
        """
        判断下一次loop是否满足loop_condition
        :param outputs:
        :return:
        """
        data = {
            'index': self.model.loop_index + 1,
            'input': self.model.input,
            'output': outputs,
            'context': (self.model.root or self.model).context
        }

        loop_condition = self.model.dag.loop_config['condition']

        return not jsonLogic(loop_condition, data)

    def _generate_task_config(self, dag):
        """
        :param dag: sub dag
        :return:
        """
        task_config = TaskConfig.from_json(
            (self.root or self).model.extra.get('tasks_config', {}).get(dag.name, {})).trim()
        config = TaskConfig(
            max_retries=dag.max_retries,
            retry_countdown=dag.retry_countdown,
            timeout=dag.timeout,
        ).trim()
        config.update(task_config)
        return config

    def _generate_step_config(self, node):
        """
        :param node:
        :return:
        """
        step_config = TaskConfig.from_json(
            (self.root or self).model.extra.get('steps_config', {}).get(node.name, {})).trim()
        config = StepConfig(
            max_retries=node.max_retries,
            retry_countdown=node.retry_countdown,
            timeout=node.timeout,
        ).trim()
        config.update(step_config)
        return config

    def _do_callback(self, event):
        """
        :param event:
        :return:
        """
        if self.model.config.get('callback'):
            cb = self.model.config.get('callback')
            if cb['is_async']:
                do_callback.apply_async((cb['func'], event, self.model.to_json()))
            else:
                do_callback.apply((cb['func'], event, self.model.to_json()))


class SeaflowStep(object):

    def __init__(self):
        # model
        self.model = None
        self.id = None
        self.name = None
        self.context = None
        self.seagull = None
        self.config = None

        #
        self.task = None  # SeaflowTask
        self.root = None  # SeaflowTask

    @classmethod
    def get(cls, step_id=None, step=None):
        """
        :param step_id:
        :param step:
        :return: SeaflowStep
        """
        m = step or Step.objects.select_related('node', 'node__action',
                                                'task', 'root').defer('logs').get(pk=step_id)
        r = cls()
        r.model = m
        r.load()
        return r

    def load(self):
        self.id = self.model.id
        self.name = self.model.name
        self.context = self.model.root.context
        self.seagull = Seagull.instance(self.model)
        self.config = self.model.config

        self.task = core.contrib.seaflow.base.SeaflowTask.get(task=self.model.task)
        self.root = core.contrib.seaflow.base.SeaflowTask.get(task=self.model.root)

    def reload(self):
        self.model.refresh_from_db()
        self.load()

    def _dispatch(self, identity={}):
        """
        派发外部任务
        :param identity: dict，领取者的身份信息
                    name:
                    key:
        :return: identifier
        """
        if self.model.state != StepStates.PUBLISH:
            raise StateException('step is not in publish state')
        if self.model.node.action_type != ActionTypes.External:
            raise SeaflowException('step is not external')
        try:
            identifier = generate_identifier('%s-%s' % (self.id, identity.get('key')), rand=False)
            self.model.update(identifier=identifier, start_time=timezone.now(),
                              state=StepStates.PROCESSING)
            self._do_callback('STEP_STATE_%s' % StepStates.PROCESSING)
            self.seagull.info('step subscribed by %s' % (identity.get('name')))
            self.seagull.info('step execution...')
        finally:
            self.seagull.flush(True)

        return identifier

    def _execute(self, celery_action):
        celery_action.step = self.model
        celery_action.seagull = self.seagull
        try:
            celery_action.task = self.model.task
            celery_action.root = self.model.root
            celery_action.context = SeaflowContext(task=self.model.root)

            if self.model.state == StepStates.PENDING:
                # fresh new
                self.seagull.info('step 【%s】 started' % self.name)
                self.seagull.info('input: %s' % json.dumps(self.model.input, ensure_ascii=False))
                if self.model.node.loopable:
                    self.seagull.info('loop started')
                    self.seagull.info('loop-%s started' % self.model.loop_index)
            # 是否睡眠状态
            elif self.model.state == StepStates.SLEEP:
                self.seagull.info('Good Morning.')
            elif self.model.state == StepStates.PROCESSING:
                if self.model.node.loopable:
                    self.seagull.info('loop-%s started' % self.model.loop_index)

            # 判断是否进入睡眠
            sleep = False
            if self.model.task.state == TaskStates.SLEEP:
                sleep = True
                self.seagull.info('detect task 【%s】 state: %s' % (self.model.task.name, self.model.task.state))
            elif self.model.root.state == TaskStates.SLEEP:
                sleep = True
                self.seagull.info(
                    'detect root task 【%s】 state: %s' % (self.model.root.name, self.model.root.state))
            if sleep:
                self.model.update(identifier=celery_action.request.id, state=StepStates.SLEEP)
                self._do_callback('STEP_STATE_%s' % StepStates.SLEEP)
                self.seagull.info('Good Night.')
                self.seagull.flush(True)
                return

            if self.model.state in [StepStates.PENDING, StepStates.RETRY, StepStates.SLEEP]:
                # fresh new/retry/awake from sleep
                self.model.update(identifier=celery_action.request.id, start_time=timezone.now(),
                                  state=StepStates.PROCESSING)
                self._do_callback('STEP_STATE_%s' % StepStates.PROCESSING)
            else:
                # loop/recovery
                self.model.update(identifier=celery_action.request.id)

            # 是否撤销
            if self.model.task.state in [
                TaskStates.ERROR,
                TaskStates.TIMEOUT,
                TaskStates.REVOKE,
                TaskStates.TERMINATE,
            ]:
                raise RevokeException(
                    'detect task 【%s】 state: %s' % (self.model.task.name, self.model.task.state))
            if self.model.root.state in [
                TaskStates.ERROR,
                TaskStates.TIMEOUT,
                TaskStates.REVOKE,
                TaskStates.TERMINATE,
            ]:
                raise RevokeException(
                    'detect root task 【%s】 state: %s' % (self.model.root.name, self.model.root.state))

            self.seagull.info('step execution...')
            if self.model.config.get('timeout'):
                res = timeout(self.model.config.get('timeout'),
                              timeout_exception=TimeoutException)(celery_action.func)(celery_action, **self.model.input)
            else:
                res = celery_action.func(celery_action, **self.model.input)
            res = res or {}
            state, outputs = res.get('state', StepStates.SUCCESS), res.get('data', {})
            # self.seagull.info('action【%s】 output: %s' % (self.model.node.action.name,
            #                                              json.dumps(outputs, cls=ComplexJSONEncoder)))
            self.seagull.flush(True)
            outputs = self._adapt_outputs(outputs)
            if state == StepStates.SUCCESS:
                if self.model.node.loopable:
                    # loop step
                    self._loop_next(outputs)
                else:
                    # finish step
                    self._finish(outputs, state)
            else:
                self._break_off(outputs=outputs)
        except Exception as e:
            self._break_off(e)
        finally:
            del_attrs(celery_action, 'seagull', 'task', 'root_task', 'step', 'context', 'func')

    def _publish(self):
        try:
            # 是否睡眠状态
            if self.model.state == StepStates.SLEEP:
                # 唤醒
                self.seagull.info('Good Morning.')
            else:
                self.seagull.info('step 【%s】 started' % self.name)
                self.seagull.info('input: %s' % json.dumps(self.model.input, ensure_ascii=False))
                # 判断是否进入睡眠
                sleep = False
                if self.model.task.state == TaskStates.SLEEP:
                    sleep = True
                    self.seagull.info('detect task 【%s】 state: %s' % (self.model.task.name, self.model.task.state))
                elif self.model.root.state == TaskStates.SLEEP:
                    sleep = True
                    self.seagull.info(
                        'detect root task 【%s】 state: %s' % (self.model.root.name, self.model.root.state))
                if sleep:
                    self.model.update(state=StepStates.SLEEP)
                    self._do_callback('STEP_STATE_%s' % StepStates.SLEEP)
                    self.seagull.info('Good Night.')
                    self.seagull.flush(True)
                    return

            # 是否撤销
            if self.model.task.state in [
                TaskStates.ERROR,
                TaskStates.TIMEOUT,
                TaskStates.REVOKE,
                TaskStates.TERMINATE,
            ]:
                raise RevokeException(
                    'detect task 【%s】 state: %s' % (self.model.task.name, self.model.task.state))
            if self.model.root.state in [
                TaskStates.ERROR,
                TaskStates.TIMEOUT,
                TaskStates.REVOKE,
                TaskStates.TERMINATE,
            ]:
                raise RevokeException(
                    'detect root task 【%s】 state: %s' % (self.model.root.name, self.model.root.state))

            self.model.update(state=StepStates.PUBLISH)
            self._do_callback('STEP_STATE_%s' % StepStates.PUBLISH)
            self.seagull.info('step published')
            self.seagull.flush(True)
        except Exception as e:
            self._break_off(e)

    def _carry(self, celery_action):
        try:
            self.model.update(identifier=celery_action.request.id, start_time=timezone.now(),
                              state=StepStates.PROCESSING)
            self._do_callback('STEP_STATE_%s' % StepStates.PROCESSING)
            self.seagull.info('step 【%s】 started' % self.name)
            # 是否撤销
            revoke = self.model.task.state in [
                TaskStates.ERROR,
                TaskStates.TIMEOUT,
                TaskStates.REVOKE,
                TaskStates.TERMINATE,
            ] or self.model.root.state in [
                         TaskStates.ERROR,
                         TaskStates.TIMEOUT,
                         TaskStates.REVOKE,
                         TaskStates.TERMINATE,
                     ]

            if revoke:
                raise RevokeException(
                    'detect task 【%s】 state: %s' % (self.model.task.name, self.model.task.state))

            self.seagull.flush(True)
            self._finish(self._adapt_outputs(self.model.input))
        except Exception as e:
            self._break_off(e)

    def _finish(self, outputs={}, state=StepStates.SUCCESS):
        self.reload()
        if self.model.state != StepStates.PROCESSING:
            return

        self.seagull.info('step 【%s】%s%s finished: %s'
                          % (self.name,
                             ' fission-%s' % self.model.fission_index if self.model.node.fissionable else '',
                             ' iter-%s' % self.model.iter_index if self.model.node.iterable else '',
                             state.value))
        self.seagull.info('output: %s' % json.dumps(outputs, cls=ComplexJSONEncoder))

        end_time = timezone.now()
        duration = end_time - self.model.start_time

        self.seagull.flush(True, merge=True)
        self.model.update(
            state=state,
            end_time=end_time,
            duration=float('%s.%s' % (duration.seconds, duration.microseconds)),
            output=outputs or {},
            iter_end=self._is_iter_end()
        )
        # 路在脚下
        self._forward()

    def _break_off(self, e=None, outputs={}):
        self.reload()
        if self.model.state not in [StepStates.PENDING, StepStates.PUBLISH, StepStates.PROCESSING, StepStates.RETRY,
                                    StepStates.SLEEP]:
            return

        if e:
            error = self.seagull.trace_error(e)
            if self.model.retries < self.model.config.get('max_retries', 0) and not isinstance(e, RevokeException):
                state = StepStates.RETRY
            elif isinstance(e, TimeoutException):
                state = StepStates.TIMEOUT
            elif isinstance(e, RevokeException):
                state = StepStates.REVOKE
            else:
                state = StepStates.ERROR
        else:
            self.seagull.info('output: %s' % json.dumps(outputs, cls=ComplexJSONEncoder))
            error = ""
            if self.model.retries < self.model.config.get('max_retries', 0):
                state = StepStates.RETRY
            else:
                state = StepStates.ERROR

        self.seagull.error('step 【%s】 broken: %s' % (self.name, state.value))
        if state == StepStates.RETRY:
            self.seagull.info('retry in %ss.' % self.model.node.retry_countdown)
        self.seagull.flush(True, merge=True)

        end_time = timezone.now()
        duration = (end_time - self.model.start_time) if self.model.start_time else None
        self.model.update(
            state=state,
            retries=self.model.retries + 1 if state == StepStates.RETRY else self.model.retries,
            end_time=None if state == StepStates.RETRY else end_time,
            duration=None if (not duration or state == StepStates.RETRY) else float(
                '%s.%s' % (duration.seconds, duration.microseconds)),
            error=error,
            output=outputs or {}
        )
        if state == StepStates.RETRY:
            # celery_task.retry(countdown=step.node.retry_countdown, exc=e, throw=False)
            get_func(self.model.node.action.func).apply_async((self.id,),
                                                              countdown=self.model.config.get('retry_countdown', 0))
        else:
            # propagate
            self.task._break_off(e, outputs=outputs)

    def _loop_next(self, outputs={}):
        self.reload()
        if self.model.state != StepStates.PROCESSING:
            return

        self.seagull.info(
            'loop-%s end, output: %s' % (self.model.loop_index, json.dumps(outputs, cls=ComplexJSONEncoder)))
        loop_end = self._is_loop_end(outputs)
        if loop_end:
            self.seagull.info('loop end')
            self._finish(outputs)
        else:
            self.model.update(loop_index=self.model.loop_index + 1, _refresh=False)
            countdown = self.model.node.loop_config.get('countdown', 0)
            self.seagull.info('sleep %ss...' % countdown)
            get_func(self.model.node.action.func).apply_async((self.id,), countdown=countdown)

    def _forward(self):
        try:
            outputs = self.model.output
            self.task.seagull.info('node 【%s】%s%s finished'
                                   % (self.model.node.name,
                                      ' fission-%s' % self.model.fission_index if self.model.node.fissionable else '',
                                      ' iter-%s' % self.model.iter_index if self.model.node.iterable else '',
                                      ))
            if self.model.node.iterable:
                if not self.model.iter_end:
                    # 继续iter
                    self.task.seagull.info('node 【%s】%s forward to iter-%s'
                                           % (self.model.node.name,
                                              ' fission-%s' % self.model.fission_index if self.model.node.fissionable else '',
                                              self.model.iter_index + 1,
                                              ))
                    self.task._iter_node(self.model.node, self.model.fission_index, self.model.iter_index + 1)
                    return

            # 是否是fission
            if self.model.node.fissionable:
                # 判断兄弟们是否完成
                siblings = Step.objects.filter(task=self.model.task, node=self.model.node).exclude(
                    iter_end=False)
                if len([s for s in siblings if
                        s.state == StepStates.SUCCESS]) != self.model.fission_count:
                    self.task.seagull.flush(True)
                    return
                self.task.seagull.info('node 【%s】 all fissions finished' % self.model.node.name)
                outputs = merge_fission_outputs(*[s.output for s in siblings])

            self.task.seagull.info(
                'node【%s】 output: %s' % (self.model.node.name, json.dumps(outputs, ensure_ascii=True)))

            # 有没有后继dag和node
            next_dags, next_nodes = self.model.node.next_dags.all(), self.model.node.next_nodes.all()
            if next_dags or next_nodes:
                if next_dags:
                    self.task.seagull.info('apply next dags from 【%s】...' % self.model.node.name)
                    [self.task._apply_dag(d) for d in next_dags]
                if next_nodes:
                    self.task.seagull.info('apply next nodes from 【%s】...' % self.model.node.name)
                    [self.task._apply_node(n) for n in next_nodes]
            else:
                # 自己处于尾部，判断所在task是否完成
                task_finished, task_outputs = self.task._is_finished()
                if task_finished:
                    self.task._finish(task_outputs)
        except Exception as e:
            self.task._break__off(e)
        finally:
            self.task.seagull.flush(True)

    def _adapt_outputs(self, outputs):
        # outputs = outputs or {}

        output_def = ParamDefinition.from_json(self.model.node.action.output_def)
        if not (self.model.node.action_type == ActionTypes.Carrier and not output_def):
            # carrier特殊处理
            valid, errors = output_def.validate(outputs)
            if not valid:
                self.seagull.error('validate output failed: expect %s, got %s'
                                   % (output_def.dump(),
                                      json.dumps(outputs,
                                                 cls=ComplexJSONEncoder,
                                                 ensure_ascii=False)))
                self.seagull.flush(True)
                raise ParamDefinitionException(*errors.keys())
        output_adapter = ParamAdapter.from_json(self.model.node.output_adapter)
        if not (self.model.node.action_type == ActionTypes.Carrier and not output_adapter):
            outputs = output_adapter.adapt(outputs)

        return outputs

    def _is_iter_end(self):
        if not self.model.node.iterable:
            return
        iter_context = self.model.extra['iter_context']
        if self.model.node.iter_config.get('key'):
            return self.model.iter_index == (len(iter_context['sequence']) - 1)

    def _is_loop_end(self, outputs):
        """
        判断下一次次loop是否满足loop_condition
        :param outputs:
        :return:
        """

        data = {
            'index': self.model.loop_index + 1,
            'input': self.model.input,
            'output': outputs,
            'context': (self.model.root or self.model.task).context
        }

        loop_condition = self.model.node.loop_config['condition']

        return not jsonLogic(loop_condition, data)

    def _do_callback(self, event):
        """
        :param event:
        :return:
        """
        if self.model.config.get('callback'):
            cb = self.model.config.get('callback')
            if cb['is_async']:
                do_callback.apply_async((cb['func'], event, self.model.to_json()))
            else:
                do_callback.apply((cb['func'], event, self.model.to_json()))
