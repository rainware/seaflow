# Generated by Django 3.1 on 2022-07-22 04:55

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Action',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(db_index=True, max_length=64, unique=True)),
                ('title', models.CharField(max_length=64)),
                ('type', models.CharField(choices=[('Default', 'default'), ('Carrier', 'carrier'), ('External', 'external')], max_length=32)),
                ('func', models.CharField(max_length=128, null=True, unique=True)),
                ('input_def', models.JSONField(default=dict)),
                ('output_def', models.JSONField(default=dict)),
                ('create_time', models.DateTimeField(auto_now_add=True)),
                ('update_time', models.DateTimeField(auto_now=True)),
            ],
            options={
                'verbose_name': '流程Action',
                'verbose_name_plural': '流程Action',
                'db_table': 'seaflow_action',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='Dag',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(db_index=True, max_length=64)),
                ('title', models.CharField(max_length=64)),
                ('version', models.PositiveIntegerField(db_index=True, null=True)),
                ('latest', models.BooleanField(null=True)),
                ('input_adapter', models.JSONField(default=dict)),
                ('output_adapter', models.JSONField(default=dict)),
                ('fissionable', models.BooleanField(default=False, verbose_name='可分裂')),
                ('fission_config', models.JSONField(default=dict)),
                ('iterable', models.BooleanField(default=False, verbose_name='可迭代')),
                ('iter_config', models.JSONField(default=dict)),
                ('loopable', models.BooleanField(default=False, verbose_name='可循环')),
                ('loop_config', models.JSONField(default=dict)),
                ('max_retries', models.IntegerField(default=0)),
                ('retry_countdown', models.IntegerField(default=0)),
                ('timeout', models.IntegerField(null=True)),
                ('create_time', models.DateTimeField(auto_now_add=True)),
                ('update_time', models.DateTimeField(auto_now=True)),
                ('parent', models.ForeignKey(db_constraint=False, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='children', to='seaflow.dag')),
                ('previous_dags', models.ManyToManyField(db_constraint=False, db_table='seaflow_dag_to_dag', related_name='next_dags', to='seaflow.Dag')),
            ],
            options={
                'verbose_name': '流程图',
                'verbose_name_plural': '流程图',
                'db_table': 'seaflow_dag',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='Log',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('ref_type', models.CharField(choices=[('TASK', 'Task'), ('STEP', 'Step')], max_length=16)),
                ('ref_id', models.IntegerField(db_index=True)),
                ('ts', models.FloatField()),
                ('content', models.TextField()),
            ],
            options={
                'verbose_name': '日志',
                'verbose_name_plural': '日志',
                'db_table': 'seaflow_log',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='Node',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=64)),
                ('title', models.CharField(max_length=64)),
                ('input_adapter', models.JSONField(default=dict)),
                ('output_adapter', models.JSONField(default=dict)),
                ('action_type', models.CharField(choices=[('Default', 'default'), ('Carrier', 'carrier'), ('External', 'external')], max_length=32)),
                ('fissionable', models.BooleanField(default=False, verbose_name='可分裂')),
                ('fission_config', models.JSONField(default=dict)),
                ('iterable', models.BooleanField(default=False, verbose_name='可迭代')),
                ('iter_config', models.JSONField(default=dict)),
                ('loopable', models.BooleanField(default=False, verbose_name='可循环')),
                ('loop_config', models.JSONField(default=dict)),
                ('max_retries', models.IntegerField(default=0)),
                ('retry_countdown', models.IntegerField(default=0)),
                ('timeout', models.IntegerField(null=True)),
                ('create_time', models.DateTimeField(auto_now_add=True)),
                ('update_time', models.DateTimeField(auto_now=True)),
                ('action', models.ForeignKey(db_constraint=False, on_delete=django.db.models.deletion.PROTECT, to='seaflow.action')),
                ('dag', models.ForeignKey(db_constraint=False, on_delete=django.db.models.deletion.CASCADE, related_name='nodes', to='seaflow.dag')),
                ('previous_dags', models.ManyToManyField(db_constraint=False, db_table='seaflow_dag_to_node', related_name='next_nodes', to='seaflow.Dag')),
                ('previous_nodes', models.ManyToManyField(db_constraint=False, db_table='seaflow_node_to_node', related_name='next_nodes', to='seaflow.Node')),
                ('root_dag', models.ForeignKey(db_constraint=False, on_delete=django.db.models.deletion.CASCADE, related_name='all_nodes', to='seaflow.dag')),
            ],
            options={
                'verbose_name': '流程节点',
                'verbose_name_plural': '流程节点',
                'db_table': 'seaflow_node',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='Step',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('identifier', models.CharField(db_index=True, max_length=64, null=True)),
                ('name', models.CharField(max_length=64)),
                ('title', models.CharField(max_length=64)),
                ('state', models.CharField(choices=[('PENDING', '等待中'), ('PUBLISH', '已发布'), ('PROCESSING', '正在执行'), ('SLEEP', '休眠'), ('RETRY', '等待重试'), ('TIMEOUT', '超时'), ('SUCCESS', '成功'), ('ERROR', '错误'), ('REVOKE', '撤销'), ('TERMINATE', '终止')], max_length=64)),
                ('config', models.JSONField(default=dict, verbose_name='配置')),
                ('retries', models.IntegerField(default=0, verbose_name='重试次数')),
                ('fission_count', models.IntegerField(default=1, verbose_name='分裂数')),
                ('fission_index', models.IntegerField(db_index=True, default=0, verbose_name='分裂序号')),
                ('iter_index', models.IntegerField(db_index=True, default=0, verbose_name='迭代序号')),
                ('iter_end', models.BooleanField(null=True, verbose_name='迭代末端')),
                ('loop_index', models.IntegerField(default=0, verbose_name='循环序号')),
                ('input', models.JSONField(verbose_name='输入')),
                ('output', models.JSONField(default=dict, verbose_name='输出')),
                ('extra', models.JSONField(default=dict)),
                ('error', models.TextField(default='', verbose_name='错误信息')),
                ('logs', models.JSONField(default=list, verbose_name='执行日志')),
                ('logfile', models.CharField(default='', max_length=256, verbose_name='s3日志文件路径')),
                ('log_refetch_time', models.DateTimeField(db_index=True, null=True)),
                ('start_time', models.DateTimeField(null=True)),
                ('end_time', models.DateTimeField(null=True)),
                ('duration', models.FloatField(null=True)),
                ('create_time', models.DateTimeField(auto_now_add=True)),
                ('update_time', models.DateTimeField(auto_now=True)),
                ('node', models.ForeignKey(db_constraint=False, on_delete=django.db.models.deletion.PROTECT, to='seaflow.node')),
                ('previous_steps', models.ManyToManyField(db_constraint=False, db_table='seaflow_step_to_step', related_name='next_steps', to='seaflow.Step')),
            ],
            options={
                'verbose_name': '任务步骤',
                'verbose_name_plural': '任务步骤',
                'db_table': 'seaflow_step',
                'managed': True,
            },
        ),
        migrations.CreateModel(
            name='Task',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=64)),
                ('title', models.CharField(max_length=64)),
                ('state', models.CharField(choices=[('PENDING', '等待中'), ('PROCESSING', '正在执行'), ('SLEEP', '休眠'), ('RETRY', '等待重试'), ('TIMEOUT', '超时'), ('SUCCESS', '成功'), ('ERROR', '错误'), ('REVOKE', '撤销'), ('TERMINATE', '终止')], max_length=64)),
                ('config', models.JSONField(default=dict, verbose_name='配置')),
                ('retries', models.IntegerField(default=0, verbose_name='重试次数')),
                ('fission_count', models.IntegerField(default=1, verbose_name='分裂数')),
                ('fission_index', models.IntegerField(db_index=True, default=0, verbose_name='分裂序号')),
                ('iter_index', models.IntegerField(db_index=True, default=0, verbose_name='迭代序号')),
                ('iter_end', models.BooleanField(null=True, verbose_name='迭代末端')),
                ('loop_index', models.IntegerField(default=0, verbose_name='循环序号')),
                ('input', models.JSONField(verbose_name='输入')),
                ('context', models.JSONField(default=dict, verbose_name='上下文')),
                ('output', models.JSONField(default=dict, verbose_name='输出')),
                ('extra', models.JSONField(default=dict)),
                ('error', models.TextField(default='', verbose_name='错误信息')),
                ('logs', models.JSONField(default=list, verbose_name='执行日志')),
                ('logfile', models.CharField(default='', max_length=256, verbose_name='s3日志文件路径')),
                ('log_refetch_time', models.DateTimeField(db_index=True, null=True)),
                ('start_time', models.DateTimeField(null=True)),
                ('end_time', models.DateTimeField(null=True)),
                ('duration', models.FloatField(null=True)),
                ('create_time', models.DateTimeField(auto_now_add=True)),
                ('update_time', models.DateTimeField(auto_now=True)),
                ('dag', models.ForeignKey(db_constraint=False, on_delete=django.db.models.deletion.PROTECT, to='seaflow.dag')),
                ('parent', models.ForeignKey(db_constraint=False, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='children', to='seaflow.task')),
                ('previous_steps', models.ManyToManyField(db_constraint=False, db_table='seaflow_step_to_task', related_name='next_tasks', to='seaflow.Step')),
                ('previous_tasks', models.ManyToManyField(db_constraint=False, db_table='seaflow_task_to_task', related_name='next_tasks', to='seaflow.Task')),
                ('root', models.ForeignKey(db_constraint=False, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='descendants', to='seaflow.task')),
            ],
            options={
                'verbose_name': '任务',
                'verbose_name_plural': '任务',
                'db_table': 'seaflow_task',
                'managed': True,
                'unique_together': {('parent', 'dag', 'fission_index', 'iter_index')},
            },
        ),
        migrations.AddField(
            model_name='step',
            name='previous_tasks',
            field=models.ManyToManyField(db_constraint=False, db_table='seaflow_task_to_step', related_name='next_steps', to='seaflow.Task'),
        ),
        migrations.AddField(
            model_name='step',
            name='root',
            field=models.ForeignKey(db_constraint=False, on_delete=django.db.models.deletion.CASCADE, related_name='all_steps', to='seaflow.task'),
        ),
        migrations.AddField(
            model_name='step',
            name='task',
            field=models.ForeignKey(db_constraint=False, on_delete=django.db.models.deletion.CASCADE, related_name='steps', to='seaflow.task'),
        ),
        migrations.AddField(
            model_name='dag',
            name='previous_nodes',
            field=models.ManyToManyField(db_constraint=False, db_table='seaflow_node_to_dag', related_name='next_dags', to='seaflow.Node'),
        ),
        migrations.AddField(
            model_name='dag',
            name='root',
            field=models.ForeignKey(db_constraint=False, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='descendants', to='seaflow.dag'),
        ),
        migrations.AlterUniqueTogether(
            name='step',
            unique_together={('task', 'node', 'fission_index', 'iter_index')},
        ),
    ]
