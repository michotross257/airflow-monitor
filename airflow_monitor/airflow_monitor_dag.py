from datetime import datetime, timedelta, timezone
import time
from typing import Dict

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import SkipMixin, Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.slack_operator import SlackAPIPostOperator


SELF_DAG_ID = 'airflow_monitor_dag'  # ID of this dag
POSTGRES_CONN_ID = 'airflow_postgres'
airflow_db_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
slack_alert = SlackAPIPostOperator(
    task_id='None',
    channel=Variable.get('slack_channel'),
    token=Variable.get('slack_password')
)
sql_statements = {
    'exists': \
        '''
        SELECT * from information_schema.tables
        WHERE table_name = 'dag_condition';
        ''',
    'conditions': \
        '''
        SELECT * FROM dag_condition;
        ''',
    'button': \
        '''
        SELECT dag_id, is_paused
        FROM dag
        WHERE is_active;
        ''',
    'runtime': \
        '''
        SELECT dr.dag_id, start_date
        FROM dag_run dr
        LEFT JOIN dag d
        ON dr.dag_id = d.dag_id
        WHERE is_active
        AND state = 'running';
        '''
}


class SkipTaskPythonOperator(PythonOperator, SkipMixin):
    """
    Skip select tasks rather all downstream tasks like in BranchPythonOperator
    in the case where branches converge to the same downstream tasks.

    The tasks that should be skipped are passed as a list of tasks (task meaning
    any Airflow object that can be a node in a DAG - e.g. PythonOperator; not
    just task ids) to the kwarg `tasks_to_skip` and these tasks are skipped (or
    not) based on the function passed to the kwarg `python_callable`.
    """
    def execute(self, context: Dict):
        tasks = super().execute(context)
        if not len(tasks):
            self.log.info('No tasks to skip.')
            return
        self.log.info('Skipping tasks...')
        self.log.debug("Task_ids %s", tasks)
        self.skip(context['dag_run'], context['ti'].execution_date, tasks)
        self.log.info("Done - tasks skipped.")


def check_dag_condition_table_exists(hook, query, tasks_to_skip):
    """
    Check to see if the dag_condition table exists.

    Args:
        hook (PostgresHook): the hook to use to run the query
        query (str): the query to run to check for the existence of the dag_condition table
        tasks_to_skip (list): list of Airflow operator objects (e.g. PythonOperator)

    Returns:
        list: either list of Airflow operator objects or empty list
    """
    table_exists = hook.get_records(query)
    if len(table_exists):
        return tasks_to_skip
    return []


def wait_for_conditions_table(hook, query, poke_interval_seconds):
    """
    Sensor to check to see if the dag_condition table exists.

    Args:
        hook (PostgresHook): the hook to use to run the query
        query (str): the query to run to check for the existence of the dag_condition table
        poke_interval_seconds (int): number of seconds to wait between checks
                                     of the existence of the dag_condition table

    Returns:
        None
    """
    while True:
        table_exists = hook.get_records(query)
        if len(table_exists):
            return
        time.sleep(poke_interval_seconds)


def send_slack_alert(slack_alert, msg):
    """
    Send a slack alert.

    Args:
        slack_alert (SlackAPIPostOperator): Operator used to post message on Slack
        msg (str): the message of the slack alert

    Returns:
        None
    """
    slack_alert.text = msg
    slack_alert.execute()


def get_dag_conditions_data(hook, query, **context):
    """
    Get data from dag_conditions table. Parse into two dicts - one for runtime
    evaluation and one for button status evaluation.

    Args:
        hook (PostgresHook): the hook to use to run the query
        query (str): the query to run to return data from the dag_condition table
        **context (dict): dictionary providing the context of the task execution

    Returns:
        None
    """
    records = hook.get_records(query)
    context['ti'].xcom_push(key='runtime', value={x[0]: x[1] for x in records})
    context['ti'].xcom_push(key='button', value={x[0]: x[2] for x in records})


def check_button_statuses(slack_alert, **context):
    """
    Check the button status of the DAG and determine if it is as expected.
    If button is 'on' then the DAG is not paused, so the equivalent `is_paused` value is False.
    If button is 'off' then the DAG is paused, so the equivalent `is_paused` value is True.

    Args:
        slack_alert (SlackAPIPostOperator): Operator used to post message on Slack
        **context (dict): dictionary providing the context of the task execution

    Returns:
        None
    """
    dag_records = context['ti'].xcom_pull(task_ids='get_button_status_data')
    dag_conditions = context['ti'].xcom_pull(key='button', task_ids='get_dag_conditions_data')
    is_paused_to_button_status = {False: 'on', True: 'off'}
    for record in dag_records:
        dag_id, is_paused = record
        if dag_conditions.get(dag_id) and is_paused != dag_conditions.get(dag_id):
            msg = '''
            :radio_button: DAG on/off button needs to be toggled.
            *Dag*: {}
            *Current Button Status*: {}
            '''.format(dag_id, is_paused_to_button_status[is_paused])
            send_slack_alert(slack_alert, msg)


def check_runtimes(slack_alert, **context):
    """
    Check the runtime of the DAG and determine if it is greater than expected.

    Args:
        slack_alert (SlackAPIPostOperator): Operator used to post message on Slack
        **context (dict): dictionary providing the context of the task execution

    Returns:
        None
    """
    dag_records = context['ti'].xcom_pull(task_ids='get_runtime_data')
    dag_conditions = context['ti'].xcom_pull(key='runtime', task_ids='get_dag_conditions_data')
    for record in dag_records:
        dag_id, start_date = record
        time_diff = (datetime.now(tz=timezone.utc) - start_date).total_seconds()
        runtime_threshold = dag_conditions.get(dag_id)
        if runtime_threshold and time_diff > runtime_threshold:
            msg = '''
            :spinner: DAG is running longer than expected.
            *Dag*: {}
            *Start Date*: {}
            *Time Running*: ~{:,} seconds
            *Runtime Threshold*: {:,} seconds
            '''.format(dag_id, start_date, time_diff, runtime_threshold)
            send_slack_alert(slack_alert, msg)


# ================ DAG definition ================
default_args = {
    'owner': 'airflow',
    'description': 'Hourly monitor of DAGs',
    'start_date': datetime(year=2020, month=4, day=14),
    'depend_on_past': False,
    'email_on_failure': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id=SELF_DAG_ID,
    default_args=default_args,
    schedule_interval='@hourly'
)

# ================ task definitions ================
start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)
check_statuses_task = DummyOperator(task_id='check_statuses', dag=dag)

trigger_condition_table_dag_task = TriggerDagRunOperator(
    task_id='trigger_condition_table_dag',
    trigger_dag_id='condition_table_dag',
    dag=dag
)

condition_table_sensor_task = PythonOperator(
    task_id='condition_table_sensor',
    python_callable=wait_for_conditions_table,
    execution_timeout=timedelta(seconds=300),
    op_kwargs={
        'hook': airflow_db_hook,
        'query': sql_statements['exists'],
        'poke_interval_seconds': 60
    },
    dag=dag)

dag_conditions_branch_task = SkipTaskPythonOperator(
    task_id='dag_conditions_trigger_branch',
    python_callable=check_dag_condition_table_exists,
    op_kwargs={
        'hook': airflow_db_hook,
        'query': sql_statements['exists'],
        'tasks_to_skip': [trigger_condition_table_dag_task, condition_table_sensor_task]
    },
    dag=dag
)

get_dag_conditions_data_task = PythonOperator(
    task_id='get_dag_conditions_data',
    python_callable=get_dag_conditions_data,
    op_kwargs={
        'hook': airflow_db_hook,
        'query': sql_statements['conditions']
    },
    trigger_rule='all_done',
    provide_context=True,
    dag=dag
)

get_button_status_data_task = PythonOperator(
    task_id='get_button_status_data',
    python_callable=lambda: airflow_db_hook.get_records(sql_statements['button']),
    dag=dag
)

get_runtime_data_task = PythonOperator(
    task_id='get_runtime_data',
    python_callable=lambda: airflow_db_hook.get_records(sql_statements['runtime']),
    dag=dag
)

check_button_statuses_task = PythonOperator(
    task_id='check_button_statuses',
    python_callable=check_button_statuses,
    op_kwargs={
        'slack_alert': slack_alert
    },
    provide_context=True,
    dag=dag
)

check_runtimes_task = PythonOperator(
    task_id='check_runtimes',
    python_callable=check_runtimes,
    op_kwargs={
        'slack_alert': slack_alert
    },
    provide_context=True,
    dag=dag
)

# ================ set up dependencies ================
start_task >> dag_conditions_branch_task >> trigger_condition_table_dag_task >> \
condition_table_sensor_task >> get_dag_conditions_data_task >> \
[get_button_status_data_task, get_runtime_data_task] >> check_statuses_task >> \
[check_button_statuses_task, check_runtimes_task] >> end_task
