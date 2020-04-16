from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd


"""
For a given DAG, the time threshold is equal to:
mean of runtimes + (standard deviation of runtimes * NUM_ST_DEVS)
NOTE: we only care about an upper time threshold
"""
NUM_ST_DEVS = 2
POSTGRES_CONN_ID = 'airflow_postgres'
airflow_db_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
sql_statements = {
    'create': \
        '''
        CREATE TABLE IF NOT EXISTS dag_condition (
            dag_id text PRIMARY KEY,
            time_threshold_in_seconds int NOT NULL,
            on_off_button_status text NOT NULL
        );
        ''',
    'select': \
        '''
        SELECT dag_run.dag_id, start_date, end_date, state, is_paused
        FROM dag_run
        LEFT JOIN dag ON dag_run.dag_id = dag.dag_id
        WHERE start_date IS NOT NULL
        AND end_date IS NOT NULL;
        '''
}


def analyze_data(num_st_devs, **context):
    """
    Determine the time thresholds for the DAGs based on historical runtime data.

    Args:
        num_st_devs (int): the number of standard deviations to use to determine the
                           upper runtime threshold for a DAG
        **context (dict): dictionary providing the context of the task execution

    Returns:
        dict: Expected conditions for each dag_id.
    """
    query_result = context['ti'].xcom_pull(task_ids='get_runtime_data')
    dag_conditions = {}
    if len(query_result):
        df = pd.DataFrame(query_result, columns=['dag_id', 'start_date', 'end_date', 'state', 'is_paused'])
        # create elapsed time column
        df['elapsed_time_in_seconds'] = df.end_date - df.start_date
        df['elapsed_time_in_seconds'] = df['elapsed_time_in_seconds'].apply(lambda x: x.total_seconds())
        # remove outliers
        for dag_id in df.dag_id.unique():
            q1, q3 = df[df.dag_id == dag_id].elapsed_time_in_seconds.quantile([0.25, 0.75]).values
            outlier_threshold = q3 + 1.5 * (q3 - q1)
            indexes = df[(df.dag_id == dag_id) & (df.elapsed_time_in_seconds > outlier_threshold)].index
            df.drop(indexes, inplace=True)
        # accumulate statistics
        elapsed_mean = df.groupby(['dag_id']).elapsed_time_in_seconds.mean()
        elapsed_std = df.groupby(['dag_id']).elapsed_time_in_seconds.std()
        button_statuses = df.groupby('dag_id').is_paused.max()
        # convert is_paused to button_status
        is_paused_to_button_status = {False: 'on', True: 'off'}
        button_statuses.name = 'button_status'
        button_statuses = button_statuses.apply(lambda x: is_paused_to_button_status[x])
        results = pd.concat([elapsed_mean, elapsed_std], axis=1, keys=['elapsed_mean', 'elapsed_std'])
        results['time_threshold'] = results['elapsed_mean'] + num_st_devs * results['elapsed_std']
        # fill in NaN values resulting from std given only one occurrence of a dag_id
        results.fillna(method='pad', axis=1, inplace=True)
        results = pd.concat([results, button_statuses], axis=1)
        for dag_id in results.index:
            dag_conditions[dag_id] = {
                'time_threshold_in_seconds': int(results.loc[dag_id, 'time_threshold']),
                'on_off_button_status': results.loc[dag_id, 'button_status']
            }

    return dag_conditions


def upsert_dag_conditions(hook, **context):
    """
    Insert or update into table the values that were returned after
    querying and extracting data.

    NOTE: updating the table with the current on/off button status doesn't make sense
    because then the monitor will see this value as the expected value and will
    compare it to the current value, which will always be the same. So if we want
    to update the button/status, then we should do a manual update of the table.

    Args:
        hook (PostgresHook): the hook to use to run upsert statement
        **context (dict): dictionary providing the context of the task execution

    Returns:
        None
    """
    data = context['ti'].xcom_pull(task_ids='analyze_runtime_data')
    if len(data):
        query = ''
        for cnt, dag_id in enumerate(data, start=1):
            comma = ',' if cnt < len(data) else ''
            space = ' ' if cnt < len(data) else ''
            record = (dag_id, data[dag_id]['time_threshold_in_seconds'], data[dag_id]['on_off_button_status'])
            if cnt == 1:
                query += "INSERT INTO dag_condition (dag_id, time_threshold_in_seconds, on_off_button_status)"
                query += "\nVALUES {}{}{}".format(record, comma, space)
            else:
                query += "{}{}{}".format(record, comma, space)
        query += "\nON CONFLICT (dag_id) DO UPDATE"
        query += "\nSET time_threshold_in_seconds = EXCLUDED.time_threshold_in_seconds;"
        hook.run(query)


# ================ DAG definition ================
default_args = {
    'owner': 'airflow',
    'description': "Create and update dag_condition table.",
    'start_date': datetime(year=2020, month=4, day=14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='condition_table_dag',
    default_args=default_args,
    schedule_interval='@daily'
)

# ================ task definitions ================
start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

create_dag_condition_table_task = PostgresOperator(
    task_id='create_dag_condition_table',
    sql=sql_statements['create'],
    postgres_conn_id=POSTGRES_CONN_ID,
    dag=dag
)

get_runtime_data_task = PythonOperator(
    task_id='get_runtime_data',
    python_callable=lambda: airflow_db_hook.get_records(sql_statements['select']),
    dag=dag
)

analyze_runtime_data_task = PythonOperator(
    task_id='analyze_runtime_data',
    python_callable=analyze_data,
    op_kwargs={
        'num_st_devs': NUM_ST_DEVS
    },
    provide_context=True,
    dag=dag
)

upsert_dag_conditions_task = PythonOperator(
    task_id='upsert_dag_conditions',
    python_callable=upsert_dag_conditions,
    op_kwargs={
        'hook': airflow_db_hook
    },
    provide_context=True,
    dag=dag
)

# ================ set up dependencies ================
start_task >> create_dag_condition_table_task >> get_runtime_data_task >> \
analyze_runtime_data_task >> upsert_dag_conditions_task >> end_task
