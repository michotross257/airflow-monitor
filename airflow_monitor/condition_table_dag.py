from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd


"""
For a given DAG, the runtime threshold is equal to:
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
            dag_id TEXT PRIMARY KEY,
            runtime_threshold_in_seconds INT NOT NULL DEFAULT 86400,
            is_paused__expected BOOL NOT NULL DEFAULT FALSE
        );
        ''',
    'select': \
        '''
        SELECT
            dr.dag_id,
            start_date,
            end_date,
            coalesce(dc.is_paused__expected, d.is_paused) as is_paused
        FROM dag_run dr
        LEFT JOIN dag d
            ON dr.dag_id = d.dag_id
        LEFT JOIN dag_condition dc
            ON dr.dag_id = dc.dag_id
        WHERE start_date IS NOT NULL
        AND end_date IS NOT NULL
        AND is_active;
        '''
}


def analyze_data(num_st_devs, **context):
    """
    Determine the runtime thresholds for the DAGs based on historical runtime data.

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
        df = pd.DataFrame(query_result, columns=['dag_id', 'start_date', 'end_date', 'is_paused'])
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
        button_statuses.name = 'button_status'
        results = pd.concat([elapsed_mean, elapsed_std], axis=1, keys=['elapsed_mean', 'elapsed_std'])
        results['runtime_threshold'] = results['elapsed_mean'] + num_st_devs * results['elapsed_std']
        # fill in NaN values resulting from std given only one occurrence of a dag_id
        results.fillna(method='pad', axis=1, inplace=True)
        dag_conditions = pd.concat([results, button_statuses], axis=1)
        dag_conditions = dag_conditions.astype({'runtime_threshold': int})
        dag_conditions = dag_conditions.rename(columns={
            'runtime_threshold': 'runtime_threshold_in_seconds',
            'button_status': 'is_paused__expected'
        })[['runtime_threshold_in_seconds', 'is_paused__expected']].to_dict()

    return dag_conditions


def truncate_and_insert_dag_condition(hook, **context):
    """
    Truncate then bulk insert the data to the dag_condition table.

    Args:
        hook (PostgresHook): the hook to use to run upsert statement
        **context (dict): dictionary providing the context of the task execution

    Returns:
        None
    """
    data = context['ti'].xcom_pull(task_ids='analyze_runtime_data')
    if len(data):
        print('Truncating dag_condition table...')
        hook.run('truncate dag_condition;')
        print('Done - dag_condition table truncated.')
        dag_conditions = pd.DataFrame(data)
        print('Inserting data into dag_condition table...')
        dag_conditions.to_sql(
            name='dag_condition',
            con=hook.get_sqlalchemy_engine(),
            if_exists='append',
            index_label='dag_id',
            method='multi'
        )
        print('Done - data inserted into dag_condition table.')


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
    schedule_interval='@daily',
    catchup=False
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

truncate_and_insert_dag_condition_task = PythonOperator(
    task_id='truncate_and_insert_dag_condition',
    python_callable=truncate_and_insert_dag_condition,
    op_kwargs={
        'hook': airflow_db_hook
    },
    provide_context=True,
    dag=dag
)

# ================ set up dependencies ================
start_task >> create_dag_condition_table_task >> get_runtime_data_task >> \
analyze_runtime_data_task >> truncate_and_insert_dag_condition_task >> end_task
