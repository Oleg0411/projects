from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from connections import VERTICA_CONNECTION
import vertica_python
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'provide_context': True,
    'description': 'Update datamart to Vertica',
    'schedule_interval': timedelta(days=1),
    'start_date': datetime(2022, 10, 18),
    'end_date': datetime(2022, 10, 19),
    'catchup': True,
    }

conn_vertica = VERTICA_CONNECTION

dag = DAG(
    'datamart_update',
    default_args=default_args,
    )


def load_cdm_global_metrics(execution_date):
    # Set up data upload for the previous day
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d").date() - timedelta(days=1)

    # Connect to the Vertica database
    with vertica_python.connect(**conn_vertica) as v_conn:
        with v_conn.cursor() as v_cur:

            # Clean the data for the loaded day
            with open('/lessons/dags/sql/delete_global_metrics.sql', 'r', encoding='utf-8') as sql_file:
                query_delete_global_metrics = sql_file.read().replace('\n', ' ')

            query_delete_global_metrics = query_delete_global_metrics.replace('{execution_date}',
                                                                              execution_date.strftime("%Y-%m-%d"))
            v_cur.execute(query_delete_global_metrics)

            # Loading data to cdm
            with open('/lessons/dags/sql/insert_global_metrics.sql', 'r', encoding='utf-8') as sql_file:
                query_insert_global_metrics = sql_file.read().replace('\n', ' ')

            query_insert_global_metrics = query_insert_global_metrics.replace('{execution_date}',
                                                                              execution_date.strftime("%Y-%m-%d"))
            v_cur.execute(query_insert_global_metrics)


trigger_dag_data_import = TriggerDagRunOperator(
        task_id='trigger_data_import',
        trigger_dag_id='data_import',
        dag=dag
    )


load_cdm_global_metrics = PythonOperator(task_id='load_cdm_global_metrics',
                                         python_callable=load_cdm_global_metrics,
                                         provide_context=True,
                                         op_kwargs={'execution_date': '{{ds}}'},
                                         dag=dag)

trigger_dag_data_import >> load_cdm_global_metrics
