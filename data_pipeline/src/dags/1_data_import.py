from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from connections import PG_CONNECTION, VERTICA_CONNECTION
import vertica_python
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'provide_context': True,
    'description': 'Transfer data from PostgreSQL to Vertica',
    'schedule_interval': None,
    'start_date': datetime(2022, 10, 18),
    'end_date': datetime(2022, 10, 19),
    'catchup': False,
    }

conn_vertica = VERTICA_CONNECTION

dag = DAG(
    'data_import',
    default_args=default_args,
    )


def load_stg_transactions(execution_date):
    # Set up data upload for the previous day
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d").date() - timedelta(days=1)

    # Connect to the Postgres database
    dwh_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    pg_conn = dwh_hook.get_conn()
    pg_cur = pg_conn.cursor()

    with open('/lessons/dags/sql/load_transactions.sql', 'r', encoding='utf-8') as sql_file:
        query_load_transactions = sql_file.read().replace('\n', ' ')

    query_load_transactions = query_load_transactions.replace('{execution_date}', execution_date.strftime("%Y-%m-%d"))

    pg_cur.execute(query_load_transactions)
    rows = pg_cur.fetchall()

    # Connect to the Vertica database
    with vertica_python.connect(**conn_vertica) as v_conn:
        with v_conn.cursor() as v_cur:
            with open('/lessons/dags/sql/insert_transactions.sql', 'r', encoding='utf-8') as sql_file:
                query_insert_transactions = sql_file.read().replace('\n', ' ')
            for row in rows:
                query_insert_transactions_with_values = query_insert_transactions.replace('%s1', str(row[0]))
                query_insert_transactions_with_values = query_insert_transactions_with_values.replace('%s2',
                                                                                                      str(row[1]))
                query_insert_transactions_with_values = query_insert_transactions_with_values.replace('%s3',
                                                                                                      str(row[2]))
                query_insert_transactions_with_values = query_insert_transactions_with_values.replace('%s4', str(row[3]))
                query_insert_transactions_with_values = query_insert_transactions_with_values.replace('%s5',
                                                                                                      str(row[4]))
                query_insert_transactions_with_values = query_insert_transactions_with_values.replace('%s6',
                                                                                                      str(row[5]))
                query_insert_transactions_with_values = query_insert_transactions_with_values.replace('%s7', str(row[6]))
                query_insert_transactions_with_values = query_insert_transactions_with_values.replace('%s8',
                                                                                                      str(row[7]))
                query_insert_transactions_with_values = query_insert_transactions_with_values.replace('%s9',
                                                                                                      str(row[8]))
                v_cur.execute(query_insert_transactions_with_values)

    pg_cur.close()
    pg_conn.close()


def load_stg_currencies(execution_date):
    # Set up data upload for the previous day
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d").date() - timedelta(days=1)

    # Connect to the Postgres database
    dwh_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    pg_conn = dwh_hook.get_conn()
    pg_cur = pg_conn.cursor()

    with open('/lessons/dags/sql/load_currencies.sql', 'r', encoding='utf-8') as sql_file:
        query_load_currencies = sql_file.read().replace('\n', ' ')

    query_load_currencies = query_load_currencies.replace('{execution_date}',
                                                          execution_date.strftime("%Y-%m-%d"))

    pg_cur.execute(query_load_currencies)
    rows = pg_cur.fetchall()

    # Connect to the Vertica database
    with vertica_python.connect(**conn_vertica) as v_conn:
        with v_conn.cursor() as v_cur:
            with open('/lessons/dags/sql/insert_currencies.sql', 'r', encoding='utf-8') as sql_file:
                query_insert_currencies = sql_file.read().replace('\n', ' ')
            for row in rows:
                query_insert_currencies_with_values = query_insert_currencies.replace('%s1', str(row[0]))
                query_insert_currencies_with_values = query_insert_currencies_with_values.replace('%s2', str(row[1]))
                query_insert_currencies_with_values = query_insert_currencies_with_values.replace('%s3', str(row[2]))
                query_insert_currencies_with_values = query_insert_currencies_with_values.replace('%s4', str(row[3]))
                v_cur.execute(query_insert_currencies_with_values)

    pg_cur.close()
    pg_conn.close()


load_stg_transactions = PythonOperator(task_id='load_stg_transactions',
                                       python_callable=load_stg_transactions,
                                       provide_context=True,
                                       op_kwargs={'execution_date': '{{ds}}'},
                                       dag=dag)

load_stg_currencies = PythonOperator(task_id='load_stg_currencies',
                                     python_callable=load_stg_currencies,
                                     provide_context=True,
                                     op_kwargs={'execution_date': '{{ds}}'},
                                     dag=dag)


load_stg_transactions >> load_stg_currencies
