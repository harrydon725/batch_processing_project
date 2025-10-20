from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json


endpoint_conn_id = "rds-endpoint-batchp"
source_conn_id = "rds-source-batchp"



'''
> Task 1: Cleaning the posts contents
- All posts have a body field that is not empty
- Extracts all posts from the source database
- Loads into target database while keeping track of the last processed posts
- Id, title, body, owner_user_id, creation_date
- Runs every 15 mins

'''

def extract_posts_data():
    sql = """
        SELECT id, title, body, owner_user_id, creation_date
        FROM posts
        WHERE body IS NOT NULL;
    """
    hook = PostgresHook(postgres_conn_id=source_conn_id)
    records = hook.get_records(sql)
    return records

def print_posts(**context):
    records = context['task_instance'].xcom_pull(task_ids='extract_posts')
    print(f"Number of records: {len(records)}")
    for record in records[:5]:
        print(record)

def clear_empty_body(**context):
    records = context['task_instance'].xcom_pull(task_ids='extract_posts')
    for record in records:
        if record[15] == None:
            records.remove(record)
    return records

def insert_posts(**context):
    records = context['task_instance'].xcom_pull(task_ids='clean_body') or []
    if not records:
        return "no records to insert"
    hook = PostgresHook(postgres_conn_id=endpoint_conn_id)
    create_sql = """
    CREATE TABLE IF NOT EXISTS posts_summary (
        id BIGINT PRIMARY KEY,
        title TEXT,
        body TEXT,
        owner_user_id BIGINT,
        creation_date TIMESTAMP WITH TIME ZONE
    );
    """
    hook.run(create_sql)
    hook.insert_rows(
        table='posts_summary',
        rows=records,
        target_fields=['id', 'title', 'body', 'owner_user_id', 'creation_date'],
        commit_every=1000
    )



default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    dag_id='project_dag',
    default_args=default_args,
    description='dag for project',
    schedule_interval=timedelta(days=15),
    start_date=datetime(2023, 4, 28),
    catchup=False,
    )as dag:
    t1 = PythonOperator(
        task_id='extract_posts',
        python_callable=extract_posts_data,
        dag=dag,
    )
    t2 = PythonOperator(
        task_id = "clean_body",
        python_callable=clear_empty_body,
        dag=dag,
    )
    t3 = PythonOperator(
        task_id = "insert_data",
        python_callable=insert_posts,
        dag=dag,
    )
t1 >> t2 >> t3



