from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2.extras


endpoint_conn_id = "rds-endpoint-batchp"
source_conn_id = "rds-source-batchp"


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def stream_posts_to_target(**context):
    batch_size = 1000
    src_hook = PostgresHook(postgres_conn_id=source_conn_id)
    target_hook = PostgresHook(postgres_conn_id=endpoint_conn_id)
    src_conn = src_hook.get_conn()
    target_conn = target_hook.get_conn()

    src_cur = src_conn.cursor(name="posts_stream_cursor")
    src_cur.itersize = batch_size
    src_cur.execute("""
        SELECT id, title, body, owner_user_id, creation_date
        FROM posts
        WHERE body IS NOT NULL;
    """)

    target_cur = target_conn.cursor()
    target_cur.execute("""
    CREATE TABLE IF NOT EXISTS posts_summary (
        id BIGINT PRIMARY KEY,
        title TEXT,
        body TEXT,
        owner_user_id BIGINT,
        creation_date TIMESTAMP
    );
    """)
    target_conn.commit()

    rows = src_cur.fetchmany(batch_size)
    insert_sql = """
        INSERT INTO posts_summary (id, title, body, owner_user_id, creation_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """

    while rows:
        cleaned = []
        for r in rows:
            body = r[2]
            if body is None:
                continue
            if isinstance(body, str) and body.strip() == "":
                continue
            cleaned.append(r)

        if cleaned:
            psycopg2.extras.execute_batch(target_cur, insert_sql, cleaned, page_size=500)
            target_conn.commit()

        rows = src_cur.fetchmany(batch_size)

    src_cur.close()
    target_cur.close()


with DAG(
    dag_id='project_dag_copilot',
    default_args=default_args,
    description='stream posts from source to target without XCom',
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2023, 4, 28),
    catchup=False,
) as dag:

    stream = PythonOperator(
        task_id='stream_posts',
        python_callable=stream_posts_to_target,
        provide_context=True,
    )

    stream
