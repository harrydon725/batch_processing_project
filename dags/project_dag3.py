from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2.extras
from bs4 import BeautifulSoup
import re


endpoint_conn_id = "rds-endpoint-batchp"
source_conn_id = "rds-source-batchp"


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def clean_html_content(text):
    if not text or not isinstance(text, str):
        return text
        
    soup = BeautifulSoup(text, 'html.parser')
    
    for tag in soup.find_all(['p', 'br', 'div']):
        tag.replace_with('\n' + tag.get_text() + '\n')
    
    cleaned_text = soup.get_text()
    cleaned_text = re.sub(r'\n\s*\n', '\n\n', cleaned_text)
    cleaned_text = cleaned_text.strip()
    
    return cleaned_text

def stream_and_clean_posts(**context):
    batch_size = 1000
    last_run_time = context.get('prev_execution_date')
    if last_run_time is None:
        last_run_time = datetime(2000, 1, 1)
    else:
        last_run_time = datetime(str(last_run_time))

    src_hook = PostgresHook(postgres_conn_id=source_conn_id)
    tgt_hook = PostgresHook(postgres_conn_id=endpoint_conn_id)

    src_conn = src_hook.get_conn()
    tgt_conn = tgt_hook.get_conn()

    src_cur = src_conn.cursor(name="posts_stream_cursor")
    src_cur.itersize = batch_size
    src_cur.execute("""
        SELECT id, title, body, owner_user_id, creation_date
        FROM posts
        WHERE body IS NOT NULL
        AND (
            creation_date > %s
            OR (last_edit_date IS NOT NULL AND last_edit_date > %s)
        )
        ORDER BY creation_date
    """, (last_run_time, last_run_time))

    tgt_cur = tgt_conn.cursor()
    tgt_cur.execute("""
        CREATE TABLE IF NOT EXISTS posts_summary_clean_date (
            id BIGINT PRIMARY KEY,
            title TEXT,
            body TEXT,
            owner_user_id BIGINT,
            creation_date TIMESTAMP WITH TIME ZONE,
            cleaned_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    """)
    tgt_conn.commit()

    insert_sql = """
        INSERT INTO posts_summary_clean_date 
        (id, title, body, owner_user_id, creation_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE 
        SET body = EXCLUDED.body,
            cleaned_at = CURRENT_TIMESTAMP
        WHERE posts_summary_clean_date.body != EXCLUDED.body
    """

    total_processed = 0
    total_updated = 0

    while True:
        rows = src_cur.fetchmany(batch_size)
        if not rows:
            break

        cleaned_rows = []
        for row in rows:
            cleaned_body = clean_html_content(row[2])
            if cleaned_body is None or cleaned_body.strip() == "":
                continue
                
            cleaned_row = (
                row[0],
                row[1],
                cleaned_body,
                row[3],
                row[4]
            )
            cleaned_rows.append(cleaned_row)

        if cleaned_rows:
            psycopg2.extras.execute_batch(tgt_cur, insert_sql, cleaned_rows, page_size=500)
            tgt_conn.commit()
            
            total_processed += len(rows)
            total_updated += len(cleaned_rows)

    src_cur.close()
    tgt_cur.close()
    
    context['task_instance'].xcom_push(
        key='processing_summary',
        value={
            'total_processed': total_processed,
            'total_updated': total_updated
        }
    )

    return f"Processed {total_processed} posts, updated {total_updated} with cleaned HTML"


with DAG(
    dag_id='project_dag5',
    default_args=default_args,
    description='Stream and clean HTML from posts body field',
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2023, 4, 28),
    catchup=False,
) as dag:

    clean_and_load = PythonOperator(
        task_id='clean_and_load_posts',
        python_callable=stream_and_clean_posts,
        provide_context=True,
    )

    clean_and_load
