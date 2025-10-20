# Batch Processing Project
### Harry Donnelly
*Please note that this is a repository is used for visualisation of a project on a cloud client - None of the code found in this repo will be functional without proper set up of databases, ec2 instance and proper dependency installation.*

## Tasks & Breakdown

> Task 1: Cleaning the posts contents
- All posts have a body field that is not empty
- Extracts all posts from the source database
- Loads into target database while keeping track of the last processed posts
- Id, title, body, owner_user_id, creation_date
- Runs every 15 mins


> Task 2: Cleanup HTML in posts contents
- Detetct empty boday tags and removes and replaces the value.

> Task 3: 
- now runs for 7 mins, and pagination limit should increase to 10000.

## Early plans and set up

We have our ec2 connection ready, however we have to be able to access the data. The data is held in an rds database. This is cloud storage, so conenctions are slightly different when it comes to psql. We are used to using local host (our own machine) for database - as this time it is being hosted externally - when creating a connecting in psql, you have to include the host address - in the case of redshift it looks something like this:

> http://analyticaldb-8.ihavereplacedthisbit.eu-west-2.rds.amazonaws.com/

Other than that, setting up and directly interacting with the database is the exact same as we already know.

---
Now we have a connection to the database, we can have a look at the what the tables are, and what data they hold. using \dt we can see 5 database tables within the connection.

1. Comments
- Table of comments with ids for the post and user, comment body, creation time stamp and other data
2. Posts
- Table of posts with ids of the user, and last editor, title, body, tags(html), timestamps for key dates(creation, last edit) and some other data
3. Tags
- A colletion of html tags being used across the app
4. Users
- Collection of users with id, name, description, timestamps for key events and other data (Upon checking this, I could see users being taken up by companies like google and gitlab - these have negative IDs and may affect later actions so will keep in mind)
5. Votes
- Table of votes for a post - with post they relate to ID, the user(NULL on most records), vote_type and creation timestamp

From the first two tasks at least, it seems we can stay completley within the posts table and make a cleaned version on the analytical one. This seems simple enough, however we are taking from one database to another, so we will need to bring the data out of the database rather than to a seperate table in the same database. As far as i know, postgresoperator doesnt expect a return or can handle one - so this might be a bit of a blocker if it comes to it. 

Little bit of research on the above shwos postgreshook could work, and expects a result.

## Process of coding

To start with, i wanted to test how I would access the data within python - for this, I wanted to make a small dag that would take a table and output it somewhere else. Using postgres hook and xcom, I was able to make a dag that proved conenction (shown below).

```python
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

    t1 = PythonOperator(
        task_id='extract_posts',
        python_callable=extract_posts_data,
        dag=dag,
    )
    t2 = PythonOperator(
        task_id = "test_print",
        python_callable=print_posts,
        dag=dag,
    )
t1 >> t2
```

 With this we were able to see records in the logs for this, however this introduced a new potential problem. This took a long time to run, there are around 60 million posts in this table alone. So my thought with this would be to somehow reduce the amount of data we are taking in. Before attempting, the catchup arugment is coming to mind. This dag has to run every 15 mins or so, if we only query the posts that have had edits made since the last run (using current date minus 15 mins and comparing to last edit date), meaning we are only querying the necessary posts each run (other than the first which will catchup to the current date).

 I have now made a version of this that should take all the posts, remove any post with nothing in the body and input to the analytical database with only the requested columns. This is doing all the records, right after seeing the potential time problem, this is as I am curious about how long this will take, and if it is something we want to 100% avoid. You can see the new tasks below.

 ```python
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
 ```

 

