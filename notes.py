def query_and_process():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    
    # Define your SQL query
    sql = "SELECT id, name FROM users WHERE active = true;"
    
    # Run query and fetch results
    results = hook.get_records(sql)
    
    # Process results (just printing for now)
    for row in results:
        user_id, name = row
        print(f"User ID: {user_id}, Name: {name}")


