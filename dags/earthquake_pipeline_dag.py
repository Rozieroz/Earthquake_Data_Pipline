import os, sys, json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from dotenv import load_dotenv
load_dotenv()

# get project root from .env 
PROJECT_ROOT = os.getenv("PROJECT_ROOT")
if not PROJECT_ROOT:
    raise ValueError("PROJECT_ROOT is not set in your .env file.")

# add project root to sys.path to import pipeline)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)


# # path setup to import from earthquake_project
# PROJECT_ROOT = os.getenv("PROJECT_ROOT")
# sys.path.append(os.path.join(PROJECT_ROOT, 'pipeline'))

try: 
    from pipeline.earthquakes import create_db_and_table, fetch_earthquake_data, earthquakes_to_df, load_to_mysql
except ImportError as e:
    raise ImportError(f"Error importing earthquake module: {e}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='earthquake_pipeline',
    default_args=default_args,
    description='Pipeline to fetch, process, and load earthquake data into MySQL',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    # task 1: create db and table if not exists
    create_table_task = PythonOperator(     
        task_id='create_db_and_table',
        python_callable=create_db_and_table
    )

    # task 2 Fetch data and push to XCom
    def _fetch_data(**context):             # py func to be used with PythonOperator below
        data = fetch_earthquake_data()      # fetch data from USGS API
        count = len(data)                   # count number of earthquake events fetched
        print(f"✅ Fetched {count} earthquake events")

        # convert datetime objects to ISO format for JSON serialization
        for row in data:
            if isinstance(row["time"], datetime):
                row["time"] = row["time"].isoformat()


        # (Push) raw data to XCom so next task can access it... the data is stored in Airflow DB, serialized as JSON
        context['ti'].xcom_push(key="earthquake_data", value=json.dumps(data))
        return count  # will also appear in Airflow UI
    
    fetch_data_task = PythonOperator(
        task_id="fetch_data",
        python_callable=_fetch_data,
        provide_context=True
    )

    # task 3: process data
    def _process_and_save(**context):
        # Pull data back from XCom (pull)...retrieve the stored data
        raw_data = context['ti'].xcom_pull(task_ids='fetch_data', key='earthquake_data')
        if not raw_data:
            print("⚠️ No earthquake data received from fetch_data task.")
            return
        
        data = json.loads(raw_data)
        # convert time strings back to datetime before creating Spark DF
        for row in data:
            if isinstance(row.get("time"), str):
                row["time"] = datetime.fromisoformat(row["time"])

        if not data:
            print("✅ No new earthquakes found, skipping save.")
            return
        
        df = earthquakes_to_df(data)
        print(f"✅ Converting to Spark DataFrame with {df.count()} rows...")
        load_to_mysql(df)
        print("✅ Data successfully saved to MySQL.")

    process_and_save_task = PythonOperator(
        task_id="process_and_save",
        python_callable=_process_and_save,
        provide_context=True
    )

    create_table_task >> fetch_data_task >> process_and_save_task

