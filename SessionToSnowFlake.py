from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import snowflake.connector


def return_snowflake_conn():
    user_id = Variable.get('SNOWFLAKE_USER')
    password = Variable.get('SNOWFLAKE_PASSWORD')
    account = Variable.get('SNOWFLAKE_ACCOUNT')

    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,
        warehouse=Variable.get('SNOWFLAKE_WAREHOUSE'),
        database=Variable.get('SNOWFLAKE_DATABASE'),
        schema=Variable.get('SNOWFLAKE_SCHEMA')
    )
    return conn.cursor()


def set_stage_and_tables():
    cursor = return_snowflake_conn()

    create_user_session_channel = """
    CREATE TABLE IF NOT EXISTS raw_data.user_session_channel (
        userId int not NULL,
        sessionId varchar(32) primary key,
        channel varchar(32) default 'direct'  
    );
    """

    create_session_timestamp = """
    CREATE TABLE IF NOT EXISTS raw_data.session_timestamp (
        sessionId varchar(32) primary key,
        ts timestamp  
    );
    """

    create_stage = """
    CREATE OR REPLACE STAGE raw_data.blob_stage
    url = 's3://s3-geospatial/readonly/'
    file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
    """

    cursor.execute(create_user_session_channel)
    cursor.execute(create_session_timestamp)
    cursor.execute(create_stage)
    cursor.close()


def load_data():
    cursor = return_snowflake_conn()

    load_user_session_channel = """
    COPY INTO raw_data.user_session_channel
    FROM @raw_data.blob_stage/user_session_channel.csv;
    """

    load_session_timestamp = """
    COPY INTO raw_data.session_timestamp
    FROM @raw_data.blob_stage/session_timestamp.csv;
    """

    cursor.execute(load_user_session_channel)
    cursor.execute(load_session_timestamp)
    cursor.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1),
    'retries': 1
}

with DAG(
    dag_id='SessionToSnowflake',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    set_stage = PythonOperator(
        task_id='set_stage_and_tables',
        python_callable=set_stage_and_tables
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    set_stage >> load
