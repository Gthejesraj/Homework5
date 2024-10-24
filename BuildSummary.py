from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import snowflake.connector
import logging


def return_snowflake_conn():
    user_id = Variable.get('SNOWFLAKE_USER')
    password = Variable.get('SNOWFLAKE_PASSWORD')
    account = Variable.get('SNOWFLAKE_ACCOUNT')

    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,
        warehouse='LAB1',  # Updated warehouse
        database='lab1',  # Updated database
        schema='raw_data'  # Updated schema
    )
    return conn.cursor()


def run_ctas():
    cursor = return_snowflake_conn()

    create_session_summary = """
    CREATE OR REPLACE TABLE raw_data.session_summary AS
    SELECT
        usc.userId,
        usc.sessionId,
        usc.channel,
        st.ts
    FROM raw_data.user_session_channel usc
    JOIN raw_data.session_timestamp st
        ON usc.sessionId = st.sessionId
    GROUP BY 
        usc.userId, usc.sessionId, usc.channel, st.ts;
    """
    cursor.execute(create_session_summary)

    check_duplicates_query = """
    SELECT sessionId, COUNT(*) as count
    FROM raw_data.session_summary
    GROUP BY sessionId
    HAVING COUNT(*) > 1;
    """

    cursor.execute(check_duplicates_query)
    duplicates = cursor.fetchall()

    if len(duplicates) > 0:
        logging.warning(
            f"Found {len(duplicates)} duplicate session IDs in session_summary.")
        for dup in duplicates:
            logging.warning(f"Duplicate sessionId: {dup[0]}, Count: {dup[1]}")
    else:
        logging.info("No duplicate session IDs found in session_summary.")

    cursor.close()


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 1),
    'retries': 1
}

with DAG(
    'BuildSummary',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    run_ctas_task = PythonOperator(
        task_id='run_ctas',
        python_callable=run_ctas
    )

    run_ctas_task
