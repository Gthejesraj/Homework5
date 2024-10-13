from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import pandas as pd

# Define the default_args
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
with DAG(
    dag_id='stock_price_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 10, 10),
    catchup=False,
    schedule_interval='@daily',
    tags=['ETL']
) as dag:

    @task
    def extract_stock_data():
        API_KEY = Variable.get('api_key')
        stock_symbol = "AAPL"

        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={API_KEY}"
        response = requests.get(url)
        data = response.json().get("Time Series (Daily)", {})

        if data:
            df = pd.DataFrame.from_dict(data, orient='index')
            df.index = pd.to_datetime(df.index)
            df.columns = ['open', 'high', 'low', 'close', 'volume']
            df['symbol'] = stock_symbol

            df = df.loc[df.index >= (datetime.now() - timedelta(days=90))]
            df.reset_index(inplace=True)
            df.rename(columns={"index": "date"}, inplace=True)

            return df.to_dict(orient='records')
            return []

    @task
    def load_data_to_snowflake(stock_data):
        if not stock_data:
            print("No stock data to insert into Snowflake.")
            return

        df = pd.DataFrame(stock_data)

        snowflake_user = Variable.get('snowflake_user')
        snowflake_password = Variable.get('snowflake_password')
        snowflake_account = Variable.get('snowflake_account')

        hook = SnowflakeHook(
            account=snowflake_account,
            user=snowflake_user,
            password=snowflake_password,
            database=Variable.get('snowflake_database'),
            warehouse=Variable.get('snowflake_warehouse'),
            schema=Variable.get('snowflake_schema')
        )
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            for _, row in df.iterrows():
                check_query = f"""
                SELECT COUNT(1) FROM raw_data.stock_prices
                WHERE date = '{row['date'].strftime('%Y-%m-%d')}' AND symbol = '{row['symbol']}'
                """
                cursor.execute(check_query)
                exists = cursor.fetchone()[0]

                if exists == 0:
                    insert_query = f"""
                    INSERT INTO raw_data.stock_prices (date, open, high, low, close, volume, symbol)
                    VALUES ('{row['date'].strftime('%Y-%m-%d')}', {row['open']}, {row['high']}, {row['low']}, {row['close']}, {row['volume']}, '{row['symbol']}')
                    """
                    cursor.execute(insert_query)

            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"Error occurred: {e}")
        finally:
            cursor.close()
            conn.close()

    stock_data = extract_stock_data()
    load_data_to_snowflake(stock_data)
