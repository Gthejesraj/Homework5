from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from datetime import timedelta, datetime
import requests
import pandas as pd
import snowflake.connector
import logging
import time


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


@task
def fetch_stock_data(stock_symbol):
    API_KEY = Variable.get('api_key')

    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={API_KEY}"
    logging.info(f"Fetching data from Alpha Vantage for {stock_symbol}.")

    retries = 3
    for attempt in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Check if valid data is returned
            if "Time Series (Daily)" not in data:
                logging.error(
                    f"No 'Time Series (Daily)' data found for {stock_symbol}. Response: {data}")
                if "Information" in data and "Alpha Vantage" in data["Information"]:
                    logging.warning(
                        f"Rate limit hit, retrying in 60 seconds... ({attempt + 1}/{retries})")
                    time.sleep(60)
                else:
                    raise KeyError(
                        f"No 'Time Series (Daily)' data found for {stock_symbol}. Response: {data}")
            else:
                df = pd.DataFrame.from_dict(
                    data["Time Series (Daily)"], orient='index')
                df.index = pd.to_datetime(df.index)
                df.columns = ['open', 'high', 'low', 'close', 'volume']
                df['symbol'] = stock_symbol

                # Filter data for the last 90 days
                df = df.loc[df.index >= (datetime.now() - timedelta(days=90))]
                df.reset_index(inplace=True)
                df.rename(columns={"index": "date"}, inplace=True)

                logging.info(f"Data fetched successfully for {stock_symbol}.")
                return df

        except Exception as e:
            logging.error(f"Failed to fetch data for {stock_symbol}: {e}")
            if attempt == retries - 1:
                raise Exception(f"Failed after {retries} attempts: {e}")

    return None


@task
def load_forecast_to_snowflake(data_df, stock_symbol):
    if data_df is None or data_df.empty:
        logging.error(f"No data to load for {stock_symbol}.")
        raise ValueError(
            f"Data for {stock_symbol} is None or empty. Cannot load into Snowflake.")

    cur = return_snowflake_conn()

    try:

        for _, row in data_df.iterrows():
            insert_query = f"""
            INSERT INTO LAB1.RAW_DATA.STOCK_FORECASTS (date, close, symbol)
            VALUES ('{row['date'].strftime('%Y-%m-%d')}', {row['close']}, '{row['symbol']}')
            """
            cur.execute(insert_query)

        cur.execute("COMMIT;")
        logging.info(
            f"Data for {stock_symbol} successfully loaded into Snowflake.")

    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error(
            f"Failed to load data into Snowflake for {stock_symbol}: {e}")
        raise e
    finally:
        cur.close()


with DAG(
    dag_id='stock_forecast_pipeline',
    start_date=datetime(2024, 10, 12),
    catchup=False,
    schedule_interval='*/10 * * * *',
    tags=['stocks', 'ETL']
) as dag:

    mcd_data = fetch_stock_data('MCD')
    aapl_data = fetch_stock_data('AAPL')

    load_forecast_to_snowflake(mcd_data, 'MCD')
    load_forecast_to_snowflake(aapl_data, 'AAPL')
