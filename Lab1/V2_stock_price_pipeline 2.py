from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests
import pandas as pd

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
def extract_stock_data(stock_symbol):
    API_KEY = Variable.get('vantage_api_key')
    
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()["Time Series (Daily)"]
    
    df = pd.DataFrame.from_dict(data, orient='index')
    df.index = pd.to_datetime(df.index)
    df.columns = ['open', 'high', 'low', 'close', 'volume']
    df['symbol'] = stock_symbol
    
    df = df.loc[df.index >= (datetime.now() - timedelta(days=90))]
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)
    
    return df

@task
def load_data_to_snowflake(df):
    cur = return_snowflake_conn()
    
    try:
        for _, row in df.iterrows():
            check_query = f"SELECT COUNT(1) FROM raw_data.stock_prices WHERE date = '{row['date'].strftime('%Y-%m-%d')}' AND symbol = '{row['symbol']}'"
            cur.execute(check_query)
            exists = cur.fetchone()[0]

            if exists == 0:
                insert_query = f"""
                INSERT INTO raw_data.stock_prices (date, open, high, low, close, volume, symbol)
                VALUES ('{row['date'].strftime('%Y-%m-%d')}', {row['open']}, {row['high']}, {row['low']}, {row['close']}, {row['volume']}, '{row['symbol']}')
                """
                cur.execute(insert_query)
        
        cur.execute("COMMIT;")  
    except Exception as e:
        cur.execute("ROLLBACK;")  
        print(f"Error occurred: {e}")
        raise e
    finally:
        cur.close()

with DAG(
    dag_id='V2_stock_price_pipeline',
    start_date=datetime(2024, 10, 11),
    catchup=False,
    schedule_interval='@daily',
    tags=['ETL']
) as dag:
    
    stock_symbol = ["AAPL","MSFT"]
    for stock_symbol in stock_symbol:
        stock_data = extract_stock_data(stock_symbol)
        load_data_to_snowflake(stock_data)