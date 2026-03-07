from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import snowflake.connector
import os

default_args = {
    'owner': 'kevin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SNOWFLAKE_CONFIG = {
    'account': 'IMGPXDX-SXC90910',
    'user': 'KEVINS254',
    'password': os.environ.get('SNOWFLAKE_PASSWORD'),
    'warehouse': 'COMPUTE_WH',
    'database': 'NYC_TAXI',
    'schema': 'RAW',
    'role': 'ACCOUNTADMIN',
}

def download_data(**context):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    local_path = "/tmp/yellow_tripdata_2024-01.parquet"
    print(f"Downloading {url}...")
    response = requests.get(url, stream=True)
    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded to {local_path}")
    return local_path

def create_table(**context):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NYC_TAXI.RAW.YELLOW_TAXI_TRIPS (
            VENDORID NUMBER,
            TPEP_PICKUP_DATETIME TIMESTAMP,
            TPEP_DROPOFF_DATETIME TIMESTAMP,
            PASSENGER_COUNT NUMBER,
            TRIP_DISTANCE FLOAT,
            RATECODEID NUMBER,
            STORE_AND_FWD_FLAG VARCHAR,
            PULOCATIONID NUMBER,
            DOLOCATIONID NUMBER,
            PAYMENT_TYPE NUMBER,
            FARE_AMOUNT FLOAT,
            EXTRA FLOAT,
            MTA_TAX FLOAT,
            TIP_AMOUNT FLOAT,
            TOLLS_AMOUNT FLOAT,
            IMPROVEMENT_SURCHARGE FLOAT,
            TOTAL_AMOUNT FLOAT,
            CONGESTION_SURCHARGE FLOAT,
            AIRPORT_FEE FLOAT
        )
    """)
    print("Table created successfully")
    cursor.close()
    conn.close()

def load_to_snowflake(**context):
    local_path = "/tmp/yellow_tripdata_2024-01.parquet"
    print("Reading parquet file...")
    df = pd.read_parquet(local_path)
    df.columns = [col.upper() for col in df.columns]

    # Keep only needed columns
    cols = [
        'VENDORID','TPEP_PICKUP_DATETIME','TPEP_DROPOFF_DATETIME',
        'PASSENGER_COUNT','TRIP_DISTANCE','RATECODEID','STORE_AND_FWD_FLAG',
        'PULOCATIONID','DOLOCATIONID','PAYMENT_TYPE','FARE_AMOUNT','EXTRA',
        'MTA_TAX','TIP_AMOUNT','TOLLS_AMOUNT','IMPROVEMENT_SURCHARGE',
        'TOTAL_AMOUNT','CONGESTION_SURCHARGE','AIRPORT_FEE'
    ]
    df = df[[c for c in cols if c in df.columns]]

    # Save as CSV for Snowflake loading
    csv_path = "/tmp/yellow_tripdata_2024-01.csv"
    df.to_csv(csv_path, index=False)
    print(f"Saved {len(df)} rows to CSV")

    # Load into Snowflake
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    # Create stage and load
    cursor.execute("CREATE OR REPLACE STAGE NYC_TAXI.RAW.TAXI_STAGE")
    cursor.execute(f"PUT file://{csv_path} @NYC_TAXI.RAW.TAXI_STAGE")
    cursor.execute("""
        COPY INTO NYC_TAXI.RAW.YELLOW_TAXI_TRIPS
        FROM @NYC_TAXI.RAW.TAXI_STAGE
        FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
        PURGE = TRUE
    """)
    print("Data loaded into Snowflake successfully!")
    cursor.close()
    conn.close()

with DAG(
    dag_id='nyc_taxi_ingestion',
    default_args=default_args,
    description='Download NYC Taxi data and load into Snowflake',
    schedule_interval='@monthly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc_taxi', 'ingestion'],
) as dag:

    t1 = PythonOperator(
        task_id='download_data',
        python_callable=download_data,
    )

    t2 = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    t3 = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
    )

    t1 >> t2 >> t3
