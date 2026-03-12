from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import snowflake.connector
import os

default_args = {
    'owner': 'kevin',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
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

MONTHS = [
    '2024-01', '2024-02', '2024-03', '2024-04',
    '2024-05', '2024-06', '2024-07', '2024-08',
    '2024-09', '2024-10', '2024-11', '2024-12',
]

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
            AIRPORT_FEE FLOAT,
            MONTH_PARTITION VARCHAR
        )
    """)
    print("Table ready.")
    cursor.close()
    conn.close()

def download_and_load(month, **context):
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
    local_path = f"/tmp/yellow_tripdata_{month}.parquet"
    csv_path = f"/tmp/yellow_tripdata_{month}.csv"

    # Download
    print(f"Downloading {month}...")
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download {month}: HTTP {response.status_code}")
    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded {month}")

    # Read & validate
    df = pd.read_parquet(local_path)
    df.columns = [col.upper() for col in df.columns]
    print(f"{month}: {len(df)} rows loaded")

    if len(df) == 0:
        raise Exception(f"No data found for {month}")

    cols = [
        'VENDORID','TPEP_PICKUP_DATETIME','TPEP_DROPOFF_DATETIME',
        'PASSENGER_COUNT','TRIP_DISTANCE','RATECODEID','STORE_AND_FWD_FLAG',
        'PULOCATIONID','DOLOCATIONID','PAYMENT_TYPE','FARE_AMOUNT','EXTRA',
        'MTA_TAX','TIP_AMOUNT','TOLLS_AMOUNT','IMPROVEMENT_SURCHARGE',
        'TOTAL_AMOUNT','CONGESTION_SURCHARGE','AIRPORT_FEE'
    ]
    df = df[[c for c in cols if c in df.columns]]
    df['MONTH_PARTITION'] = month

    # Delete existing data for this month (idempotent reload)
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    cursor.execute(f"DELETE FROM NYC_TAXI.RAW.YELLOW_TAXI_TRIPS WHERE MONTH_PARTITION = '{month}'")
    print(f"Cleared existing {month} data")

    # Save CSV and load
    df.to_csv(csv_path, index=False)
    cursor.execute("CREATE OR REPLACE STAGE NYC_TAXI.RAW.TAXI_STAGE")
    cursor.execute(f"PUT file://{csv_path} @NYC_TAXI.RAW.TAXI_STAGE AUTO_COMPRESS=TRUE")
    cursor.execute(f"""
        COPY INTO NYC_TAXI.RAW.YELLOW_TAXI_TRIPS
        FROM @NYC_TAXI.RAW.TAXI_STAGE/yellow_tripdata_{month}.csv.gz
        FILE_FORMAT = (
            TYPE = CSV
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('', 'NULL', 'null')
        )
        PURGE = TRUE
    """)
    print(f"Loaded {month} into Snowflake successfully!")

    # Cleanup
    os.remove(local_path)
    os.remove(csv_path)
    cursor.close()
    conn.close()

def validate_load(**context):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MONTH_PARTITION, COUNT(*) as ROW_COUNT
        FROM NYC_TAXI.RAW.YELLOW_TAXI_TRIPS
        GROUP BY 1
        ORDER BY 1
    """)
    results = cursor.fetchall()
    total = 0
    print("=== Load Summary ===")
    for month, count in results:
        print(f"  {month}: {count:,} rows")
        total += count
    print(f"  TOTAL: {total:,} rows")
    cursor.close()
    conn.close()

with DAG(
    dag_id='nyc_taxi_ingestion_full_year',
    default_args=default_args,
    description='Load full year 2024 NYC Taxi data into Snowflake',
    schedule_interval='@yearly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc_taxi', 'ingestion', 'full_year'],
) as dag:

    t_create = PythonOperator(
        task_id='create_table',
        python_callable=create_table,
    )

    load_tasks = []
    for month in MONTHS:
        t = PythonOperator(
            task_id=f'load_{month.replace("-", "_")}',
            python_callable=download_and_load,
            op_kwargs={'month': month},
        )
        load_tasks.append(t)

    t_validate = PythonOperator(
        task_id='validate_load',
        python_callable=validate_load,
    )

    # create → load all months in sequence → validate
    t_create >> load_tasks[0]
    for i in range(len(load_tasks) - 1):
        load_tasks[i] >> load_tasks[i + 1]
    load_tasks[-1] >> t_validate
