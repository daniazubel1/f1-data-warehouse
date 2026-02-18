from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def fetch_all_ergast_data(resource_url, limit=1000):
    all_data = []
    offset = 0
    while True:
        url = f"{resource_url}?limit={limit}&offset={offset}"
        logging.info(f"Fetching {url}")
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            mr_data = data['MRData']
            table_key = list(mr_data.keys())[1] # usually 'DriverTable', 'RaceTable' etc.
            # The list inside might be 'Drivers', 'Races' etc.
            inner_list = list(mr_data[table_key].values())[0] # The list of items
            
            if not inner_list:
                break
                
            all_data.extend(inner_list)
            
            total = int(mr_data['total'])
            if offset + limit >= total:
                break
            offset += limit
        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            raise

    return all_data

def load_drivers(**kwargs):
    drivers = fetch_all_ergast_data("http://ergast.com/api/f1/drivers.json")
    df = pd.DataFrame(drivers)
    # Basic cleaning handles nested API structure? 
    # Ergast returns clean dicts mostly, but nested URL or driverId
    # Simplify for staging: just dump relevant columns
    
    # Needs to normalize if needed, or dump as JSON
    # For now, let's dump as is usually works for flattened text
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    
    # We load to a staging table 'raw_drivers'
    df.to_sql('raw_drivers', engine, schema='public', if_exists='replace', index=False)
    logging.info(f"Loaded {len(df)} drivers to raw_drivers")

def load_circuits(**kwargs):
    circuits = fetch_all_ergast_data("http://ergast.com/api/f1/circuits.json")
    df = pd.DataFrame(circuits)
    # Location is nested: "Location": {"lat": "...", "long": "...", "locality": "...", "country": "..."}
    # Need to flatten location
    if 'Location' in df.columns:
        loc_df = pd.json_normalize(df['Location'])
        df = df.drop('Location', axis=1).join(loc_df)
        
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    df.to_sql('raw_circuits', engine, schema='public', if_exists='replace', index=False)
    logging.info(f"Loaded {len(df)} circuits to raw_circuits")

def load_constructors(**kwargs):
    constructors = fetch_all_ergast_data("http://ergast.com/api/f1/constructors.json")
    df = pd.DataFrame(constructors)
    
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    df.to_sql('raw_constructors', engine, schema='public', if_exists='replace', index=False)
    logging.info(f"Loaded {len(df)} constructors to raw_constructors")

def load_races(**kwargs):
    # Races might need more careful handling (seasons)
    # for simplicity, assume we can fetch all (might be large)
    races = fetch_all_ergast_data("http://ergast.com/api/f1/races.json")
    df = pd.DataFrame(races)
    # Circuit is nested
    if 'Circuit' in df.columns:
         # Just keep circuitId
         df['circuitId'] = df['Circuit'].apply(lambda x: x.get('circuitId') if x else None)
         df = df.drop('Circuit', axis=1)

    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()
    df.to_sql('raw_races', engine, schema='public', if_exists='replace', index=False)
    logging.info(f"Loaded {len(df)} races to raw_races")

with DAG(
    'f1_historical_load',
    default_args=default_args,
    description='Load historical F1 data to Postgres',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    t_drivers = PythonOperator(task_id='load_drivers', python_callable=load_drivers)
    t_circuits = PythonOperator(task_id='load_circuits', python_callable=load_circuits)
    t_constructors = PythonOperator(task_id='load_constructors', python_callable=load_constructors)
    t_races = PythonOperator(task_id='load_races', python_callable=load_races)

    [t_drivers, t_circuits, t_constructors, t_races]
