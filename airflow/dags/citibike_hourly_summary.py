import json
import glob
import os
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

DATA_DIR = "/opt/airflow/data"

def summarize_last_hour(**context):
    execution_date = context["logical_date"] #pulls out which hour this dag run is responsible for
    dt_str = execution_date.strftime("%Y-%m-%d")
    hour_str = execution_date.strftime("%H")

    pattern = f"{DATA_DIR}/dt={dt_str}/hour={hour_str}/*.json"
    files = glob.glob(pattern)

    if not files:
        print(f"No files found for {pattern}, skipping")
        return
    
    dfs = [pd.read_json(f,lines=True) for f in files]
    df = pd.concat(dfs,ignore_index=True)
    df=df[df["is_installed"] == 1]

    summary = (
        df.groupby("station_id")
        .agg(
            avg_bikes_available=("num_bikes_available","mean"),
            min_bikes_available=("num_bikes_available","min"),
            max_bikes_available=("num_bikes_available","max"),
            readings=("num_bikes_available","count")
        )
        .reset_index()
    )
    output_path = f"{DATA_DIR}/summaries/dt={dt_str}/hour={hour_str}.csv"
    os.makedirs(os.path.dirname(output_path),exist_ok=True)
    summary.to_csv(output_path,index=False)
    print(f"wrote summary for {len(summary)} stations to {output_path}")

def check_data_quality(**context):
    execution_date = context["logical_date"] #pulls out which hour this dag run is responsible for
    dt_str = execution_date.strftime("%Y-%m-%d")
    hour_str = execution_date.strftime("%H")

    pattern = f"{DATA_DIR}/dt={dt_str}/hour={hour_str}/*.json"
    files = glob.glob(pattern)

    if not files:
        return
    
    dfs = [pd.read_json(f,lines=True) for f in files]
    df = pd.concat(dfs,ignore_index=True)
    df = df.drop_duplicates(subset=["station_id","last_reported"]) 

    '''
        this is accounting for bikes and docks that are disabled
        the data may show that available bikes = 0
        and docks avaible = 0 
        but we need to account for the bikes and docks that are disabled
        there will be no avaible bikes or docks if they are disabled
    '''
    df["total_capacity_accounted"] = ( 
        df["num_bikes_available"]
        + df["num_bikes_disabled"]
        + df["num_docks_available"]
        + df["num_docks_disabled"]
    )
    
    #filters out stations that aren't actually active in the system
    df  = df[
        (df["is_installed"] == 1)
        &(df["is_renting"] == 1)
        &(df["is_returning"] == 1)
    ]

    #check for suspicious data 
    suspicious = df[
        (df["total_capacity_accounted"] == 0)
        & (df["num_bikes_available"] == 0) 
        & (df["num_docks_available"] == 0)
    ]
    if len(suspicious) > 0:
        output_path = f"{DATA_DIR}/suspicious/dt={dt_str}/hour={hour_str}.csv"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        suspicious.to_csv(output_path, index=False)
        print(f"WARNING {len(suspicious)} readings with 0 bikes and 0 docks - likely a feed issue, not real state")
        print(f"wrote suspicious readings to {output_path}")
    else:
        print("data quality check passed...")

default_args = {
    "owner":"you",
    "retries":1,
    "retry_delay":timedelta(minutes=5),
}

with DAG(
    dag_id="citibike_hourly_summary",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2026,7,1),
    catchup=False,
    tags=['citibike'],
) as dag:
    summarize_task = PythonOperator(
        task_id="summarize_last_hour",
        python_callable=summarize_last_hour,
    )
    quality_check_task = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality
    )
    summarize_task >> quality_check_task