import json
import glob
import os
import io
from datetime import datetime, timedelta
import pandas as pd
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

load_dotenv()
# DATA_DIR = "/opt/airflow/data"
S3_BUCKET = os.environ["S3_BUCKET"]
s3 = boto3.client("s3")

def list_hour_keys(dt_str, hour_str):
    prefix = f"raw/dt={dt_str}/hour={hour_str}/"
    response = s3.list_objects_v2(Bucket=s3, Prefix=prefix)
    return [obj["Key"] for obj in response.get("Contents", [])]

def read_json_lines_from_s3(key):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    content = obj["Body"].read().decode("utf-8")
    return pd.read_json(io.StringIO(content), lines=True)

def write_cv_to_s3(df,key):
    csv_buffer=io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue().encode("utf-8"))


def summarize_last_hour(**context):
    execution_date = context["logical_date"] #pulls out which hour this dag run is responsible for
    dt_str = execution_date.strftime("%Y-%m-%d")
    hour_str = execution_date.strftime("%H")

    # pattern = f"{DATA_DIR}/dt={dt_str}/hour={hour_str}/*.json"
    # files = glob.glob(pattern)

    # if not files:
    #     print(f"No files found for {pattern}, skipping")
    #     return

    keys = list_hour_keys(dt_str,hour_str)
    if not keys:
        print(f"No files found for {dt_str}/{hour_str}, skipping.")
        return

    dfs = [read_json_lines_from_s3(k) for k in keys]
    df = pd.concat(dfs,ignore_index=True)

    df  = df[
        (df["is_installed"] == 1)
        &(df["is_renting"] == 1)
        &(df["is_returning"] == 1)
    ]
    
    # dfs = [pd.read_json(f,lines=True) for f in files]
    # df = pd.concat(dfs,ignore_index=True)
    # df=df[df["is_installed"] == 1]

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
    # output_path = f"{DATA_DIR}/summaries/dt={dt_str}/hour={hour_str}.csv"
    # os.makedirs(os.path.dirname(output_path),exist_ok=True)
    # summary.to_csv(output_path,index=False)
    output_key = f"summaries/dt={dt_str}/hour={hour_str}.csv"
    write_cv_to_s3(summary,output_key)
    print(f"wrote summary for {len(summary)} stations to s3://{S3_BUCKET}/{output_key}")

def check_data_quality(**context):
    execution_date = context["logical_date"] #pulls out which hour this dag run is responsible for
    dt_str = execution_date.strftime("%Y-%m-%d")
    hour_str = execution_date.strftime("%H")

    # pattern = f"{DATA_DIR}/dt={dt_str}/hour={hour_str}/*.json"
    # files = glob.glob(pattern)

    # if not files:
    #     return
    keys = list_hour_keys(dt_str,hour_str)
    if not keys:
        print(f"No files found for {dt_str}/{hour_str}, skipping.")
        return
    
    # dfs = [pd.read_json(f,lines=True) for f in files]
    dfs = [read_json_lines_from_s3(k) for k in keys]
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
        # output_path = f"{DATA_DIR}/suspicious/dt={dt_str}/hour={hour_str}.csv"
        # os.makedirs(os.path.dirname(output_path), exist_ok=True)
        # suspicious.to_csv(output_path, index=False)
        output_key = f"suspicious/dt={dt_str}/hour={hour_str}.csv"
        write_cv_to_s3(suspicious, output_key)
        print(f"WARNING {len(suspicious)} readings with 0 bikes and 0 docks - likely a feed issue, not real state")
        print(f"wrote suspicious readings to s3://{S3_BUCKET}/{output_key}")
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