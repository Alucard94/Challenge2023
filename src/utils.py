import sys
import os 
import boto3
import json
import pandas as pd
from pandas import json_normalize
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs
import io
import datetime
import time
import json

AWS_PUBLIC_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
RAW_BUCKET = os.getenv('RAW_BUCKET')
LANDING_BUCKET = os.getenv('LANDING_BUCKET')
CURRENT_DAY = str(datetime.date.today()).replace('-', '')[:8]
PROCESS_DATE = str(datetime.date.today() - datetime.timedelta(days = 1))[:10]

def aws_session():
    session = boto3.Session(
        aws_access_key_id = AWS_PUBLIC_KEY,
        aws_secret_access_key = AWS_SECRET_KEY,
        region_name = "us-east-1"
    )
    return session


def get_incremental_file(prefix):
    """ Return a list with all json files with last day events
        Parameters
        ----------
        prefix: str
            Folder name where raw data is stored (in json format)
    """
    s3 = aws_session().client('s3')
    s3_res = aws_session().resource('s3')

    # List objects in a folder inside S3 bucket, we remove the root folder (first element of returned list)
    objs = s3.list_objects_v2(Bucket = RAW_BUCKET, Prefix = prefix + "/")
    files = [obj['Key'] for obj in objs['Contents']][1:]
    files = [f for f in files if PROCESS_DATE in f] # Use PROCESS_DATE value to filter files from last day

    # Create a list with all json objects readed from bucket
    jsonList = []
    for filename in files:
        obj = s3_res.Object(bucket_name = RAW_BUCKET, key = filename)
        file_content = obj.get()['Body'].read().decode('utf-8') 
        s3_res.Bucket(RAW_BUCKET).download_file(filename, filename.split('/')[1])        

        with open(filename.split('/')[1]) as f:
            for jsonObj in f:
                jsonDict = json.loads(jsonObj)
                jsonList.append(jsonDict)
                
    print('Daily scan completed!')
    return jsonList


def upload_parquet(df, bucket_name, folder, suffix = ''):
    """ Generates a parquet file with daily events
        Parameters
        ----------
        df: Dataframe
            Object with all last day events from an entity
        bucket_name: str
            Name of staging bucket
        folder: str
            Name of folder where data is going to be stored
        suffix: str (optional)
            Used to identify a particular file (not required)
    """
    # Adding parquet file to S3 bucket originated from dataframe with json data
    s3_client = aws_session().client('s3')
    
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    FILEPATH = folder + "/" + CURRENT_DAY + "/" + current_time + suffix + ".parquet"

    if not os.path.exists(folder + "/" + CURRENT_DAY + "/"):
        os.makedirs(folder + "/" + CURRENT_DAY + "/")

    ptable = pa.Table.from_pandas(df)
    pq.write_table(ptable, FILEPATH, use_deprecated_int96_timestamps = True)

    with open('./' + FILEPATH, "rb") as file:
        s3_client.put_object(
            Bucket = bucket_name, Key = FILEPATH, Body = file
        )
    
    print('Load completed!')


def generate_dataframe_from_daily_file(folder):
    """ Scan the json files with the last day events and generate one dataframe per entity
        Parameters
        ----------
        folder: str
            Folder name of RAW bucket
    """
    jsonList = get_incremental_file(folder)
    
    # Append objects according to the entity event
    df_vehicle = []
    df_operating_period = []

    for jsonObj in jsonList:
        df = json_normalize(jsonObj) 
        if jsonObj['on'] == 'operating_period':
            df_operating_period.append(df)
        if jsonObj['on'] == 'vehicle':
            df_vehicle.append(df)
    
    # Upload data in parquet format to the corresponding folder
    if len(df_operating_period) > 0:
        data_operating_period = pd.concat(df_operating_period, axis = 0, ignore_index = True)
        upload_parquet(data_operating_period, LANDING_BUCKET, 'operating_period')
    if len(df_vehicle) > 0:
        data_vehicle = pd.concat(df_vehicle, axis = 0, ignore_index = True)
        upload_parquet(data_vehicle, LANDING_BUCKET, 'vehicle')


def get_glue_status(job_name, job_run_id):
    """ Get glue job state to verify the execution completed correctly
        Parameters
        ----------
        job_name: str
            Glue job name
        job_run_id: str
            Glue job id (unique identifier of job execution)
    """
    glue = aws_session().client('glue')
    status_detail = glue.get_job_run(JobName=job_name, RunId = job_run_id.get("JobRunId"))
    status = status_detail.get("JobRun").get("JobRunState")

    return status


def start_glue(job_name):
    """ Starts a glue job. It receives job name as parameter """
    RUNNING_STATUS = ['STARTING', 'RUNNING', 'STOPPING']

    glue = aws_session().client('glue')    
    run_id = glue.start_job_run(JobName=job_name)
    status = get_glue_status(job_name, run_id)

    while status in RUNNING_STATUS:
        time.sleep(30)
        status = get_glue_status(job_name, run_id)
        
    if status == 'SUCCEEDED':
        print(job_name + " completes successfully")
    else:
        print('ERROR in ' + job_name + ' please check')
        exit(1)
