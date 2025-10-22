import config as c
import os
import json
import gc
import time
from datetime import datetime, timedelta
import polars as pl
from zoneinfo import ZoneInfo

# import s3fs

import schema.raw as raw
import schema.master as master
import utils.helpers as h
import utils.dist_information as dist
import aws.s3 as s3
import transformation.conversion as conv
import transformation.insertion as ins
from transformation.cases import RareCases
import utils.manage_control as control
import catalog_error.BusinessError as BusinessError

STATUS = ['WON', 'LOSE', 'DRAW']

FIELD_TO_REMOVE_DUPLICATES = c.BET_RAW_METADATA["information"]["field_modify_date"][:1].upper(
)+ c.BET_RAW_METADATA["information"]["field_modify_date"][1:]

PK_COL = [col[:1].upper() + col[1:]
          for col in c.BET_RAW_METADATA["information"]["field_id"]]


def log_control(func):
    def inner(*args, **kwargs):
        datetime_start = datetime.now()
        time_start = time.time()

        func(*args, **kwargs)

        time_end = time.time()
        datetime_end = datetime.now()
        n_register = {
            'index': kwargs["index"],
            'insertion_type': "STANDARD",
            'n_registers': 0,
            'duration': time_end - time_start,
            'start_execution': datetime_start - timedelta(hours=1),
            'end_execution': datetime_end - timedelta(hours=1),
            'comments': "The execution was succesful"
        }        
        

        control_df = h.update_control_information(
            kwargs["control_df"], n_register, schema=c.BET_CONTROL_SCHEMA)
        s3.__write_csv(data=control_df, 
                        bucket=c.RAW_BUCKET,
                        filename=f"{c.RAW_BUCKET_TABLE}/control/bets.csv")


        control_analytics_bets_uri = f's3://{c.RAW_BUCKET}/{c.RAW_BUCKET_TABLE}/control/master_bets.csv'
        try:
            control_analytics_bets_df = h.read_csv_s3(control_analytics_bets_uri, schema=c.BET_CONTROL_SCHEMA, truncate_ragged_lines=True)
        except Exception as e:
            raise BusinessError.BusinessError(e, "LTB-EXT-CFE-001")
            
        information = {
            'index'    : control_analytics_bets_df.height,
            'day'      : kwargs["day"],
        }
        
        control.schedule_master_next_execution(
            control_analytics_bets_df,
            information,
            schema=c.BET_CONTROL_SCHEMA,
            bucket=c.RAW_BUCKET,
            yugioh_key=f'{c.RAW_BUCKET_TABLE}/control/master_bets.csv')
    
        
    return inner


#@log_control
def run_main(day):
    partition_size = 800000
    
    raw_source = f's3://{c.RAW_BUCKET}/{c.RAW_BUCKET_TABLE}/bets/day={day}/'
    
    # Iterate for each batch or day
    actual_raw_bet = conv.process_bets(raw_source)
    
    actual_raw_bet = (
            actual_raw_bet
            .filter(pl.col('Ruben') == 1)
        )
        
    ins.insert_information_into_table(
        batch_data_df=actual_raw_bet,
        schema=c.BET_MASTER_METADATA["schema"],
        field_id=PK_COL,
        field_to_remove_duplicates=FIELD_TO_REMOVE_DUPLICATES,
        bucket_target=c.ANALYTIC_BUCKET,
        prefix_target=c.ANALYTIC_BUCKET_TABLE,
        partition_size=partition_size
    )
    
    


# def lambda_handler(event, context):

__version__ = "0.0.9"
print(f"Version: {__version__}", flush=True)

opts = {"aws_region": "us-east-2"}

print("AWS_REGION:", os.getenv("AWS_REGION"), "AWS_DEFAULT_REGION:", os.getenv("AWS_DEFAULT_REGION"), flush=True)


#prefix_url = event.get("prefix_url")

# curr_day = "20251004"
curr_day = c.DAY_BATCH

tz = ZoneInfo("America/Caracas")

if curr_day:
    partition_day = curr_day
else:
    partition_day = (datetime.now(tz) - timedelta(days=1)).strftime("%Y%m%d")

print(f"Processing day: {partition_day}")
# Read log table
#control_uri = f's3://{c.RAW_BUCKET}/{c.RAW_BUCKET_TABLE}/control/bets.csv'

#print("Reading control table")
try:
    # control_df = h.read_csv_s3(control_uri, schema=c.BET_CONTROL_SCHEMA, truncate_ragged_lines=True)
    pass
except Exception as e:
    raise BusinessError.BusinessError(e, "LTB-EXT-CFE-001")
# Get last executed by master
#next_insertions = (
#    control_df
#    .filter(
#        pl.col('insertion_type') == '----',
#    )
#    .select('index', 'day')
#)

#if next_insertions.shape[0] == 1:
#    idx = next_insertions.select('index').to_dicts()[0]["index"]
#    day = next_insertions.select('day').to_dicts()[0]["day"]
#    print(f"Processing day: {day} with index: {idx}")

run_main(day=partition_day)

#return {
#    'statusCode': 200,
#    'body': {
#        "status": "Succeed",
#        "processed_day": partition_day
#    }
#}

#else:
#    raise Exception("No processed. See control schema. No row or many rows with (----) in insertion type!")

