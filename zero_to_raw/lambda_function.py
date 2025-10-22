import json
import polars as pl

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import config
import schema.raw as raw
import schema.master as master
import utils.manage_control as control
import utils.dist_information as dist
import utils.helpers as helpers
import catalog_error.BusinessError as BusinessError

def lambda_handler(event, context):

    for table_name, table_info in raw.TABLES_INFORMATION_RAW.items():
        curr_day = event.get("day")
        tz = ZoneInfo("America/Caracas")

        if curr_day:
            partition_day = curr_day
        else:
            partition_day = (datetime.now(tz) - timedelta(days=1)).strftime("%Y%m%d")
        
        # Table information
        table_raw_schema = table_info['schema']
        table_config     = table_info['configuration']
        
        # Configuration
        table_batch_size     = table_config['batch_size']
        table_partition_size = table_config['partition_size']

        # Control 
        #yugioh_s3_key = F"{config.RAW_DB}/control/{table_name}.csv"
        #yugioh_s3_uri = f"s3://{config.RAW_BUCKET}/{yugioh_s3_key}"

        
        # Day information
        source_partition_uri = f's3://{config.SOURCE_BUCKET}/{config.SOURCE_DB}/{table_name}/day={partition_day}/'
        print(f"[INFO] Day information: {source_partition_uri}")

        raw_data_df  = (
            pl.scan_parquet(source_partition_uri, schema=table_raw_schema)
            .select(table_raw_schema.keys())
            .cast(table_raw_schema)
            .collect()
        )
        
        print(f"[INFO] Saving Data in RAW Bucket")
        print(f"[INFO] Number of rows: {raw_data_df.height}")
        # Move information to Raw Layer
        raw_target_prefix = f'{config.RAW_DB}/{table_name}/day={partition_day}/'
        
        n_batches = dist.__write_information_bactches(
            data_df       =raw_data_df,
            bucket        =config.RAW_BUCKET,
            data_prefix   =raw_target_prefix,
            batch_size    =table_batch_size,
            partition_size=table_partition_size
        )
        
        # Updating Control file
        #print(f"[INFO] Updating Control YuGiOh table of {table_name}")
        try:
            #control_df = helpers.read_csv_s3(yugioh_s3_uri, schema=master.CONTROL_SCHEMA)
            pass
            
        except Exception as e:
            raise BusinessError.BusinessError(e, "LTB-EXT-CFE-001")

        information = {
            'day'      : partition_day,
        }
        #control.schedule_master_next_execution(control_df, information, schema=master.CONTROL_SCHEMA, bucket=config.RAW_BUCKET, yugioh_key=yugioh_s3_key)

        del raw_data_df

    return {
        'statusCode': 200,
        'status': 'OK',
        'body': json.dumps({"day_of_execution":partition_day})
    }
