import os
import polars as pl
import urllib3
import gc
import json

import aws.s3 as s3

from datetime import datetime, timedelta
import utils.helpers as h
from zoneinfo import ZoneInfo

# UTC−4
TZ_MINUS4 = ZoneInfo('Etc/GMT+4')



def initialization_timestamp(files:list, args: list) -> tuple[str, str]:

    """
    Get the last timestamp as a reference, searching the last diference timestamp into the s3 filenames.

    Args:
        args (list): Received argunments when the code was running.

    Returns:
        max_modify_date (str): The last max_modify_date, extracted from the filenames.
        max_tstamp (str): The last max_tstamp, extracted from the filenames.

    Raises:
        ConnectionError: If it cannot access to s3 files or cannot extract the information from the filenames.
        ValueError: If any file was founded, and it do not have any received paramenter.
    """

    if  len(files) > 0:
        print(f'Last parquet is founded. Searching information ...')

        try:
            last_parquet_file = max(files, key=lambda filename: datetime.strptime(filename.split('_')[-2].split('-')[-1], '%Y%m%d%H%M%S'))
            print(f'parquet file: {last_parquet_file}')

            # Getting variables
            max_modify_date = datetime.strptime(last_parquet_file.split('_')[-2].split('-')[-1], "%Y%m%d%H%M%S").strftime("%Y-%m-%dT%H:%M:%S.000")
            max_tstamp      = h.format_tstamp(last_parquet_file.split('_')[-1].split('.')[0])

            print(f'max_modify_date : {max_modify_date}')
            print(f'max_tstamp      : {max_tstamp}')

        except Exception as e:
            print("Probably, It is a problem with IAM Role or other configuration around the role and ec2. Check permittions")
            raise ConnectionError("Cannot extract information from the files in S3.")

        print(f'Variables was restored ...')

    else:
        print(f'There are not parquet files. Reading arguments ...')
        max_modify_date = None

        if len(args) == 1:
            print("ERROR: If there non files in the S3, please insert the argument to use as initial timestamp")
            raise ValueError("ERROR: If there non files in the S3, please insert the argument to use as initial timestamp")
        else:
            max_tstamp = args[1]

    return max_modify_date, max_tstamp


def write_to_s3(
    bets_data: list,
    schema: dict,
    start_date: str,
    end_date: str,
    bucket_name: str,
    prefix: str,
    marked: str,
    ) -> None:

    """
    Write dataframe into S3

    Args:
        bets_data (int): List of records to save as a parquet file with the schema.
        timestamp (str): Timestamp to include into the filename to track the evolution of fetch information in online process.
        start_modify_date (str): The minimun modify date to include in the filename
        end_modify_date (str): The maximun modify date to include in the filename
    """

    try:
        # Convert the input data to a Polars DataFrame
        data_df     = pl.DataFrame(bets_data, schema=schema)

        # Write the DataFrame to a Parquet file in memory with snappy compression
        start_date  = datetime.strptime(h.format_date(start_date), "%Y-%m-%dT%H:%M:%S.%f")
        end_date    = datetime.strptime(h.format_date(end_date), "%Y-%m-%dT%H:%M:%S.%f")

        folder_name = (end_date - timedelta(hours=3)).strftime("%Y%m%d")
        batch_name  = end_date.replace(minute=0, second=0, microsecond=0).strftime("%Y%m%d%H%M%S")
        start_str   = start_date.strftime("%Y%m%d%H%M%S")
        end_str     = end_date.strftime("%Y%m%d%H%M%S")

        # Format the S3 object key using provided dates and object prefix
        if marked:
            object_key = f'{prefix}day={folder_name}/batch={batch_name}/part_{start_str}-{end_str}_{marked}.snappy.parquet'
        else:
            object_key = f'{prefix}day={folder_name}/batch={batch_name}/part_{start_str}-{end_str}.snappy.parquet'

        s3.__write_parquet(data_df, bucket_name, object_key)
        print(f"Uploaded {object_key} to S3")
        gc.collect()

    except Exception as e:
        # Log any errors encountered during the upload process
        print(f"Failed to upload to S3, please check the permittions: {e}")
        raise e


def fetch_bets_data_by_timestamp(api_url: str, timestamp: str) -> list[list, str]:

    """
    A function to fetch information from Taiwan Team using the timestamp version of bets API Route

    Args:
        api_url (str): Root API from API.
        timestamp (str): The API to use to fetch information

    Returns:
        bets (list): The list of bets records.
        maxTimestamp (str): The last max_tstamp, you can expect that the timestamp is the maxium in the set (the bets list).
    """

    http        = urllib3.PoolManager()
    response    = http.request('GET', f'{api_url}?timestamp={timestamp}')

    if response.status != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}")

    data = json.loads(response.data.decode('utf-8'))
    print("maxMobiusModifiedOn", data["maxMobiusModifiedOn"], "date server:", datetime.now())
    print("maxTimestamp", data["maxTimestamp"])
    return data['bets'], data['maxTimestamp'], data['maxMobiusModifiedOn']

def parse_iso_timestamp(timestamp_str):
    """
    Intenta convertir un string ISO 8601 (con o sin fracción de segundo) en datetime.
    Soporta:
      - "YYYY-MM-DDTHH:MM:SS"
      - "YYYY-MM-DDTHH:MM:SS.f"      (1 a 6 dígitos en la parte fraccional)
    """
    formatos = [
        "%Y-%m-%dT%H:%M:%S.%f",  # con fracción de segundo (1–6 dígitos)
        "%Y-%m-%dT%H:%M:%S",     # sin fracción de segundo
    ]
    for fmt in formatos:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    # Si llegamos aquí, ninguno de los formatos encajó:
    raise ValueError(f"Formato de fecha inválido: {timestamp_str}")

def is_old_enough(timestamp_str, DELAY_TIME=3):
    """
    Devuelve True si la fecha en timestamp_str tiene una antigüedad mayor a DELAY_TIME
    respecto a ahora. Levanta ValueError si el formato no coincide con ninguno de los soportados.
    """
    srt = datetime.now(TZ_MINUS4)
    print("Server relative time", srt)
    timestamp = parse_iso_timestamp(timestamp_str)
    timestamp = timestamp.replace(tzinfo=TZ_MINUS4)
    print("Adding tzinfo de UTC−4 to MaxMobiusModifiedDate:", timestamp)
    return (srt - timestamp) > timedelta(minutes=DELAY_TIME)