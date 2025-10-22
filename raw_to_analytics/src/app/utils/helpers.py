from typing import Optional, List, Any
import polars as pl
import io
import boto3
from urllib.parse import urlparse

s3 = boto3.client("s3")


def format_date(date: Optional[str]) -> Optional[str]:
    """
    Append milliseconds to a date string.

    - If `date` is None or empty, return it as is.
    - If the string is 19 characters long, append '.000'.
    - Otherwise, pad the string to 23 characters with '0'.
    """
    return date if not date else f"{date}.000" if len(date) == 19 else date.ljust(23, "0")


def format_tstamp(tstamp: str) -> str:
    """
    To correct a timestamp

    Args:
        tstamp (str): the tstamp.

    Returns:
        tstamp (str): The correct tstamp.
    """

    return "0x" + tstamp.upper() if tstamp[:2] != '0x' and len(tstamp) == 16 else "0x" + tstamp[2:].upper()


def remove_fields(record: dict, fields_to_keep: list) -> dict:
    """
    A function to remove useless fileds in the records

    Args:
        data (list): The data with useless fields.

    Returns:
        data (list): The data only with selected files.
    """

    return {key: value for key, value in record.items() if key in fields_to_keep}


def standard_date(record: dict, fields_to_format: dict):
    """
    Standarize the datetime fiels in all records.

    Args:
        data (list): The data without standard datetime files.

    Returns:
        data (list): The standard data.
    """

    for field in fields_to_format:
        record[field] = standard_date(record[field])

    record['tstamp'] = standard_date(record['tstamp'])
    return record


def preprocess_data(data: list, fields_to_keep: list) -> list:
    """
    A function to pre processing the data.

    Args:
        data (list): The data without pre processing.
        fields_to_keep (list): field to keep in all records

    Returns:
        data (list): The preprocessed data.
    """
    return [remove_fields(x, fields_to_keep) for x in data]


def update_control_information(control_df: pl.DataFrame, new_register: dict, schema: dict) -> pl.DataFrame:

    try:
        control_df = (
            control_df
            .with_columns([
                pl.when(pl.col("index") == new_register['index']).then(pl.lit(
                    new_register['insertion_type'])).otherwise(pl.col("insertion_type")).alias("insertion_type"),
                pl.when(pl.col("index") == new_register['index']).then(pl.lit(
                    new_register['n_registers'])).otherwise(pl.col("n_registers")).alias("n_registers"),
                pl.when(pl.col("index") == new_register['index']).then(pl.lit(
                    new_register['duration'])).otherwise(pl.col("duration")).alias("duration"),
                pl.when(pl.col("index") == new_register['index']).then(pl.lit(new_register['start_execution'].strftime(
                    "%Y-%m-%dT%H:%M:%S.%f"))).otherwise(pl.col("start_execution")).alias("start_execution"),
                pl.when(pl.col("index") == new_register['index']).then(pl.lit(new_register['end_execution'].strftime(
                    "%Y-%m-%dT%H:%M:%S.%f"))).otherwise(pl.col("end_execution")).alias("end_execution"),
                pl.when(pl.col("index") == new_register['index']).then(pl.lit(
                    new_register['comments'])).otherwise(pl.col("comments")).alias("comments"),
            ])
            .cast(schema)
        )
    except Exception as e:
        print("[ERROR] A problemn updating yugioh information. Check the correct new_register schema or the parameters")
        raise e

    return control_df

def read_csv_s3(uri: str, **kwargs) -> pl.DataFrame:
    p = urlparse(uri)              # s3://bucket/path/to/file.csv
    bucket = p.netloc
    key = p.path.lstrip("/")
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]  # StreamingBody
    # Polars acepta cualquier file-like con .read(); lo envolvemos en BytesIO por seguridad
    return pl.read_csv(io.BytesIO(body.read()), **kwargs)
