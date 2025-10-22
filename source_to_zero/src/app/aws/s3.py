import boto3
import io

import polars as pl
from typing import Any, List, Dict


def __write_parquet(data: pl.DataFrame, bucket: str, filename: str) -> None:
    """
    To write parquet file into S3 bucket.

    Args:
        data (): the data in polars dataframe (not a lazyframe)
        bucket (str): the target bucket
        filename (str); The name of the object that will be put in the bucket.  
    """

    s3_client = boto3.client('s3')
    buffer    = io.BytesIO()
    
    data.write_parquet(buffer, compression="snappy")
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=filename, Body=buffer)
    

def get_objects(
    bucket: str,
    object_prefix: str
) -> List[str]:
    """
    Retrieves a list of object names from S3 based on the provided prefix.

    Parameters:
        bucket (str): The S3 bucket name.
        object_prefix (str): Prefix to filter the S3 objects.

    Returns:
        List[str]: A list of object names extracted from the S3 keys.
    """
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    
    files: List[str] = []
    # Iterate through each page returned by the paginator
    for page in paginator.paginate(Bucket=bucket, Prefix=object_prefix):
        # Check if the page contains any objects
        if 'Contents' in page:
            # Append the file name (last part of the key) to the list
            files.extend([obj['Key'].split('/')[-1] for obj in page['Contents']])

    return files



    