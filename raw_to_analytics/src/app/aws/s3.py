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
    

def __write_csv(data: pl.DataFrame, bucket: str, filename: str) -> None:
    """
    To write csv file into S3 bucket.

    Args:
        data (): the data in polars dataframe (not a lazyframe)
        bucket (str): the target bucket
        filename (str); The name of the object that will be put in the bucket.  
    """

    s3_client = boto3.client('s3')
    buffer    = io.BytesIO()
    
    data.write_csv(buffer, separator=",")
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=filename, Body=buffer)


def get_objects(bucket: str, object_prefix: str) -> List[str]:
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


def get_objects_complete(bucket: str, object_prefix: str) -> List[str]:
    """
    Retrieves a list of object from S3 based on the provided prefix.

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
            files.extend([obj['Key'] for obj in page['Contents']])

    return files


def delete_all_objects(bucket_name, delete_objects):
    # Dividir la lista de archivos en lotes de 1000 elementos
    chunk_size = 1000
    s3_client = boto3.client('s3')
    
    for i in range(0, len(delete_objects), chunk_size):
        chunk = delete_objects[i:i + chunk_size]

        # Hacer la llamada para eliminar el lote actual
        s3_client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': chunk
            }
        )