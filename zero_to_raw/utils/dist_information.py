import polars as pl
import aws.s3 as s3


def __write_information_into_partitions(data_df: pl.DataFrame, bucket: str, data_prefix:str, partition_size: int = 100000):

    partition_index = 0
    for i in range(0, data_df.height, partition_size): 
        partition       = data_df.slice(i, partition_size)
        object_key      = f'{data_prefix}part_{partition_index:012d}.snappy.parquet'
        partition_index += 1

        s3.__write_parquet(partition, bucket, object_key)
        del partition

    return partition_index


def __write_information_bactches(data_df: pl.DataFrame, bucket: str, data_prefix:str, batch_size: int = 400000, partition_size: int = 100000):

    batch_index = 0
    for i in range(0, data_df.height, batch_size): 
        batch_df     = data_df.slice(i, batch_size)
        _data_prefix = f'{data_prefix}batch={batch_index:012d}/'
        batch_index  += 1
        
        __write_information_into_partitions(batch_df, bucket, _data_prefix, partition_size)
        print(f'Batch {batch_index} on {_data_prefix} was written successfully')
        del batch_df

    return batch_index