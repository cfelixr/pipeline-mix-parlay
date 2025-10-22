import gc
import polars as pl

import utils.dist_information as dist
import transformation.conversion as conv
import aws.s3 as s3

from datetime import datetime, timedelta


def __delete_all(bucket: str, objects_prefix: str) -> None:
    files = s3.get_objects_complete(bucket, objects_prefix)
    s3.delete_all_objects(bucket, [{'Key': file} for file in files])


def __insert_information_into_partition(old_data: pl.DataFrame,
                                        new_data: pl.DataFrame,
                                        field_id: str,
                                        field_to_remove_duplicates: str,
                                        bucket: str,
                                        objects_prefix: str,
                                        partition_size: int) -> None:

    if old_data is not None and not old_data.is_empty():
        p_master_data_df = conv.__remove_duplicates(pl.concat(
            [new_data, old_data], how='vertical'), field_id, field_to_remove_duplicates)
        __delete_all(bucket, objects_prefix)

    else:
        p_master_data_df = conv.__remove_duplicates(
            new_data, field_id, field_to_remove_duplicates)
        
    dist.__write_information_into_partitions(
        p_master_data_df,
        bucket=bucket,
        data_prefix=objects_prefix,
        partition_size=partition_size
    )

    del p_master_data_df
    gc.collect()


def insert_information_into_table(
        batch_data_df: pl.DataFrame,
        schema: dict,
        field_id: str,
        field_to_remove_duplicates: str,
        bucket_target: str,
        prefix_target: str,
        partition_size: int) -> None:

    uniq_winlost_bet_dates = (
        batch_data_df
        .select("a_year", "a_month", "a_day")
        .filter(pl.col('a_year').is_not_null())
        .unique()
        .sort(["a_year", "a_month", "a_day"], descending=False)
    )

    for a_year, a_month, a_day in uniq_winlost_bet_dates.iter_rows():
        # filter actual bet with winlostdate
        print(f"Processing: {a_year}-{a_month}-{a_day}")
        new_analytics_filtered = (
            batch_data_df.filter(
                pl.col('a_year') == a_year,
                pl.col('a_month') == a_month,
                pl.col('a_day') == a_day
            )
            .select(schema.keys())
        )
        opts = {"aws_region": "us-east-2"}
        try:
            # Get data for that day from Analytics
            partitition_prefix=f"{prefix_target}/bets/year={a_year:04d}/month={a_month:02d}/day={a_day:02d}/"
            partition_uri = f's3://{bucket_target}/{partitition_prefix}'
            
            old_analytics_filtered = pl.read_parquet(
                partition_uri, storage_options=opts)
            
            missing = [c for c in schema.keys() if c not in old_analytics_filtered.columns]
            if missing:
                old_analytics_filtered = old_analytics_filtered.with_columns([
                    pl.lit(None).cast(schema[c]).alias(c) for c in missing
                ])
            old_analytics_filtered = old_analytics_filtered.select([pl.col(c).cast(schema[c]) for c in schema.keys()])

            print(f"old_analytics_url: {partition_uri}")
            print(f"old_analytics_filtered: {old_analytics_filtered.shape}")
         
            if old_analytics_filtered.is_empty():
                raise Exception("We don't have that file")

        except FileNotFoundError as e:
            print(f"[WARN] No existe partición para {a_year}-{a_month:02d}-{a_day:02d}: {e}")
            old_analytics_filtered = None
        except Exception as e:
            print(f"[WARN] Leyendo {partition_uri} ({a_year}-{a_month:02d}-{a_day:02d}): {type(e).__name__}: {e}")
            old_analytics_filtered = None
            
        __insert_information_into_partition(
            old_analytics_filtered,
            new_analytics_filtered,
            field_id,
            field_to_remove_duplicates,
            bucket_target,
            partitition_prefix,
            partition_size=partition_size
        )

        del old_analytics_filtered, new_analytics_filtered
        gc.collect()


