import polars as pl
import utils.helpers as h
from typing import Union
import config as c

STATUS = ['WON', 'LOSE', 'DRAW']

FIELD_TO_REMOVE_DUPLICATES = c.BET_RAW_METADATA["information"]["field_modify_date"][:1].upper(
)+ c.BET_RAW_METADATA["information"]["field_modify_date"][1:]

PK_COL = [col[:1].upper() + col[1:].upper()
          for col in c.BET_RAW_METADATA["information"]["field_id"]]


def __convert_fields(
    data_df: pl.LazyFrame,
    fields_datetime_to_format: list,
    fields_special_datetime_to_format: list,
    fields_date_to_format: list,
    fields_tstamp_to_format: list,
    fields_str_to_format: list
):
    def datetime_transform(field): return (
        pl.col(field)
          .map_elements(h.format_date, return_dtype=pl.String)
          .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f")
          .alias(field)
    )

    def special_datetime_transform(field): return (
        pl.col(field)
          .str.to_datetime(format="%m/%d/%Y %H:%M:%S")
          .alias(field)
    )

    def date_transform(field): return (
        pl.col(field)
          .map_elements(h.format_date, return_dtype=pl.String)
        # Convert to datetime
          .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f")
          .dt.date()
          .alias(field)
    )

    def tstamp_transform(field): return (
        pl.col(field)
          .map_elements(h.format_tstamp, return_dtype=pl.String)
          .alias(field)
    )

    def str_transform(field): return (
        pl.col(field)
          .str.to_uppercase()
          .alias(field)
    )

    for fields, transform in [
        (fields_datetime_to_format, datetime_transform),
        (fields_special_datetime_to_format, special_datetime_transform),
        (fields_date_to_format, date_transform),
        (fields_tstamp_to_format, tstamp_transform),
        (fields_str_to_format, str_transform)
    ]:
        if fields:
            data_df = data_df.with_columns(
                [transform(field) for field in fields])

    return data_df


def __remove_duplicates(data_df: Union[pl.DataFrame, pl.LazyFrame], field_id: list, field_to_remove_duplicates: str):

    data_df = (
        data_df
        .sort(field_to_remove_duplicates, descending=False)
        .unique(subset=field_id, keep="last", maintain_order=True)
    )

    return data_df


def process_bets(source_url):
    # transDate
    field_master = c.BET_RAW_METADATA["to_insert"]["master"]
    # winlostDate
    field_analytics = c.BET_RAW_METADATA["to_insert"]["analytics"]

    opts = {"aws_region": "us-east-2"}

    actual_raw_bet = (
        pl.scan_parquet(source_url, storage_options=opts, schema=c.BET_RAW_METADATA["schema"])
        .select(c.BET_RAW_METADATA["schema"].keys())
    )

    print("Reading data from:", source_url, flush=True)

    print("Converting fields", flush=True)

    actual_raw_bet = __convert_fields(
        actual_raw_bet,
        fields_date_to_format=c.BET_RAW_METADATA["information"]["fields_date"],
        fields_special_datetime_to_format=c.BET_RAW_METADATA[
            "information"]["fields_special_datetime"],
        fields_str_to_format=['status', ],
        fields_tstamp_to_format=[
            c.BET_RAW_METADATA["information"]["field_tstamp"]],
        fields_datetime_to_format=c.BET_RAW_METADATA["information"]["fields_datetime"],
    )

    print("Removing duplicates", flush=True)
    actual_raw_bet = __remove_duplicates(
        actual_raw_bet,
        # primary key: customer, transId
        c.BET_RAW_METADATA["information"]["field_id"],
        # values to removie id
        c.BET_RAW_METADATA["information"]["field_modify_date"]
    )

    rename_fields_map = {col: col[:1].upper() + col[1:]
                         for col in c.BET_RAW_METADATA["schema"].keys()}

    actual_raw_bet = actual_raw_bet.with_columns(
           pl.lit('llamitav2').alias('__comment')
      )

    actual_raw_bet = (
        actual_raw_bet
        # Rename columns using predefined mapping
        .rename(rename_fields_map)
        # Cast the DataFrame to the desired schema
        .cast(c.BET_MASTER_METADATA["schema"])
        # Extract parts from TransDate
        .with_columns(
            pl.col(field_master).dt.year().alias("m_year"),
            pl.col(field_master).dt.month().alias("m_month"),
            pl.col(field_master).dt.day().alias("m_day")
        )
        # Conditionally extract parts from Winlost date field
        .with_columns(  # only for support our logic
            pl.when(pl.col("Ruben")==1)
            .then(pl.col(field_analytics).dt.year())
            .otherwise(None)
            .alias("a_year"),

            pl.when(pl.col("Ruben")==1)
            .then(pl.col(field_analytics).dt.month())
            .otherwise(None)
            .alias("a_month"),

            pl.when(pl.col("Ruben")==1)
            .then(pl.col(field_analytics).dt.day())
            .otherwise(None)
            .alias("a_day")
        ).collect()
    )
    return actual_raw_bet
