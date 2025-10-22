
import polars as pl
import transformation.insertion as ins
import config as c
import utils.dist_information as d


class RareCases:
    def __init__(self, bucket_name: str, bucket_prefix:str):
        self.bucket_name = bucket_name
        self.bucket_prefix = bucket_prefix

    def set_data(self, actual_data, schema):
        self.actual_data = actual_data
        self.schema = schema

    def _compute_changed_winlost_date(actual_data, old_data):
        return (
            actual_data
            .select('main_Customer', 'main_TransId',
                    pl.col('main_ModifyDate').alias(
                        'new_ModifyDate'),
                    pl.col('main_Winlostdate').alias(
                        'new_Winlostdate'),
                    pl.col('Status').alias('new_Status'))
            .join(
                old_data.select(
                    'main_Customer', 'main_TransId',
                    pl.col('main_ModifyDate').alias(
                        'old_ModifyDate'),
                    pl.col('main_Winlostdate').alias(
                        'old_Winlostdate'),
                    pl.col('Status').alias('old_Status')),
                on=['main_Customer', 'main_TransId']
            )
            .filter(
                (pl.col('new_ModifyDate') > pl.col('old_ModifyDate')) &
                (
                    ((pl.col('old_Winlostdate') != pl.col('new_Winlostdate')) &
                     pl.col('old_Status').is_in(c.STATUS)) |
                    (pl.col('old_Status').is_in(c.STATUS) &
                     ~pl.col('new_Status').is_in(c.STATUS))
                )
            )
            .select('main_Customer', 'main_TransId', 'old_Winlostdate')
        )

    def _compute_odd_modified_dates(actual_data, old_data):
        return (
            actual_data
            .select('main_Customer', 'main_TransId', pl.col('main_ModifyDate').alias('new_ModifyDate'))
            .join(
                old_data
                .select('main_Customer', 'main_TransId', pl.col('main_ModifyDate').alias('old_ModifyDate')),
                on=['main_Customer', 'main_TransId'], how='inner'
            )
            .filter(
                [
                    pl.col('new_ModifyDate') < pl.col('old_ModifyDate'),
                ]
            )
            .select('main_Customer', 'main_TransId')
        )

    def _get_unique_dates_based_on_transDate(self, data):
        uniq_dates = (
            data
            .select("m_year", "m_month", "m_day")
            .unique()
            .sort(["m_year", "m_month", "m_day"], descending=False)
        )
        return uniq_dates

    def _read_data(self, url):
        opts = {"aws_region": "us-east-2"}
        try:
            data = pl.read_parquet(url, storage_options=opts, schema=self.schema)

            if data.is_empty():
                raise Exception("There is no data")

        except Exception as e:
            data = None

        return data

    def remove_data_based_on_ids(ids_to_remove, bucket_target, prefix_target, schema, partition_size):

        # Get unique winlostdate for modified winlostdate
        uniq_winlost_dates = (
            ids_to_remove_df
            .select("year", "month", "day")
            .unique()
            .sort(["year", "month", "day"], descending=False)
        )

        for year, month, day in uniq_winlost_dates.iter_rows():

            # Filter modified winlostdate for a specific day
            ids_to_remove_df = (
                ids_to_remove_df
                .filter(
                    pl.col("year") == year,
                    pl.col("month") == month,
                    pl.col("day") == day
                )
                .select('main_Customer', 'main_TransId')
            )

            opts = {"aws_region": "us-east-2"}

            try:
                # Read analytics data for that specific winlostdate
                part_prefix = f"{prefix_target}/mp/year={year:04d}/month={month:02d}/day={day:02d}/"
                partition_uri = f's3://{bucket_target}/{part_prefix}'
                analytic_bet = pl.read_parquet(partition_uri, storage_options=opts, schema=schema)

                if analytic_bet.is_empty():
                    raise Exception("File not found")

                # Get rid of wierd modified winlostdate from analytics data
                updated_analytic_bet = (
                    analytic_bet
                    .join(ids_to_remove_df, on=['main_Customer', 'main_TransId'], how="anti")
                )

                # delete analytic data from bet
                ins.__delete_all(bucket_target,
                                 part_prefix)

                # update analytics with new filtered bet
                d.__write_information_into_partitions(
                    updated_analytic_bet,
                    bucket=bucket_target,
                    data_prefix=part_prefix,
                    partition_size=partition_size
                )

                del updated_analytic_bet

            except FileNotFoundError as e:
                analytic_bet = None

            del marked_ids_day_df
            del analytic_bet

        del ids_to_remove_df

    def run(self):
        uniq_trans_dates = self._get_unique_dates_based_on_transDate(
            self.actual_data)

        remove_ids_df=None
        ignore_ids_df=None

        for m_year, m_month, m_day in uniq_trans_dates.iter_rows():
            # Filter data for specific transDate
            print(f"Processing: {m_year}-{m_month}-{m_day}")
            new_master_info_df = (
                self.actual_data.filter(
                    pl.col("m_year") == m_year,
                    pl.col("m_month") == m_month,
                    pl.col("m_day") == m_day
                )
                .select(self.schema.keys())
            )

            partition_url = f's3://{self.bucket_name}/{self.bucket_prefix}/mp/year={m_year:04d}/month={m_month:02d}/day={m_day:02d}/'

            old_master_info_df = self._read_data(partition_url)

            if old_master_info_df is not None:
                changed_wldate_df = self._compute_changed_winlost_date(
                    new_master_info_df, old_master_info_df)

                ignore_wldate_df = self._compute_odd_modified_dates(
                    new_master_info_df, old_master_info_df)

                if not changed_wldate_df.is_empty():
                    remove_ids_df = [changed_wldate_df if remove_ids_df is None else pl.concat(
                        [remove_ids_df, changed_wldate_df], how='vertical')]

                    del changed_wldate_df

                if not ignore_wldate_df.is_empty():
                    ignore_ids_df = [ignore_wldate_df if ignore_ids_df is None else pl.concat(
                        [ignore_ids_df, ignore_wldate_df], how='vertical')]

                    del ignore_wldate_df
            
            del old_master_info_df

        # format ids to remove
        if remove_ids_df is not None:
            remove_ids_df = (
                remove_ids_df
                .with_columns(  # only for support our logic
                    pl.col('old_Winlostdate').dt.year().alias("year"),
                    pl.col('old_Winlostdate').dt.month().alias("month"),
                    pl.col('old_Winlostdate').dt.day().alias("day"),
                )
            )
        print("Remove ID")
        print(remove_ids_df)
        print("Ignore ID")
        print(ignore_ids_df)
        return remove_ids_df, ignore_ids_df
