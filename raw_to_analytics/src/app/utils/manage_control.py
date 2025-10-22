import polars as pl
import aws.s3 as s3
import catalog_error.BusinessError as BusinessError

def update_control_information(control_df: pl.DataFrame, new_register: dict, schema: dict) -> pl.DataFrame:

    try:
        control_df = (
            control_df
            .with_columns([
                pl.when(pl.col("index") == new_register['index']).then(pl.lit(new_register['insertion_type'])).otherwise(pl.col("insertion_type")).alias("insertion_type"),
                pl.when(pl.col("index") == new_register['index']).then(pl.lit(new_register['n_registers'])).otherwise(pl.col("n_registers")).alias("n_registers"),
                pl.when(pl.col("index") == new_register['index']).then(pl.lit(new_register['duration'])).otherwise(pl.col("duration")).alias("duration"),
                pl.when(pl.col("index") == new_register['index']).then(pl.lit(new_register['start_execution'].strftime("%Y-%m-%dT%H:%M:%S.%f"))).otherwise(pl.col("start_execution")).alias("start_execution"),
                pl.when(pl.col("index") == new_register['index']).then(pl.lit(new_register['end_execution'].strftime("%Y-%m-%dT%H:%M:%S.%f"))).otherwise(pl.col("end_execution")).alias("end_execution"),
                pl.when(pl.col("index") == new_register['index']).then(pl.lit(new_register['comments'])).otherwise(pl.col("comments")).alias("comments"),
            ])
            .cast(schema)
        )
    except Exception as e:
        print("[ERROR] A problemn updating yugioh information. Check the correct new_register schema or the parameters")
        raise e

    return control_df


def schedule_master_next_execution(control_df: pl.DataFrame, new_register: dict, schema: dict, bucket: str, yugioh_key: str) -> None:
    
    try:

        next_execution = pl.DataFrame({
            "index"          : new_register['index'],
            "day"            : new_register['day'],
            "insertion_type" : "----", 
            "n_registers"    : 0, 
            "duration"       : 0.0,
            "start_execution": "",
            "end_execution"  : "",
            "comments"       : "---"

        }, schema=schema)
        control_df = pl.concat([control_df, next_execution], how="vertical")
        s3.__write_csv(data=control_df, bucket=bucket, filename=yugioh_key)
        
    except Exception as e:
        print("[ERROR] A problemn updating the yugioh csv. Check the correct parameters in the functioon")
        raise BusinessError.BusinessError(e, "LTB-EXT-CFE-002")

