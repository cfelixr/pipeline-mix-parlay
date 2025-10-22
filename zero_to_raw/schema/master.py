import polars as pl


CONTROL_SCHEMA = {
    'index' : pl.UInt32,
    'day'   : pl.String,
    'insertion_type': pl.String,
    'n_registers'   : pl.UInt32,
    'duration'      : pl.Float32,
    'start_execution': pl.String,
    'end_execution' : pl.String,
    'comments'      : pl.String
}