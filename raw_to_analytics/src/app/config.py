import os
import schema.raw as raw
import schema.master as master


# s3-bucket-prod-lake-analytics/bd_bets
RAW_BUCKET = os.getenv("RAW_BUCKET")
RAW_BUCKET_TABLE = os.getenv("RAW_BUCKET_TABLE")

MASTER_BUCKET = os.getenv("MASTER_BUCKET")
MASTER_BUCKET_TABLE = os.getenv("MASTER_BUCKET_TABLE")

ANALYTIC_BUCKET = os.getenv("ANALYTIC_BUCKET")
ANALYTIC_BUCKET_TABLE = os.getenv("ANALYTIC_BUCKET_TABLE")

DAY_BATCH = os.getenv("DAY_BATCH")


BET_RAW_METADATA = raw.BETS_INFORMATION_RAW
BET_MASTER_METADATA = master.BETS_INFORMATION_MASTER

BET_CONTROL_SCHEMA = master.CONTROL_SCHEMA

STATUS = ['WON', 'LOSE', 'DRAW']
