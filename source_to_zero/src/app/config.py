import schema.raw as raw
import os

# API 
ROOT_API_ENDPOINT   = os.getenv("ROOT_API_ENDPOINT")

BETS_ONLINE_SCHEMA  = raw.BETS_INFORMATION_RAW['schema']
FIELDS_TO_KEEP      = BETS_ONLINE_SCHEMA.keys()

# S3
BUCKET_TARGET       = os.getenv("BUCKET_TARGET")
OBJECT_KEY          = os.getenv("OBJECT_KEY")
DELAY_TIME          = int(os.getenv("DELAY_TIME", 3))  # Default to 3 minutes if not set