import os

# ─────────────────────────────────────────
# S3 CONFIGURATION
# ─────────────────────────────────────────
S3_CONFIG = {
    "bronze_bucket": "payment-intel-bronze-dev",
    "bronze_prefix": "transactions",
    "region": "ap-south-1"
}

# ─────────────────────────────────────────
# KAFKA CONFIGURATION
# ─────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:29092",
    "topic": "payments.transactions",
    "starting_offsets": "earliest"
}

# ─────────────────────────────────────────
# STREAMING CONFIGURATION
# ─────────────────────────────────────────
STREAMING_CONFIG = {
    "trigger_interval": "30 seconds",   # write to S3 every 30 seconds
    "checkpoint_location": "/tmp/bronze_checkpoint",
    "output_mode": "append"             # always append to bronze — never overwrite
}

# ─────────────────────────────────────────
# TRANSACTION SCHEMA
# ─────────────────────────────────────────
# We define the schema explicitly rather than
# inferring it from the data. This is important
# because:
# 1. Schema inference is slow — reads all data first
# 2. Explicit schema catches corrupt messages early
# 3. Schema is our data contract with upstream
TRANSACTION_SCHEMA = """
    txn_id STRING,
    txn_timestamp STRING,
    arrival_timestamp STRING,
    amount DOUBLE,
    currency STRING,
    status STRING,
    failure_reason STRING,
    user_id STRING,
    user_segment STRING,
    merchant_id STRING,
    merchant_category STRING,
    gateway_id STRING,
    gateway_name STRING,
    processor_id STRING,
    processor_name STRING,
    payment_method STRING,
    device_type STRING,
    location_city STRING,
    location_state STRING,
    is_international BOOLEAN
"""