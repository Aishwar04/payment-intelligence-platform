from dataclasses import dataclass, field
from typing import List, Dict

# ─────────────────────────────────────────
# KAFKA CONFIGURATION

# KAFKA_CONFIG
# Tells the simulator where Kafka is running and which topic to publish to. localhost:29092 is the port 
# we exposed for Mac host access — remember we set up two ports for Kafka: 9092 for internal Docker network 
# and 29092 for your Mac.


# ─────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap_servers": "localhost:29092",  # port 29092 is for Mac host access
    "topic": "payments.transactions",
    "num_partitions": 3,
    "replication_factor": 1
}

# ─────────────────────────────────────────
# SIMULATION CONFIGURATION

# SIMULATION_CONFIG
# Controls how fast the simulator runs. transactions_per_second: 10 means it generates 10 transactions every second. 
# late_arrival_probability: 0.05 means 5% of transactions will have a backdated txn_timestamp to simulate late arrivals 
# — exactly the problem we designed our pipeline to handle.
# ─────────────────────────────────────────

SIMULATION_CONFIG = {
    "transactions_per_second": 100,    # how many transactions to generate per second
    "total_transactions": None,       # None means run forever
    "late_arrival_probability": 0.05  # 5% of transactions arrive late
}

# ─────────────────────────────────────────
# USER CONFIGURATION

# USER_CONFIG
# Defines 10,000 fake users across 4 segments. weight controls what percentage of users belong to that segment. 
# txn_frequency controls how often that segment transacts. 
# Premium users transact more, dormant users almost never.
# ─────────────────────────────────────────


USER_CONFIG = {
    "total_users": 10000,
    "segments": {
        "premium":  {"weight": 0.10, "avg_amount": 5000,  "txn_frequency": 0.40},
        "regular":  {"weight": 0.60, "avg_amount": 800,   "txn_frequency": 0.45},
        "new":      {"weight": 0.20, "avg_amount": 300,   "txn_frequency": 0.10},
        "dormant":  {"weight": 0.10, "avg_amount": 150,   "txn_frequency": 0.05}
    }
}

# ─────────────────────────────────────────
# MERCHANT CONFIGURATION

# MERCHANT_CONFIG
# Defines 1,000 fake merchants across 8 categories. Each category has its own average transaction amount 
# and failure rate. Travel has higher failure rate (8%) because international transactions fail more. 
# Utilities have low failure rate (2%) because they're predictable recurring payments.
# ─────────────────────────────────────────
MERCHANT_CONFIG = {
    "total_merchants": 1000,
    "categories": {
        "food_delivery":     {"weight": 0.25, "avg_amount": 400,   "failure_rate": 0.03},
        "ecommerce":         {"weight": 0.20, "avg_amount": 1200,  "failure_rate": 0.04},
        "utilities":         {"weight": 0.15, "avg_amount": 2500,  "failure_rate": 0.02},
        "entertainment":     {"weight": 0.10, "avg_amount": 600,   "failure_rate": 0.05},
        "travel":            {"weight": 0.10, "avg_amount": 8000,  "failure_rate": 0.08},
        "grocery":           {"weight": 0.10, "avg_amount": 350,   "failure_rate": 0.02},
        "healthcare":        {"weight": 0.05, "avg_amount": 1500,  "failure_rate": 0.03},
        "fuel":              {"weight": 0.05, "avg_amount": 2000,  "failure_rate": 0.02}
    }
}

# ─────────────────────────────────────────
# PAYMENT GATEWAY CONFIGURATION

# GATEWAY_CONFIG
# Three payment gateways with different success rates and latencies. Razorpay is fastest and most reliable. 
# PayU is slowest. This difference is what our RL agent will learn to exploit — routing transactions to the best gateway 
# based on context.
# ─────────────────────────────────────────

GATEWAY_CONFIG = {
    "GW-001": {
        "name": "Razorpay",
        "success_rate": 0.97,
        "avg_latency_ms": 120,
        "supported_methods": ["UPI", "CARD", "NETBANKING", "WALLET"]
    },
    "GW-002": {
        "name": "PayU",
        "success_rate": 0.95,
        "avg_latency_ms": 180,
        "supported_methods": ["UPI", "CARD", "NETBANKING"]
    },
    "GW-003": {
        "name": "Cashfree",
        "success_rate": 0.96,
        "avg_latency_ms": 150,
        "supported_methods": ["UPI", "CARD", "WALLET"]
    }
}

# ─────────────────────────────────────────
# PAYMENT PROCESSOR CONFIGURATION
# PROCESSOR_CONFIG
# Four payment processors. NPCI handles all UPI — near instant settlement. Visa and Mastercard handle cards — 24 hour settlement.
# This is realistic — UPI settles in seconds in real life.
# ─────────────────────────────────────────

PROCESSOR_CONFIG = {
    "PRC-001": {
        "name": "NPCI",
        "supported_methods": ["UPI"],
        "success_rate": 0.97,
        "settlement_hours": 0
    },
    "PRC-002": {
        "name": "Visa",
        "supported_methods": ["CARD"],
        "success_rate": 0.95,
        "settlement_hours": 24
    },
    "PRC-003": {
        "name": "Mastercard",
        "supported_methods": ["CARD"],
        "success_rate": 0.94,
        "settlement_hours": 24
    },
    "PRC-004": {
        "name": "RBI_NEFT",
        "supported_methods": ["NETBANKING"],
        "success_rate": 0.98,
        "settlement_hours": 2
    }
}

# ─────────────────────────────────────────
# PAYMENT METHOD CONFIGURATION

# PAYMENT_METHOD_CONFIG
# UPI dominates at 55% — realistic for India. Cards at 25%. Each method has its own failure rate and maps to a specific 
# processor.
# ─────────────────────────────────────────
PAYMENT_METHOD_CONFIG = {
    "UPI":        {"weight": 0.55, "failure_rate": 0.025, "processor": "PRC-001"},
    "CARD":       {"weight": 0.25, "failure_rate": 0.060, "processor": "PRC-002"},
    "NETBANKING": {"weight": 0.12, "failure_rate": 0.040, "processor": "PRC-004"},
    "WALLET":     {"weight": 0.08, "failure_rate": 0.030, "processor": "PRC-001"}
}

# ─────────────────────────────────────────
# FAILURE REASON CONFIGURATION

# FAILURE_REASONS
# Weighted distribution of why transactions fail. Insufficient funds is the most common real-world failure reason at 35%.
# ─────────────────────────────────────────
FAILURE_REASONS = {
    "INSUFFICIENT_FUNDS":     0.35,
    "BANK_DECLINED":          0.20,
    "TIMEOUT":                0.15,
    "INVALID_CREDENTIALS":    0.10,
    "GATEWAY_ERROR":          0.10,
    "DAILY_LIMIT_EXCEEDED":   0.05,
    "FRAUD_SUSPECTED":        0.05
}

# ─────────────────────────────────────────
# LOCATION CONFIGURATION

# LOCATION_CONFIG
# 11 Indian cities with weighted distribution. Mumbai and Delhi get the most transactions — realistic for India's 
# payment volumes.
# ─────────────────────────────────────────
LOCATION_CONFIG = [
    {"city": "Mumbai",    "state": "Maharashtra",  "weight": 0.20},
    {"city": "Delhi",     "state": "Delhi",         "weight": 0.18},
    {"city": "Bangalore", "state": "Karnataka",     "weight": 0.15},
    {"city": "Hyderabad", "state": "Telangana",     "weight": 0.10},
    {"city": "Chennai",   "state": "Tamil Nadu",    "weight": 0.08},
    {"city": "Kolkata",   "state": "West Bengal",   "weight": 0.07},
    {"city": "Pune",      "state": "Maharashtra",   "weight": 0.06},
    {"city": "Ahmedabad", "state": "Gujarat",       "weight": 0.05},
    {"city": "Jaipur",    "state": "Rajasthan",     "weight": 0.04},
    {"city": "Lucknow",   "state": "Uttar Pradesh", "weight": 0.03},
    {"city": "Other",     "state": "Other",         "weight": 0.04}
]

# ─────────────────────────────────────────
# TIME OF DAY PATTERNS

# HOURLY_WEIGHTS
# 24 weights — one per hour of the day. This makes our data realistic — very few transactions at 3 AM, peaks at 
# lunch and evening. Most toy projects use uniform random distribution — ours won't.

# ─────────────────────────────────────────
# Weight for each hour of the day (0-23)
# Higher weight = more transactions at that hour
HOURLY_WEIGHTS = [
    0.01, 0.01, 0.01, 0.01,  # 0-3 AM  (very low)
    0.01, 0.02, 0.03, 0.04,  # 4-7 AM  (early morning)
    0.05, 0.06, 0.06, 0.06,  # 8-11 AM (morning)
    0.07, 0.07, 0.06, 0.05,  # 12-3 PM (lunch peak)
    0.05, 0.05, 0.06, 0.07,  # 4-7 PM  (evening)
    0.08, 0.07, 0.05, 0.03   # 8-11 PM (night peak)
]