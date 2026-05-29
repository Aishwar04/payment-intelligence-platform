from great_expectations.core import ExpectationSuite, ExpectationConfiguration


def build_bronze_expectation_suite() -> ExpectationSuite:
    """
    Defines all data quality rules for the bronze layer.
    These rules are our data contract — if upstream data
    violates these, we catch it before it corrupts
    downstream silver and gold layers.
    """
    suite = ExpectationSuite(expectation_suite_name="bronze_transactions")

    # ─────────────────────────────────────────
    # txn_id CHECKS
    # ─────────────────────────────────────────

    # txn_id must never be null
    # if txn_id is null we cannot deduplicate in silver
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "txn_id"}
    ))

    # txn_id must be unique
    # duplicate txn_ids would cause incorrect MERGE in silver
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "txn_id"}
    ))

    # txn_id must match our format TXN-XXXXXXXXXXXX
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_match_regex",
        kwargs={
            "column": "txn_id",
            "regex": r"^TXN-[A-Z0-9]{12}$"
        }
    ))

    # ─────────────────────────────────────────
    # AMOUNT CHECKS
    # ─────────────────────────────────────────

    # amount must never be null
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "amount"}
    ))

    # amount must always be positive
    # negative amounts would corrupt ML features
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column": "amount",
            "min_value": 1.0,
            "max_value": 1000000.0
        }
    ))

    # ─────────────────────────────────────────
    # STATUS CHECKS
    # ─────────────────────────────────────────

    # status must be one of SUCCESS or FAILED
    # any other value is a data quality issue
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "status",
            "value_set": ["SUCCESS", "FAILED"]
        }
    ))

    # status must never be null
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "status"}
    ))

    # ─────────────────────────────────────────
    # USER CHECKS
    # ─────────────────────────────────────────

    # user_id must never be null
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "user_id"}
    ))

    # user_id must match format USR-XXXXXX
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_match_regex",
        kwargs={
            "column": "user_id",
            "regex": r"^USR-\d{6}$"
        }
    ))

    # user_segment must be one of known segments
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "user_segment",
            "value_set": ["premium", "regular", "new", "dormant"]
        }
    ))

    # ─────────────────────────────────────────
    # MERCHANT CHECKS
    # ─────────────────────────────────────────

    # merchant_id must never be null
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "merchant_id"}
    ))

    # merchant_id must match format MRC-XXXXXX
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_match_regex",
        kwargs={
            "column": "merchant_id",
            "regex": r"^MRC-\d{6}$"
        }
    ))

    # merchant_category must be one of known categories
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "merchant_category",
            "value_set": [
                "food_delivery", "ecommerce", "utilities",
                "entertainment", "travel", "grocery",
                "healthcare", "fuel"
            ]
        }
    ))

    # ─────────────────────────────────────────
    # PAYMENT METHOD CHECKS
    # ─────────────────────────────────────────

    # payment_method must be one of known methods
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "payment_method",
            "value_set": ["UPI", "CARD", "NETBANKING", "WALLET"]
        }
    ))

    # ─────────────────────────────────────────
    # TIMESTAMP CHECKS
    # ─────────────────────────────────────────

    # txn_timestamp must never be null
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "txn_timestamp"}
    ))

    # arrival_timestamp must never be null
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "arrival_timestamp"}
    ))

    # ─────────────────────────────────────────
    # GATEWAY CHECKS
    # ─────────────────────────────────────────

    # gateway_id must never be null
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "gateway_id"}
    ))

    # gateway_id must be one of known gateways
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "gateway_id",
            "value_set": ["GW-001", "GW-002", "GW-003"]
        }
    ))

    # ─────────────────────────────────────────
    # SCHEMA CHECKS
    # ─────────────────────────────────────────

    # all required columns must exist
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_table_columns_to_match_ordered_list",
        kwargs={
            "column_list": [
                "txn_id", "txn_timestamp", "arrival_timestamp",
                "amount", "currency", "status", "failure_reason",
                "user_id", "user_segment", "merchant_id",
                "merchant_category", "gateway_id", "gateway_name",
                "processor_id", "processor_name", "payment_method",
                "device_type", "location_city", "location_state",
                "is_international", "arrival_year", "arrival_month",
                "arrival_day", "arrival_hour", "pipeline_timestamp"
            ],
            "exact_match": False  # allow extra columns
        }
    ))

    return suite