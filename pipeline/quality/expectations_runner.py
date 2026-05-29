import logging
import json
import boto3
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
import great_expectations as gx
from great_expectations.dataset import SparkDFDataset

from pipeline.quality.bronze_expectations import build_bronze_expectation_suite
from pipeline.bronze.bronze_config import S3_CONFIG

# ─────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class BronzeQualityRunner:
    """
    Runs data quality checks on bronze layer data.
    Reads latest bronze partition from S3,
    validates against expectations,
    writes validation report back to S3.
    """

    def __init__(self, spark: SparkSession, logical_date: str):
        """
        logical_date: the date we are validating
        format: YYYY-MM-DD
        example: 2026-05-28
        """
        self.spark = spark
        self.logical_date = logical_date
        self.s3_client = boto3.client(
            "s3",
            region_name=S3_CONFIG["region"]
        )
        self.suite = build_bronze_expectation_suite()

    # ─────────────────────────────────────────
    # READ BRONZE DATA
    # ─────────────────────────────────────────
    def _read_bronze_partition(self) -> DataFrame:
        """
        Reads bronze data for the logical date.
        Parses logical_date into year/month/day
        to build the correct S3 partition path.
        """
        date = datetime.strptime(self.logical_date, "%Y-%m-%d")

        partition_path = (
            f"s3a://{S3_CONFIG['bronze_bucket']}/"
            f"{S3_CONFIG['bronze_prefix']}/"
            f"arrival_year={date.year}/"
            f"arrival_month={date.month}/"
            f"arrival_day={date.day}/"
        )

        logger.info(f"Reading bronze partition: {partition_path}")

        df = self.spark.read.parquet(partition_path)

        logger.info(f"Read {df.count():,} records from bronze")

        return df

    # ─────────────────────────────────────────
    # RUN VALIDATIONS
    # ─────────────────────────────────────────
    def _run_validations(self, df: DataFrame) -> dict:
        """
        Runs all expectations against the DataFrame.
        Returns validation results as a dictionary.
        """
        logger.info("Running data quality validations...")

        # wrap PySpark DataFrame with Great Expectations
        ge_df = SparkDFDataset(df)

        # run all expectations in the suite
        results = ge_df.validate(
            expectation_suite=self.suite,
            result_format="SUMMARY"
        )

        return results

    # ─────────────────────────────────────────
    # WRITE VALIDATION REPORT
    # ─────────────────────────────────────────
    def _write_validation_report(self, results: dict):
        """
        Writes validation report to S3.
        Report is a JSON file with full details
        of which expectations passed and failed.
        """
        date = datetime.strptime(self.logical_date, "%Y-%m-%d")

        report_key = (
            f"quality_reports/bronze/"
            f"year={date.year}/month={date.month}/day={date.day}/"
            f"validation_report.json"
        )

        report = {
            "logical_date": self.logical_date,
            "validated_at": datetime.now(timezone.utc).isoformat(),
            "success": results["success"],
            "statistics": results["statistics"],
            "failed_expectations": [
                {
                    "expectation_type": r["expectation_config"]["expectation_type"],
                    "kwargs": r["expectation_config"]["kwargs"],
                    "result": r["result"]
                }
                for r in results["results"]
                if not r["success"]
            ]
        }

        self.s3_client.put_object(
            Bucket=S3_CONFIG["bronze_bucket"],
            Key=report_key,
            Body=json.dumps(report, indent=2),
            ContentType="application/json"
        )

        logger.info(
            f"Validation report written to "
            f"s3://{S3_CONFIG['bronze_bucket']}/{report_key}"
        )

    # ─────────────────────────────────────────
    # WRITE SUCCESS MARKER
    # ─────────────────────────────────────────
    def _write_success_marker(self):
        """
        Writes a _SUCCESS marker file to S3.
        Downstream Glue jobs check for this file
        before processing. If it doesn't exist,
        they know data quality failed and they
        should not proceed.
        This is the same _SUCCESS pattern used
        by Hadoop and Spark natively.
        """
        date = datetime.strptime(self.logical_date, "%Y-%m-%d")

        marker_key = (
            f"{S3_CONFIG['bronze_prefix']}/"
            f"arrival_year={date.year}/"
            f"arrival_month={date.month}/"
            f"arrival_day={date.day}/"
            f"_SUCCESS"
        )

        self.s3_client.put_object(
            Bucket=S3_CONFIG["bronze_bucket"],
            Key=marker_key,
            Body=b"",
            ContentType="text/plain"
        )

        logger.info(
            f"SUCCESS marker written to "
            f"s3://{S3_CONFIG['bronze_bucket']}/{marker_key}"
        )

    # ─────────────────────────────────────────
    # MAIN RUN METHOD
    # ─────────────────────────────────────────
    def run(self) -> bool:
        """
        Main entry point.
        Returns True if all validations pass.
        Returns False if any validation fails.
        """
        logger.info(
            f"Starting bronze quality check "
            f"for logical_date: {self.logical_date}"
        )

        try:
            # read bronze data for this date
            df = self._read_bronze_partition()

            # run validations
            results = self._run_validations(df)

            # always write the report — pass or fail
            self._write_validation_report(results)

            if results["success"]:
                logger.info(
                    f"All validations PASSED for {self.logical_date}"
                )
                # write success marker — downstream jobs can proceed
                self._write_success_marker()
                return True
            else:
                failed = results["statistics"]["unsuccessful_expectations"]
                logger.error(
                    f"Validations FAILED for {self.logical_date} — "
                    f"{failed} expectations failed. "
                    f"Check validation report in S3."
                )
                # no success marker — downstream jobs will not proceed
                return False

        except Exception as e:
            logger.error(
                f"Quality check failed with error: {e}"
            )
            raise