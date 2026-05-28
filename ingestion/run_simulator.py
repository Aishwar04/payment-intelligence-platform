import time
import signal
import logging
import sys
from ingestion.simulator.transaction_generator import TransactionGenerator
from ingestion.simulator.kafka_producer import PaymentKafkaProducer
from ingestion.simulator.config import SIMULATION_CONFIG

# ─────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# GRACEFUL SHUTDOWN HANDLER
# ─────────────────────────────────────────
class SimulatorShutdown:
    """
    Handles graceful shutdown when Ctrl+C is pressed.
    Without this, pressing Ctrl+C would kill the process
    immediately — potentially losing buffered messages
    that haven't been sent to Kafka yet.
    With this, we catch the signal, flush all buffered
    messages, close the producer cleanly, then exit.
    """
    def __init__(self):
        self.shutdown = False
        # register signal handlers for Ctrl+C and kill command
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logger.info("Shutdown signal received. Finishing up...")
        self.shutdown = True


# ─────────────────────────────────────────
# MAIN SIMULATOR LOOP
# ─────────────────────────────────────────
def run():
    """
    Main entry point for the transaction simulator.
    Generates and publishes transactions continuously
    at the configured rate until stopped.
    """
    logger.info("Starting Payment Transaction Simulator")
    logger.info(f"Target rate: {SIMULATION_CONFIG['transactions_per_second']} TPS")
    logger.info(f"Designed for: {SIMULATION_CONFIG['designed_tps']} TPS in production")
    logger.info(f"Late arrival probability: {SIMULATION_CONFIG['late_arrival_probability'] * 100}%")

    # initialise generator and producer
    generator = TransactionGenerator()
    producer = PaymentKafkaProducer()
    shutdown = SimulatorShutdown()

    # counters for logging
    total_published = 0
    total_failed = 0
    total_late_arrivals = 0
    batch_size = SIMULATION_CONFIG["batch_size"]
    tps = SIMULATION_CONFIG["transactions_per_second"]

    # sleep interval between batches
    # example: 100 TPS with batch_size 10
    # means 10 batches per second
    # so sleep 0.1 seconds between batches
    sleep_interval = batch_size / tps

    logger.info("Simulator running. Press Ctrl+C to stop.")

    try:
        while not shutdown.shutdown:
            batch_start = time.time()

            # generate and publish one batch
            for _ in range(batch_size):
                transaction = generator.generate()

                # track late arrivals for logging
                if transaction["txn_timestamp"] < transaction["arrival_timestamp"][:10]:
                    total_late_arrivals += 1

                # track success vs failure for logging
                if transaction["status"] == "FAILED":
                    total_failed += 1

                producer.publish(transaction)
                total_published += 1

            # flush buffer every batch
            producer.flush()

            # log progress every 1000 transactions
            if total_published % 1000 == 0:
                failure_rate = (total_failed / total_published) * 100
                late_rate = (total_late_arrivals / total_published) * 100
                logger.info(
                    f"Published: {total_published:,} | "
                    f"Failure rate: {failure_rate:.1f}% | "
                    f"Late arrivals: {late_rate:.1f}%"
                )

            # sleep to maintain target TPS
            elapsed = time.time() - batch_start
            sleep_time = max(0, sleep_interval - elapsed)
            time.sleep(sleep_time)

    except Exception as e:
        logger.error(f"Simulator error: {e}")
        raise

    finally:
        # always close producer cleanly
        # this runs whether we stopped normally or crashed
        logger.info(
            f"Simulator stopped. "
            f"Total published: {total_published:,} | "
            f"Total failed: {total_failed:,} | "
            f"Late arrivals: {total_late_arrivals:,}"
        )
        producer.close()


# ─────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────
if __name__ == "__main__":
    run()