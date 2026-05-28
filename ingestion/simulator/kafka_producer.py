import json
import logging
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from ingestion.simulator.config import KAFKA_CONFIG

# ─────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class PaymentKafkaProducer:
    """
    Handles all Kafka operations for the
    payment transaction simulator.
    Responsible for:
    - Creating the Kafka topic if it doesn't exist
    - Serialising transaction events to JSON
    - Publishing events to the correct topic
    - Handling publish errors gracefully
    """

    # When the producer is created it does two things — creates a connection to Kafka and creates the topic if it doesn't exist yet. 
    # Both happen automatically.

    def __init__(self):
        self.topic = KAFKA_CONFIG["topic"]
        self.producer = self._create_producer()
        self._create_topic_if_not_exists()

    # ─────────────────────────────────────────
    # CREATE PRODUCER

    # Creates the actual Kafka connection with retry logic. Why retry? Because when we run docker compose up -d, Kafka takes a few seconds to fully start. 
    # If the simulator starts before Kafka is ready, without retry logic it would just crash. With retry it waits and tries again up to 5 times with 5 second gaps.
    # ─────────────────────────────────────────
    def _create_producer(self) -> KafkaProducer:
        """
        Creates and returns a KafkaProducer instance.
        Retries up to 5 times if Kafka is not ready yet.
        This handles the case where Kafka container is
        still starting up when the simulator runs.
        """
        retries = 5
        for attempt in range(retries):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],

                    # serialise every message as JSON bytes
                    # Kafka only understands bytes — not Python dicts
                    # This converts every transaction dict to JSON bytes automatically before sending
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),

                    # use txn_id as the key for partitioning
                    # ensures related transactions go to same partition
                    # every Kafka message has a key and a value. 
                    # We use txn_id as the key. Kafka uses this key to decide which partition to send the message to — same key always goes to same partition.
                    key_serializer=lambda k: k.encode("utf-8") if k else None,

                    # wait for all replicas to acknowledge
                    # acks=1 means only leader needs to acknowledge
                    # fine for local dev — use acks='all' in production
                    acks=1,

                    # retry failed sends up to 3 times
                    retries=3,

                    # batch messages for efficiency
                    # sends in batches of 16KB
                    batch_size=16384,

                    # wait up to 10ms to fill a batch
                    # balances latency vs throughput
                    linger_ms=10
                )
                logger.info("Kafka producer created successfully")
                return producer

            except Exception as e:
                logger.warning(
                    f"Attempt {attempt + 1}/{retries} — "
                    f"Kafka not ready yet: {e}"
                )
                time.sleep(5)

        raise RuntimeError("Could not connect to Kafka after 5 attempts")

    # ─────────────────────────────────────────
    # CREATE TOPIC
    # ─────────────────────────────────────────
    def _create_topic_if_not_exists(self):
        """
        Creates the Kafka topic if it doesn't exist.
        Safe to call multiple times — won't fail if
        topic already exists.
        """
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"]
            )
            topic = NewTopic(
                name=KAFKA_CONFIG["topic"],
                num_partitions=KAFKA_CONFIG["num_partitions"],
                replication_factor=KAFKA_CONFIG["replication_factor"]
            )
            admin.create_topics([topic])
            logger.info(f"Topic '{self.topic}' created successfully")
            admin.close()

        except TopicAlreadyExistsError:
            logger.info(f"Topic '{self.topic}' already exists — skipping creation")

        except Exception as e:
            logger.error(f"Error creating topic: {e}")
            raise

    # ─────────────────────────────────────────
    # PUBLISH TRANSACTION
    # ─────────────────────────────────────────
    def publish(self, transaction: dict):
        """
        Publishes one transaction event to Kafka.
        Uses txn_id as the message key so all events
        for the same transaction go to the same partition.
        This ensures ordering is preserved per transaction.
        """
        try:
            self.producer.send(
                topic=self.topic,
                key=transaction["txn_id"],
                value=transaction
            )

        except Exception as e:
            logger.error(
                f"Failed to publish transaction "
                f"{transaction.get('txn_id', 'unknown')}: {e}"
            )

    # ─────────────────────────────────────────
    # FLUSH
    # ─────────────────────────────────────────
    def flush(self):
        """
        Forces all buffered messages to be sent immediately.
        Called periodically to ensure messages don't
        sit in the buffer for too long.
        """
        self.producer.flush()

    # ─────────────────────────────────────────
    # CLOSE
    # ─────────────────────────────────────────
    def close(self):
        """
        Cleanly shuts down the producer.
        Always call this when stopping the simulator
        to ensure all buffered messages are sent.
        """
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka producer closed")