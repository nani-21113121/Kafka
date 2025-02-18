import json
import logging
from kafka import KafkaConsumer

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Define Kafka settings
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "flight-data"

# Initialize Kafka consumer
consumer = None
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    logger.info("Kafka consumer initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize Kafka consumer: {e}")
    exit(1)

def consume_flight_data():
    """Consume and process flight data from Kafka."""
    if consumer is None:
        logger.error("Kafka consumer is not initialized.")
        return

    try:
        for message in consumer:
            flight_data = message.value
            logger.info(f"Consumed: {flight_data}")
    except KeyboardInterrupt:
        logger.info("Consumer stopped.")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")

if __name__ == "__main__":
    consume_flight_data()
