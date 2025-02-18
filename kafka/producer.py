import random
import json
import time
import logging
from faker import Faker
from datetime import datetime
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize Faker
fake = Faker()

# Define constants
AIRLINES = ["Southwest", "JetBlue", "United", "American Airlines"]
STATUSES = ["On Time", "Delayed"]
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "flight-data"

# Define airport codes (since Faker doesn't have an airport_code function)
AIRPORT_CODES = ["JFK", "LAX", "ORD", "ATL", "DFW", "DEN", "SFO", "SEA", "MIA", "LAS"]

# Initialize Kafka producer
producer = None
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        acks=1,
    )
    logger.info("Kafka producer initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    exit(1)

def generate_flight_data():
    """Generate a realistic flight data entry."""
    flight_data = {
        "FlightNumber": "DL{}".format(random.randint(1000, 9999)),
        "Airline": random.choice(AIRLINES),
        "OriginAirport": random.choice(AIRPORT_CODES),
        "DestinationAirport": random.choice(AIRPORT_CODES),
        "DepartureTime": fake.date_time_this_year().strftime("%m/%d/%Y %H:%M"),
        "ArrivalTime": fake.date_time_this_year().strftime("%m/%d/%Y %H:%M"),
        "FlightStatus": random.choice(STATUSES),
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return flight_data

def stream_flight_data_to_kafka(topic, interval=1):
    """Stream generated flight data entries to Kafka."""
    if producer is None:
        logger.error("Kafka producer is not initialized.")
        return

    try:
        while True:
            flight_data = generate_flight_data()
            producer.send(topic, value=flight_data)
            logger.info(f"Produced: {flight_data}")
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Streaming stopped.")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

if __name__ == "__main__":
    stream_flight_data_to_kafka(KAFKA_TOPIC)
