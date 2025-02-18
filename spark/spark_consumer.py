import json
import pandas as pd
from kafka import KafkaConsumer

# Kafka configurations
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "flight-data"

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# List to store incoming flight data
flight_data_list = []

print("Consuming messages from Kafka...")

for message in consumer:
    flight_data_list.append(message.value)  # Append message to list
    
    # Convert to Pandas DataFrame after receiving some data
    if len(flight_data_list) >= 10:  # Process after 10 messages
        df = pd.DataFrame(flight_data_list)
        
        # Display first few rows
        print("\nSample Data:\n", df.head())

        # Perform analysis
        print("\nFlight Status Counts:\n", df["FlightStatus"].value_counts())
        print("\nMost Frequent Airlines:\n", df["Airline"].value_counts())

        # Clear data for fresh batch processing
        flight_data_list = []
