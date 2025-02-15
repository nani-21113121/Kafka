from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime

# Kafka configuration
kafka_broker = 'localhost:9092'  # Replace with your Kafka broker address
kafka_topic = 'flight-data'

# Function to parse flight entries
def parse_flight_entry(flight_entry):
    parts = flight_entry.split(", ")
    timestamp = datetime.strptime(parts[0], "%Y-%m-%d %H:%M:%S")
    flight_status = parts[6]
    return timestamp, flight_status

# Analyze flight data
def analyze_flights(flights):
    flight_status_counts = defaultdict(int)
    flight_status_timestamps = defaultdict(list)

    for flight in flights:
        timestamp, flight_status = parse_flight_entry(flight)
        flight_status_counts[flight_status] += 1
        flight_status_timestamps[flight_status].append(timestamp)

    # Calculate time intervals between flight messages for each status
    flight_status_intervals = defaultdict(list)
    for status, timestamps in flight_status_timestamps.items():
        for i in range(1, len(timestamps)):
            interval = (timestamps[i] - timestamps[i - 1]).total_seconds()
            flight_status_intervals[status].append(interval)

    return flight_status_counts, flight_status_intervals

# Kafka consumer function
def consume_flights():
    # Create a Kafka consumer
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=[kafka_broker],
        auto_offset_reset='earliest',  # Start reading from the earliest message
        group_id='flight_analyzer_group',  # Consumer group ID
        value_deserializer=lambda x: x.decode('utf-8')  # Decode messages as UTF-8
    )

    flights = []
    try:
        for message in consumer:
            # Append the flight message to the list
            flight_message = message.value
            flights.append(flight_message)
            print("Consumed: {}".format(flight_message))

            # Analyze flights periodically (e.g., after every 10 messages)
            if len(flights) % 10 == 0:
                flight_status_counts, flight_status_intervals = analyze_flights(flights)
                print("\nFlight Status Counts:")
                for status, count in flight_status_counts.items():
                    print("{}: {}".format(status, count))  # Replaced f-string with .format
                print("\nAverage Time Intervals Between Flight Messages (in seconds):")
                for status, intervals in flight_status_intervals.items():
                    avg_interval = sum(intervals) / len(intervals) if intervals else 0
                    print("{}: {:.2f}".format(status, avg_interval))  # Replaced f-string with .format

    except KeyboardInterrupt:
        print("Consumption interrupted by user.")
    finally:
        consumer.close()

# Main function
if __name__ == "__main__":
    consume_flights()
