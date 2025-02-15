from kafka import KafkaProducer
from faker import Faker
import json
import random
import time
 
# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
 
# Initialize Faker
fake = Faker()
 
# Sample airlines and flight statuses
airlines = ['Southwest', 'JetBlue', 'United', 'American Airlines']
statuses = ['On Time', 'Delayed']
 
while True:
    flight_data = {
        "FlightNumber": f"DL{random.randint(1000, 9999)}",
        "Airline": random.choice(airlines),
        "OriginAirport": fake.airport_code(),
        "DestinationAirport": fake.airport_code(),
        "DepartureTime": fake.date_time_this_year().strftime('%m/%d/%Y %H:%M'),
        "ArrivalTime": fake.date_time_this_year().strftime('%m/%d/%Y %H:%M'),
        "FlightStatus": random.choice(statuses),
        "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    # Send flight data to Kafka topic
    producer.send('flight-data', flight_data)
    print(f'Produced: {flight_data}')
    time.sleep(1)