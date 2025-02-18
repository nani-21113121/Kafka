import json
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark import SparkContext

# Initialize Spark
spark = SparkSession.builder.appName("KafkaRDDAnalysis").getOrCreate()
sc = spark.sparkContext

# Kafka Config
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "flight-data"

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# Collect incoming flight data
flight_data_list = []

print("Consuming messages from Kafka...")

for message in consumer:
    flight_data_list.append(message.value)
    
    # Process data in RDD after receiving 10 messages
    if len(flight_data_list) >= 10:
        # Convert list to RDD
        flight_rdd = sc.parallelize(flight_data_list)

        # Extract Flight Status and Airline
        status_rdd = flight_rdd.map(lambda x: (x["FlightStatus"], 1))
        airline_rdd = flight_rdd.map(lambda x: (x["Airline"], 1))

        # Count occurrences using reduceByKey
        status_count = status_rdd.reduceByKey(lambda a, b: a + b).collect()
        airline_count = airline_rdd.reduceByKey(lambda a, b: a + b).collect()

        # Display analysis
        print("\nFlight Status Count (RDD):", status_count)
        print("\nMost Frequent Airlines (RDD):", airline_count)

        # Clear data for fresh batch processing
        flight_data_list = []
