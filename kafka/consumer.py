from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
 
# Initialize Spark Session
spark = SparkSession.builder.appName("KafkaSparkConsumer").getOrCreate()
 
# Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flight-data") \
    .load()
 
# Convert Kafka value from bytes to String
flight_df = df.selectExpr("CAST(value AS STRING) as flight_data")
 
# Parse JSON flight data
flight_df = flight_df.selectExpr("json_tuple(flight_data, 'FlightNumber', 'Airline', 'OriginAirport', 'DestinationAirport', 'DepartureTime', 'ArrivalTime', 'FlightStatus', 'Timestamp') as (FlightNumber, Airline, OriginAirport, DestinationAirport, DepartureTime, ArrivalTime, FlightStatus, Timestamp)")
 
# Print structured flight data to console
query = flight_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
 
query.awaitTermination()