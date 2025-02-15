from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Initialize SparkContext and StreamingContext
sc = SparkContext(appName="KafkaFlightConsumer")
ssc = StreamingContext(sc, 5)  # 5 seconds batch interval

# Create Direct Kafka Stream
kafkaStream = KafkaUtils.createDirectStream(ssc, ['flight-data'], {"metadata.broker.list": 'localhost:9092'})

# Extract data from Kafka Stream
kafkaRDD = kafkaStream.map(lambda message: message[1])

# Perform operations on RDD
# Example: Count occurrences of each flight status
flightStatusRDD = kafkaRDD.map(lambda flight: (flight.split(",")[6], 1))  # Assuming flight status is the 7th field
flightStatusCounts = flightStatusRDD.reduceByKey(lambda x, y: x + y)

# Print counts to console
flightStatusCounts.pprint()

# Start the streaming context and await termination
ssc.start()
ssc.awaitTermination()
