from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)
from pyspark.sql import SparkSession
from conf.configs import kafka_config
from kafka_admin import kafka_topics
import os
import uuid

CSV_DIR = "data"
CSV_FILE="alerts_conditions.csv"
KEY = str(uuid.uuid4())

# package necesserary for the Kafka reading from Spark Streaming
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# define Kafka topics
topic_building_sensors = kafka_topics["topic_building_sensors"]
topic_kafka_alerts = kafka_topics["topic_kafka_alerts"]

# create SparkSession
spark = (
    SparkSession.builder.appName("SparkStreamingFromKafka")
    .master("local[*]")
    .getOrCreate()
)

# ------------------------------------------
# Step 1. CSV file data reading and processing 
# ------------------------------------------

# define JSON schema for cvs file data reading
csv_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("humidity_min", DoubleType(), True),
    StructField("humidity_max", DoubleType(), True),
    StructField("temperature_min", DoubleType(), True),
    StructField("temperature_max", DoubleType(), True),
    StructField("code", IntegerType(), True),
    StructField("message", StringType(), True)
])

# read data from csv file
csvDF = spark.read \
    .option("sep", ",") \
    .option("header", "true") \
    .schema (csv_schema) \
    .csv(f'{CSV_DIR}/{CSV_FILE}') \

# visualise data from csv file
csvDF.show()   
    
# --------------------------------------------------------------------------
# Step 2. Kafka data reading and processing in Spark Streaming  
# --------------------------------------------------------------------------

# define configurations for Kafka sensor data reading
sensor_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", kafka_config["jaas_config"])
    .option("subscribe", topic_building_sensors)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "10")
    .load()
)


# define JSON schema for sensor data reading
json_schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("obtained_at",  DoubleType(), True),
    ]
)

# data manipulations
clean_df = (
    sensor_df.selectExpr(
        "CAST(key AS STRING) AS key_deserialized",
        "CAST(value AS STRING) AS value_deserialized",
        "*",
    )
    .drop("key", "value")
    .withColumnRenamed("key_deserialized", "key")
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema))
    .withColumn("temperature", col("value_json.temperature"))
    .withColumn("humidity", col("value_json.humidity"))
    .withColumn("obtained_at", from_unixtime(col("value_json.obtained_at")).cast("timestamp"))
    .drop("value_json", "value_deserialized")
)

processed_df = (clean_df
    .withWatermark("obtained_at", "10 seconds")
    .groupBy( window("obtained_at", "1 minutes", "30 seconds"))
    .agg(round(avg("temperature"),2).alias("avg_temperature"),
        round(avg("humidity"),2).alias("avg_humidity"))
    .crossJoin(csvDF)
    .filter(((col("avg_temperature")>col("temperature_min")) & (col("avg_temperature")<col("temperature_max"))) | ((col("avg_humidity")>col("humidity_min")) & (col("avg_humidity")<col("humidity_max"))))
    .drop("humidity_min", "humidity_max", "temperature_min", "temperature_max","id")
    .withColumn("timestamp", current_timestamp())
    )

# print data to the console
displaying_df = (
    processed_df.writeStream.trigger(availableNow=True)
    .outputMode("append")
    .format("console")
    .options(truncate=False)
    .option("checkpointLocation", "/tmp/checkpoints-spark-hw-06")
    .start()
)

displaying_df.awaitTermination()


# ----------------------------------------------------------------
# Step 3. Write preprocessed data from Spark Streaming to Kafka 
# ----------------------------------------------------------------

# prepare data for writing to Kafka
prepare_to_kafka_df = processed_df.select(to_json(struct(col("window"),col("avg_temperature"), col("avg_humidity"),col("code"),col("message"),col("timestamp"))).alias("value")).withColumn(
    "key", lit(KEY))


query = (
    prepare_to_kafka_df.writeStream.trigger(processingTime="5 seconds")
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("kafka.security.protocol", kafka_config["security_protocol"])
    .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
    .option("kafka.sasl.jaas.config", kafka_config["jaas_config"])
    .option("topic", topic_kafka_alerts)
    .option("checkpointLocation", "/tmp/checkpoints-output-hw06")
    .start()
    .awaitTermination()
)
