from pyspark.sql import SparkSession, DataFrame
from config import configuration
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col

def main():
    spark = SparkSession.builder.appName("SmartCityStreaming")\
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469,"
                "org.scala-lang:scala-library:2.13.0")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))\
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()

    # spark.sparkContext.setLogLevel('WARN')
    spark.sparkContext.setLogLevel('INFO')
    

    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])

    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("snapshot", StringType(), True)
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("airQuality", DoubleType(), True)
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("type", StringType(), True),
        StructField("incident_id", StringType(), True),
        StructField("description", StringType(), True),
        StructField("status", IntegerType(), True)
    ])

    def read_kafka_topic(topic, schema):
        try:
            return (spark.readStream
                    .format('kafka')
                    .option('kafka.bootstrap.servers', 'broker:29092')
                    .option('subscribe', topic)
                    .option('startingOffsets', 'earliest')
                    .load()
                    .selectExpr('CAST(value AS STRING)')
                    .select(from_json(col('value'), schema).alias('data'))
                    .select('data.*')
                    .withWatermark('timestamp', '2 minutes')
                )
        except Exception as e:
            print("An error occurred while loading Kafka topic:", e)
            raise e

    # def test_read_kafka_topic():
    #     topic = 'vehicle_data'
    #     schema = StructType([
    #         StructField("id", StringType(), True),
    #         StructField("vehicle_id", StringType(), True),
    #         StructField("timestamp", TimestampType(), True),
    #         StructField("location", StringType(), True),
    #         StructField("speed", DoubleType(), True),
    #         StructField("direction", StringType(), True),
    #         StructField("make", StringType(), True),
    #         StructField("model", StringType(), True),
    #         StructField("year", IntegerType(), True),
    #         StructField("fuelType", StringType(), True),
    #     ])
    #     try:
    #         df = read_kafka_topic(topic, schema)
    #         df.show(truncate=False)
    #     except Exception as e:
    #         print("Test failed:", e)

    # # Call the test function
    # test_read_kafka_topic()


    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())

    vehicle_df = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gps_df = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    traffic_df = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weather_df = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergency_df = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    queries = [
        streamWriter(vehicle_df, 's3a://analyst-journey/checkpoints/vehicle_data', 's3a://analyst-journey/data/vehicle_data'),
        streamWriter(gps_df, 's3a://analyst-journey/checkpoints/gps_data', 's3a://analyst-journey/data/gps_data'),
        streamWriter(traffic_df, 's3a://analyst-journey/checkpoints/traffic_data', 's3a://analyst-journey/data/traffic_data'),
        streamWriter(weather_df, 's3a://analyst-journey/checkpoints/weather_data', 's3a://analyst-journey/data/weather_data'),
        streamWriter(emergency_df, 's3a://analyst-journey/checkpoints/emergency_data', 's3a://analyst-journey/data/emergency_data')
    ]

    for query in queries:
        query.awaitTermination()

if __name__ == '__main__':
    main()
