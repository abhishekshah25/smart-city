from config import configuration
from pyspark.sql import SparkSession
from pyspark.sql.DataFrame import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType,StructField, StringType,TimestampType, IntegerType, DoubleType

def main():
  spark = SparkSession.builder.appName("SmartCityStreaming")\
  .config("spark.jars.packages", 
          "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0",
          "org.apache.hadoop:hadoop-aws:3.3.1",
          "com.amazonaws:aws-java-sdk:1.11.469")\
  .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
  .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
  .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
  .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
  .getOrCreate()        

  # Adjust log level to minimize the console output on executors
  spark.sparkContext.setLogLevel('WARN')

  vehicleSchema = StructType([
    StructField("id", StringType(), True),
    StructField("vehicleId", StringType(), True),
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
    StructField("vehicleId", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("speed", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("vehicleType", StringType(), True)
  ])  

  trafficSchema = StructType([
    StructField("id", StringType(), True),
    StructField("vehicleId", StringType(), True),
    StructField("cameraId", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("snapshot", StringType(), True)
  ]) 

  weatherSchema = StructType([
    StructField("id", StringType(), True),
    StructField("vehicleId", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("weatherCondition", StringType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("windSpeed", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("airQualityIndex", DoubleType(), True)
  ])

  emergencySchema = StructType([
    StructField("id", StringType(), True),
    StructField("vehicleId", StringType(), True),
    StructField("incidentId", StringType(), True),
    StructField("type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True),
    StructField("status", StringType(), True),
    StructField("description", StringType(), True)
  ])

  def read_kafka_topic(topic,schema):
    return (spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers','broker:29092')
            .option('subscribe', topic)
            .option('startingOffsets', 'earliest')
            .load()
            .selectExpr('CAST(value AS STRING)')
            .select(from_json(col('value'), schema).alias('data'))
            .select('data.*')
            .withWatermark('timestamp', '2 minutes'))
            
  def streamWriter(input: DataFrame,checkPointFolder, output):
    return (input.writeStream
            .format('parquet')
            .option('checkpointLocation', checkPointFolder)
            .option('path',output)
            .outputMode('append')
            .start()
            )

  vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
  gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
  trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
  weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
  emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

  # Join all the DataFrames with id and timestamps

  vehicle_query = streamWriter(vehicleDF,'s3a://smart-city-spark-stream-data/checkpoints/vehicle_data','s3a://smart-city-spark-stream-data/data/vehicle_data')
  gps_query = streamWriter(gpsDF,'s3a://smart-city-spark-stream-data/checkpoints/gps_data','s3a://smart-city-spark-stream-data/data/gps_data')
  traffic_query = streamWriter(trafficDF,'s3a://smart-city-spark-stream-data/checkpoints/traffic_data','s3a://smart-city-spark-stream-data/data/traffic_data')
  weather_query = streamWriter(weatherDF,'s3a://smart-city-spark-stream-data/checkpoints/weather_data','s3a://smart-city-spark-stream-data/data/weather_data')
  emergency_query = streamWriter(emergencyDF,'s3a://smart-city-spark-stream-data/checkpoints/emergency_data','s3a://smart-city-spark-stream-data/data/emergency_data')     

  emergency_query.awaitTermination()

if __name__ == "__main__":
  main()
