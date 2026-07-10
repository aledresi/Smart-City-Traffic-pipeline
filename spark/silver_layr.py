from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

def get_spark_session():
    return SparkSession.builder.appName("silver") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

telemetry_schema = StructType([
    StructField("telemetry_id", StringType(), True),
    StructField("street_id", StringType(), True),
    StructField("avg_speed_kph", DoubleType(), True),
    StructField("event_ts", TimestampType(), True)
])


def process_bronze_to_silver_stream(spark, topic_name, schema, hdfs_path):
    print(f"--- Starting Stream: {topic_name} ---")
    
    raw_df = spark.readStream.format("parquet") \
        .load(f"hdfs://namenode:9000/smart_city/bronze/{topic_name}")
    
    parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    
    if "event_ts" in parsed_df.columns:
        parsed_df = parsed_df.withColumn("year", year(col("event_ts"))) \
                             .withColumn("month", month(col("event_ts"))) \
                             .withColumn("day", dayofmonth(col("event_ts")))
    
    query = parsed_df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", f"hdfs://namenode:9000/checkpoints/silver_{topic_name}") \
        .option("path", f"hdfs://namenode:9000/smart_city/silver/{hdfs_path}") \
        .partitionBy("year", "month", "day") \
        .trigger(once=True) \
        .start()

    query.awaitTermination()
    print(f"Successfully processed {topic_name} to Silver.")

if __name__ == "__main__":
    spark = get_spark_session()
    
    process_bronze_to_silver_stream(spark, "telemetry", telemetry_schema, "telemetry")
    process_bronze_to_silver_stream(spark, "incidents", incidents_schema, "incidents")
    process_bronze_to_silver_stream(spark, "streets_reference", streets_schema, "streets_reference")
    
    spark.stop()