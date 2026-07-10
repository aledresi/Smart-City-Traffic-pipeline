from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    DecimalType, FloatType, IntegerType, StringType, StructField, StructType,
)
import traceback

class ClickHouseClient:
    _instance = None
    @classmethod
    def get_client(cls):
        if cls._instance is None:
            import clickhouse_connect
            cls._instance = clickhouse_connect.get_client(
                host="clickhouse", port=8123, username="default", 
                password="default", database="smart_city"
            )
        return cls._instance

def get_spark_session():
    return (
        SparkSession.builder
        .appName("streaming_ingestion_service")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

telemetry_schema = StructType([
    StructField("telemetry_id", StringType(), True),
    StructField("street_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("avg_speed_kph", FloatType(), True),
    StructField("delay_minutes", FloatType(), True),
    StructField("congestion_level", StringType(), True),
])

incident_schema = StructType([
    StructField("incident_id", StringType(), True),
    StructField("street_id", StringType(), True),
    StructField("incident_type", StringType(), True),
    StructField("severity", IntegerType(), True),
    StructField("latitude", DecimalType(9, 6), True),
    StructField("longitude", DecimalType(9, 6), True),
    StructField("status", StringType(), True),
    StructField("description", StringType(), True),
    StructField("created_at", StringType(), True),
])

streets_schema = StructType([
    StructField("street_id", StringType(), True),
    StructField("street_name", StringType(), True),
    StructField("max_speed_limit", IntegerType(), True),
    StructField("geometry_json", StringType(), True),
    StructField("zone_id", StringType(), True),
    StructField("timestamp", StringType(), True),
])

def write_to_clickhouse(batch_df, batch_id, table_name):
    pdf = batch_df.toPandas()
    if pdf.empty: return
    try:
        client = ClickHouseClient.get_client()
        client.insert_df(table_name, pdf)
    except Exception as e:
        print(f"Batch {batch_id} failed: {e}")
        raise e # Re-raise to trigger Spark retry mechanism

def get_kafka_stream(spark, topic, schema):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Streams
    telemetry_final = get_kafka_stream(spark, "telemetry_topic", telemetry_schema).select(
        col("telemetry_id"), col("street_id"), to_timestamp(col("timestamp")).alias("ts"),
        col("vehicle_count"), col("avg_speed_kph"), col("delay_minutes"), col("congestion_level")
    )
    
    incident_final = get_kafka_stream(spark, "incidents_topic", incident_schema).select(
        col("incident_id"), col("street_id"), col("incident_type"), col("severity"),
        col("latitude").cast("double"), col("longitude").cast("double"), col("status"),
        col("description"), to_timestamp(col("created_at")).alias("created_at")
    )
    
    streets_final = get_kafka_stream(spark, "streets_topic", streets_schema).select(
        col("street_id"), col("street_name"), col("max_speed_limit"), 
        col("geometry_json"), col("zone_id"), to_timestamp(col("timestamp")).alias("ingested_at")
    )

    # Start Queries
    queries = [
        telemetry_final.writeStream.foreachBatch(lambda df, eid: write_to_clickhouse(df, eid, "smart_city.street_telemetry")).option("checkpointLocation", "/tmp/checkpoints/telemetry").start(),
        incident_final.writeStream.foreachBatch(lambda df, eid: write_to_clickhouse(df, eid, "smart_city.traffic_incidents")).option("checkpointLocation", "/tmp/checkpoints/incidents").start(),
        streets_final.writeStream.foreachBatch(lambda df, eid: write_to_clickhouse(df, eid, "smart_city.streets")).option("checkpointLocation", "/tmp/checkpoints/streets").start()
    ]

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        for q in queries: q.stop()