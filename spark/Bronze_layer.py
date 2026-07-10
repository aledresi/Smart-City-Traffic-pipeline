from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

SPARK_MASTER = "spark://spark-master:7077"
KAFKA_BOOTSTRAP = "kafka:9092"

def get_spark_session():
    return SparkSession.builder \
        .appName("bronze") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .master(SPARK_MASTER) \
        .config("spark.hadoop.fs.hdfs.impl.disable.cache", "true") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .config("spark.driver.extraJavaOptions", "-DHADOOP_USER_NAME=root") \
        .getOrCreate()

def ingest_incremental(spark, topic_name, hdfs_path, checkpoint_path):
    print(f"--- Starting incremental load for: {topic_name} ---")
    try:
        df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "latest") \
            .load()

        final_df = df.select(
            col("value").cast("string").alias("value"),
            current_timestamp().alias("ingested_at")
        )

        query = final_df.writeStream \
            .format("parquet") \
            .option("checkpointLocation", checkpoint_path) \
            .option("path", hdfs_path) \
            .trigger(once=True) \
            .start()

        query.awaitTermination()
        print(f"Successfully appended new data to {hdfs_path}")
    except Exception as e:
        print(f"Failed to process {topic_name}: {str(e)}")

if __name__ == "__main__":
    spark = get_spark_session()
    
    tasks = [
        ("telemetry_topic", "hdfs://namenode:9000/smart_city/bronze/telemetry", "hdfs://namenode:9000/checkpoints/telemetry"),
        ("incidents_topic", "hdfs://namenode:9000/smart_city/bronze/incidents", "hdfs://namenode:9000/checkpoints/incidents"),
        ("streets_topic", "hdfs://namenode:9000/smart_city/bronze/streets_reference", "hdfs://namenode:9000/checkpoints/streets")
    ]
    
    for topic, hdfs, chk in tasks:
        ingest_incremental(spark, topic, hdfs, chk)
        
    spark.stop()