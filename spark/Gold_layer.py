from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, date_trunc, max, lit, broadcast

def get_spark_session():
    return SparkSession.builder.appName("gold") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

def run_gold_transformation(spark):
    telemetry = spark.read.parquet("hdfs://namenode:9000/smart_city/silver/telemetry")
    incidents = spark.read.parquet("hdfs://namenode:9000/smart_city/silver/incidents")
    streets = spark.read.parquet("hdfs://namenode:9000/smart_city/silver/streets_reference")

    streets_bc = broadcast(streets)

    incident_summary = incidents.join(streets_bc, "street_id", "left") \
        .groupBy("street_name", date_trunc("hour", col("event_ts")).alias("hour_bucket")) \
        .agg(
            count("incident_id").alias("total_incidents"),
            avg("severity").alias("avg_severity"),
            max("severity").alias("max_severity")
        )

    traffic_summary = telemetry.join(streets_bc, "street_id", "left") \
        .groupBy("street_name", date_trunc("hour", col("event_ts")).alias("hour_bucket")) \
        .agg(
            avg("avg_speed_kph").alias("avg_speed"),
            count(lit(1)).alias("vehicle_count")
        )

    gold_df = traffic_summary.join(incident_summary, ["street_name", "hour_bucket"], "full_outer") \
        .fillna({"total_incidents": 0, "vehicle_count": 0, "avg_severity": 0, "avg_speed": 0}) \
        .withColumn("incident_impact_factor", 
                    when(col("total_incidents") > 0, (lit(100) - col("avg_speed"))).otherwise(lit(0))) \
        .withColumn("operational_risk_score", 
                    (col("total_incidents") * col("avg_severity") * 10) + (col("vehicle_count") * 0.1))

    gold_df.write.mode("overwrite").parquet("hdfs://namenode:9000/smart_city/gold/business_ready_metrics")
    print("Gold layer: Metrics saved successfully.")

if __name__ == "__main__":
    spark = get_spark_session()
    try:
        run_gold_transformation(spark)
    finally:
        spark.stop()