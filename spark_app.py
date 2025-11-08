from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, when, desc, avg

def create_spark():
    spark = SparkSession.builder \
        .appName("LogAnalysisApp") \
        .getOrCreate()
    return spark

def parse_logs(spark, log_path):
    pattern = r'(\S+) (\S+) (\S+) \[(.*?)\] "(.*?)" (\d{3}) (\S+)'
    logs_df = spark.read.text(log_path)
    parsed = logs_df.select(
        regexp_extract("value", pattern, 1).alias("ip"),
        regexp_extract("value", pattern, 4).alias("timestamp"),
        regexp_extract("value", pattern, 5).alias("request"),
        regexp_extract("value", pattern, 6).cast("integer").alias("status"),
        regexp_extract("value", pattern, 7).alias("size"),
    ).withColumn(
        "size",
        when(col("size") == "-", 0).otherwise(col("size").cast("integer"))
    )
    return parsed

def summarize(parsed):
    status_counts = parsed.groupBy("status").count().orderBy(desc("count"))
    ip_counts = parsed.groupBy("ip").count().orderBy(desc("count"))
    avg_sizes = parsed.groupBy("status").agg(avg("size").alias("avg_response_size"))
    return {
        "status_counts": status_counts.toPandas(),
        "ip_counts": ip_counts.toPandas(),
        "avg_sizes": avg_sizes.toPandas()
    }
