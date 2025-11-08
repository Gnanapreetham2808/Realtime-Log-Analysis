"""Versatile log analysis runner.

This script supports two modes:
 - Spark streaming mode (requires pyspark/findspark and Spark installed).
 - Batch fallback mode (default) that runs on plain Python using pandas.

Set the environment variable USE_SPARK=1 or run with --use-spark to enable Spark
mode. If Spark isn't available the script will automatically fall back to batch mode.
"""

from __future__ import annotations

import argparse
import collections
import os
import re
import shutil
import time
import logging
from typing import Dict, Tuple

import matplotlib
matplotlib.use("Agg")  # safe headless backend for servers; plots saved to files
import matplotlib.pyplot as plt


# Try optional imports for Spark
_pyspark_available = True
try:
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import regexp_extract, col, when, avg, desc
except Exception:
    _pyspark_available = False


def start_spark(app_name: str = "RealTimeLogAnalysis"):
    """Create or get a Spark session. Requires pyspark to be installed."""
    if not _pyspark_available:
        raise RuntimeError("pyspark/findspark not available in this environment")
    return SparkSession.builder.appName(app_name).getOrCreate()


def prepare_stream_files(input_file: str, stream_folder: str, chunk_size: int = 500, delay: float = 0.0) -> None:
    os.makedirs(stream_folder, exist_ok=True)
    with open(input_file, "r", encoding="utf-8", errors="ignore") as infile:
        lines = infile.readlines()
    for i in range(0, len(lines), chunk_size):
        chunk = lines[i : i + chunk_size]
        file_path = os.path.join(stream_folder, f"log_part_{i // chunk_size}.log")
        with open(file_path, "w", encoding="utf-8") as outfile:
            outfile.writelines(chunk)
        if delay:
            time.sleep(delay)


def create_parsed_stream(spark, stream_folder: str):
    logs_stream = spark.readStream.text(stream_folder)
    pattern = r"(\S+) (\S+) (\S+) \[(.*?)\] \"(.*?)\" (\d{3}) (\S+)"
    logs_parsed = logs_stream.select(
        regexp_extract("value", pattern, 1).alias("ip"),
        regexp_extract("value", pattern, 2).alias("client"),
        regexp_extract("value", pattern, 3).alias("user"),
        regexp_extract("value", pattern, 4).alias("timestamp"),
        regexp_extract("value", pattern, 5).alias("request"),
        regexp_extract("value", pattern, 6).cast("integer").alias("status"),
        regexp_extract("value", pattern, 7).alias("size"),
    )
    logs_parsed = logs_parsed.withColumn(
        "size", when(col("size") == "-", 0).otherwise(col("size").cast("integer"))
    )
    return logs_parsed


def run_spark_mode(input_file: str, stream_folder: str, run_time: int = 30):
    if not _pyspark_available:
        raise RuntimeError("Spark mode requested but pyspark/findspark not available")
    # ensure a clean stream folder
    if os.path.exists(stream_folder):
        shutil.rmtree(stream_folder)
    os.makedirs(stream_folder, exist_ok=True)

    spark = start_spark()
    # prepare initial chunks (no delay)
    if os.path.exists(input_file):
        prepare_stream_files(input_file, stream_folder, chunk_size=500, delay=0)
    else:
        logging.warning("Input file not found: %s", input_file)

    logs_parsed = create_parsed_stream(spark, stream_folder)
    status_counts = logs_parsed.groupBy("status").count()
    query = status_counts.writeStream.outputMode("complete").format("console").start()
    try:
        query.awaitTermination(run_time)
    finally:
        query.stop()


def batch_process_log(input_file: str, top_n: int = 10) -> Dict[str, any]:
    """Process the log file in plain Python (no Spark). Returns a dict of results.

    The function computes status counts, top IPs and top requests and saves a bar
    plot of status counts to `status_counts.png` in the current folder.
    """
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")

    pattern = re.compile(r"(\S+) (\S+) (\S+) \[(.*?)\] \"(.*?)\" (\d{3}) (\S+)")
    status_counter = collections.Counter()
    ip_counter = collections.Counter()
    request_counter = collections.Counter()

    with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = pattern.match(line)
            if not m:
                continue
            ip = m.group(1)
            request = m.group(5)
            status = m.group(6)
            size = m.group(7)
            status_counter[status] += 1
            ip_counter[ip] += 1
            request_counter[request] += 1

    # Convert to lists and sort
    status_items = sorted(status_counter.items(), key=lambda x: int(x[0]) if x[0].isdigit() else x[0])
    status_codes = [s for s, _ in status_items]
    counts = [c for _, c in status_items]

    # Plot status counts
    if counts:
        plt.figure(figsize=(8, 5))
        plt.bar(status_codes, counts, color="tab:blue")
        plt.xlabel("HTTP Status Code")
        plt.ylabel("Request Count")
        plt.title("HTTP Status Code Distribution")
        plt.tight_layout()
        out_png = "status_counts.png"
        plt.savefig(out_png)
        plt.close()
    else:
        out_png = None

    results = {
        "status_counts": status_counter,
        "top_ips": ip_counter.most_common(top_n),
        "top_requests": request_counter.most_common(top_n),
        "plot_file": out_png,
    }
    return results


def main(argv: list | None = None):
    parser = argparse.ArgumentParser(description="Run real-time or batch log analysis")
    parser.add_argument("--input", "-i", default=os.environ.get("LOG_INPUT_FILE", "logdata/web-server-access-logs_10k.log"), help="Input log file path")
    parser.add_argument("--stream-folder", "-s", default=os.environ.get("LOG_STREAM_FOLDER", "logdata_stream"), help="Folder to write stream chunks to")
    parser.add_argument("--use-spark", action="store_true", help="Attempt to use Spark streaming mode")
    parser.add_argument("--run-time", type=int, default=30, help="Seconds to run streaming query (Spark mode)")
    args = parser.parse_args(argv)

    use_spark = args.use_spark and _pyspark_available
    if args.use_spark and not _pyspark_available:
        logging.warning("Spark requested via --use-spark but pyspark/findspark unavailable; falling back to batch mode")

    if use_spark:
        run_spark_mode(args.input, args.stream_folder, run_time=args.run_time)
    else:
        print("Running batch mode (no Spark required)")
        results = batch_process_log(args.input)
        print("Status counts:")
        for k, v in sorted(results["status_counts"].items()):
            print(f"  {k}: {v}")
        print("Top IPs:")
        for ip, count in results["top_ips"]:
            print(f"  {ip}: {count}")
        print("Top requests:")
        for req, count in results["top_requests"]:
            print(f"  {req}: {count}")
        if results["plot_file"]:
            print(f"Saved status plot to: {results['plot_file']}")


if __name__ == "__main__":
    main()
# !apt-get install openjdk-11-jdk-headless -qq > /dev/null


## !wget -O spark-3.3.2-bin-hadoop3.tgz https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz



## !tar -xzf spark-3.3.2-bin-hadoop3.tgz


## !pip install -q findspark


import os
import findspark


os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.2-bin-hadoop3"


findspark.init()


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()

print("Spark session started successfully!")


from google.colab import files
uploaded = files.upload()


import zipfile
import os

zip_path = r"/content/archive (1).zip"
extract_path = "/content/logdata"

with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

for root, dirs, files in os.walk(extract_path):
    for file in files:
        print(os.path.join(root, file))


## !wget https://downloads.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz


## !tar xf spark-3.5.5-bin-hadoop3.tgz



## !apt-get install openjdk-8-jdk-headless -qq > /dev/null


## !pip install -q findspark


import os


os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.5.5-bin-hadoop3"

import findspark
findspark.init()


from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("RealTimeLogAnalysis") \
    .getOrCreate()


import os

input_file = "/content/logdata/web-server-access-logs_10k.log"
stream_folder = "/content/logdata_stream"
os.makedirs(stream_folder, exist_ok=True)

chunk_size = 500

with open(input_file, "r") as infile:
    lines = infile.readlines()

for i in range(0, len(lines), chunk_size):
    chunk = lines[i:i+chunk_size]
    with open(f"{stream_folder}/log_part_{i//chunk_size}.log", "w") as outfile:
        outfile.writelines(chunk)

from pyspark.sql.functions import regexp_extract, col, when


logs_stream = spark.readStream.text(stream_folder)


pattern = r'(\S+) (\S+) (\S+) \[(.*?)\] "(.*?)" (\d{3}) (\S+)'


logs_parsed = logs_stream.select(
    regexp_extract("value", pattern, 1).alias("ip"),
    regexp_extract("value", pattern, 2).alias("client"),
    regexp_extract("value", pattern, 3).alias("user"),
    regexp_extract("value", pattern, 4).alias("timestamp"),
    regexp_extract("value", pattern, 5).alias("request"),
    regexp_extract("value", pattern, 6).cast("integer").alias("status"),
    regexp_extract("value", pattern, 7).alias("size")
)


logs_parsed = logs_parsed.withColumn(
    "size",
    when(col("size") == "-", 0).otherwise(col("size").cast("integer"))
)


status_counts = logs_parsed.groupBy("status").count()


query = status_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination(30)
query.stop()


import shutil
shutil.rmtree("/content/logdata_stream")
os.makedirs("/content/logdata_stream", exist_ok=True)


query = status_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()


import time
import shutil

input_file = "/content/logdata/web-server-access-logs_10k.log"
stream_folder = "/content/logdata_stream"
chunk_size = 500

with open(input_file, "r") as infile:
    lines = infile.readlines()

for i in range(0, len(lines), chunk_size):
    chunk = lines[i:i+chunk_size]
    file_path = f"{stream_folder}/log_part_{i//chunk_size}.log"
    with open(file_path, "w") as outfile:
        outfile.writelines(chunk)
    print(f"Added {file_path}")
    time.sleep(5)

query.stop()


print(query.status)
print(query.lastProgress)


import matplotlib.pyplot as plt

3
status_codes = [200, 404, 500]
counts = [1450, 800, 560]

plt.bar(status_codes, counts, color=['green', 'orange', 'red'])
plt.xlabel('HTTP Status Code')
plt.ylabel('Request Count')
plt.title('Real-time HTTP Status Counts')
plt.show()


import os
import re
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("RealTimeLogAnalysis").getOrCreate()


log_file_path = "/content/logdata/web-server-access-logs_10k.log"


df = spark.read.text(log_file_path)


pattern = r'(\S+) (\S+) (\S+) \[(.*?)\] "(.*?)" (\d{3}) (\S+)'
parsed_df = df.select(
    F.regexp_extract("value", pattern, 6).cast("integer").alias("status")
).filter(F.col("status").isNotNull())


batch_status_counts = parsed_df.groupBy("status").count()


status_counts_pd = batch_status_counts.toPandas()


status_counts_pd = status_counts_pd.sort_values(by='count', ascending=False)


total = status_counts_pd['count'].sum()
status_counts_pd['percent'] = status_counts_pd['count'] / total * 100


plt.figure(figsize=(10,6))
bars = plt.bar(status_counts_pd['status'].astype(str), status_counts_pd['count'],
               color=plt.cm.viridis(status_counts_pd['percent'] / max(status_counts_pd['percent'])))


for bar, count, percent in zip(bars, status_counts_pd['count'], status_counts_pd['percent']):
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + max(status_counts_pd['count'])*0.01,
             f'{count}\n({percent:.1f}%)', ha='center', va='bottom', fontsize=9)

plt.xlabel('HTTP Status Code', fontsize=12)
plt.ylabel('Request Count', fontsize=12)
plt.title('HTTP Status Code Distribution with Counts and Percentages', fontsize=14)
plt.xticks(rotation=0)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()


with open(input_file, "r") as f:
    total_lines = sum(1 for line in f)
print(f"Total lines in file: {total_lines}")



for stream in spark.streams.active:
    stream.stop()


ip_counts = logs_parsed.groupBy("ip").count().orderBy(desc("count"))


request_counts = logs_parsed.groupBy("request").count().orderBy(desc("count"))


ip_query = ip_counts.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("ip_counts_memory") \
    .start()

request_query = request_counts.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("request_counts_memory") \
    .start()


import time
time.sleep(10)


ip_results_df = spark.sql("SELECT * FROM ip_counts_memory ORDER BY count DESC LIMIT 10")
request_results_df = spark.sql("SELECT * FROM request_counts_memory ORDER BY count DESC LIMIT 10")


print("Top 10 Most Frequent IP Addresses:")
ip_results_df.show(truncate=False)

print("Top 10 Most Frequent Requests:")
request_results_df.show(truncate=False)


ip_query.stop()
request_query.stop()

import time
import shutil
import os


for stream in spark.streams.active:
    stream.stop()


stream_folder = "/content/logdata_stream"
if os.path.exists(stream_folder):
    shutil.rmtree(stream_folder)
os.makedirs(stream_folder, exist_ok=True)


ip_counts = logs_parsed.groupBy("ip").count().orderBy(desc("count"))


request_counts = logs_parsed.groupBy("request").count().orderBy(desc("count"))


ip_query = ip_counts.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("ip_counts_memory") \
    .start()


request_query = request_counts.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("request_counts_memory") \
    .start()


input_file = "/content/logdata/web-server-access-logs_10k.log"
chunk_size = 500

with open(input_file, "r") as infile:
    lines = infile.readlines()

for i in range(0, len(lines), chunk_size):
    chunk = lines[i:i+chunk_size]
    file_path = f"{stream_folder}/log_part_{i//chunk_size}.log"
    with open(file_path, "w") as outfile:
        outfile.writelines(chunk)
    print(f"Added {file_path}")
    time.sleep(5)


time.sleep(10)


ip_results_df = spark.sql("SELECT * FROM ip_counts_memory ORDER BY count DESC LIMIT 10")
request_results_df = spark.sql("SELECT * FROM request_counts_memory ORDER BY count DESC LIMIT 10")


print("Top 10 Most Frequent IP Addresses:")
ip_results_df.show(truncate=False)

print("Top 10 Most Frequent Requests:")
request_results_df.show(truncate=False)


ip_query.stop()
request_query.stop()


filtered_logs_parsed = logs_parsed.filter(col("status").isNotNull())


avg_size_by_status = filtered_logs_parsed.groupBy("status").agg(avg("size").alias("average_response_size"))


avg_size_by_status = avg_size_by_status.orderBy(desc("average_response_size"))


query = avg_size_by_status.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("avg_size_by_status_memory") \
    .start()


import time
time.sleep(10)


avg_size_results_df = spark.sql("SELECT * FROM avg_size_by_status_memory ORDER BY average_response_size DESC")


print("Average Response Size by HTTP Status Code:")
avg_size_results_df.show()


query.stop()

import time
import shutil
import os
from pyspark.sql.functions import avg, desc, col



for stream in spark.streams.active:
    stream.stop()


stream_folder = "/content/logdata_stream"
if os.path.exists(stream_folder):
    shutil.rmtree(stream_folder)
os.makedirs(stream_folder, exist_ok=True)


filtered_logs_parsed = logs_parsed.filter(col("status").isNotNull())


avg_size_by_status = filtered_logs_parsed.groupBy("status").agg(avg("size").alias("average_response_size"))


avg_size_by_status = avg_size_by_status.orderBy(desc("average_response_size"))


query = avg_size_by_status.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("avg_size_by_status_memory") \
    .start()


input_file = "/content/logdata/web-server-access-logs_10k.log"
chunk_size = 500

with open(input_file, "r") as infile:
    lines = infile.readlines()

for i in range(0, len(lines), chunk_size):
    chunk = lines[i:i+chunk_size]
    file_path = f"{stream_folder}/log_part_{i//chunk_size}.log"
    with open(file_path, "w") as outfile:
        outfile.writelines(chunk)
    print(f"Added {file_path}")
    time.sleep(5)


time.sleep(10)


avg_size_results_df = spark.sql("SELECT * FROM avg_size_by_status_memory ORDER BY average_response_size DESC")


print("Average Response Size by HTTP Status Code:")
avg_size_results_df.show()


query.stop()

import matplotlib.pyplot as plt
import pyspark.sql.functions as F
import pandas as pd
from pyspark.sql.functions import to_timestamp, date_trunc, col

for stream in spark.streams.active:
    stream.stop()

from pyspark.sql.functions import to_timestamp, date_trunc, col


logs_parsed_time = logs_parsed.select(
    to_timestamp(col("timestamp"), "dd/MMM/yyyy:HH:mm:ss Z").alias("timestamp"),
    "ip", "client", "user", "request", "status", "size"
)


request_volume_over_time = logs_parsed_time.groupBy(date_trunc('hour', col('timestamp')).alias('hour')).count()


request_volume_over_time = request_volume_over_time.orderBy('hour')

request_volume_over_time_str = request_volume_over_time.withColumn("hour_str", col("hour").cast("string"))


query_volume = request_volume_over_time_str.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("request_volume_memory") \
    .start()

import pandas as pd
import matplotlib.pyplot as plt


request_volume_results_df = spark.sql("SELECT hour_str, count FROM request_volume_memory ORDER BY hour_str")


request_volume_pd = request_volume_results_df.toPandas()


request_volume_pd['hour'] = pd.to_datetime(request_volume_pd['hour_str'], unit='ns')


plt.figure(figsize=(12, 6))
plt.plot(request_volume_pd['hour'], request_volume_pd['count'])
plt.xlabel('Time')
plt.ylabel('Request Count')
plt.title('Request Volume Over Time')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


query_volume.stop()
