import sys
import math
import boto3
import json
import psycopg2  # Native driver pre-installed in Glue PySpark environments
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ----------------------------------------------------------------------
# 1. Initialize AWS Glue & Spark Contexts
# ----------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DB_SECRET_NAME', 'S3_BUCKET_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read runtime parameters
db_secret_name = args['DB_SECRET_NAME']
s3_bucket = args['S3_BUCKET_NAME']

input_s3_path = f"s3://{s3_bucket}/inbound_bai/*.bai"
staging_s3_prefix = "staging/L2_Transaction/"
staging_s3_full_path = f"s3://{s3_bucket}/{staging_s3_prefix}"

# ----------------------------------------------------------------------
# 2. Read Raw BAI2 File & Apply Stateful Ingestion
# ----------------------------------------------------------------------
raw_df = spark.read.text(input_s3_path)

# Step 1: Assign monotonic line IDs to preserve file sequence
indexed_df = raw_df.withColumn("line_id", F.monotonically_increasing_id())

# Step 2: Extract record codes, account anchors, and transaction anchors
parsed_df = indexed_df \
    .withColumn("rec_code", F.substring(F.col("value"), 1, 2)) \
    .withColumn("extracted_acct", F.when(F.col("rec_code") == "03", F.split(F.col("value"), ",")[1])) \
    .withColumn("txn_anchor_id", F.when(F.col("rec_code") == "16", F.col("line_id")))

# Step 3: Define directional order window frame
order_window = Window.orderBy("line_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Step 4: Forward-fill Account ID and Transaction Anchor ID
stateful_df = parsed_df \
    .withColumn("acct_id", F.last("extracted_acct", ignorenulls=True).over(order_window)) \
    .withColumn("parent_txn_line_id", F.last("txn_anchor_id", ignorenulls=True).over(order_window))

# Step 5: Filter strictly for '16' (Transactions) and '88' (Continuations)
txn_and_88_df = stateful_df.filter(F.col("rec_code").isin("16", "88"))

# Step 6: Group by transaction anchor & collapse '88' continuation lines
aligned_transactions = txn_and_88_df.groupBy("acct_id", "parent_txn_line_id").agg(
    F.first(F.when(F.col("rec_code") == "16", F.col("value")), ignorenulls=True).alias("txn_16_raw"),
    F.collect_list(F.when(F.col("rec_code") == "88", F.col("value"))).alias("continuation_88_lines"),
    F.concat_ws(" | ", F.collect_list(F.when(F.col("rec_code") == "88", F.col("value")))).alias("combined_88_text")
)

# Step 7: Parse CSV string fields into target schema
final_l2_dataset = aligned_transactions.select(
    F.col("acct_id"),
    F.split(F.col("txn_16_raw"), ",")[1].alias("type_code"),
    F.split(F.col("txn_16_raw"), ",")[2].alias("amount"),
    F.split(F.col("txn_16_raw"), ",")[6].alias("bank_refno"),
    F.split(F.col("txn_16_raw"), ",")[7].alias("cust_refno"),
    F.split(F.col("txn_16_raw"), ",")[8].alias("txn_text"),
    F.to_json(F.col("continuation_88_lines")).alias("continuation_88_json"),
    F.col("combined_88_text")
)

# ----------------------------------------------------------------------
# 3. Dynamic Partition Calculation & Parquet Write
# ----------------------------------------------------------------------
TARGET_ROWS_PER_PARTITION = 200000

# Get total processed transaction count
total_rows = final_l2_dataset.count()

# Calculate dynamic partitions (guarantee at least 1 partition for small files)
num_partitions = max(1, math.ceil(total_rows / TARGET_ROWS_PER_PARTITION))

print(f"Processed {total_rows:,} total transactions. Writing using {num_partitions} dynamic partition(s).")

# Write Parquet files to S3 staging location
final_l2_dataset \
    .repartition(num_partitions) \
    .write \
    .mode("overwrite") \
    .parquet(staging_s3_full_path)

# ----------------------------------------------------------------------
# 4. Trigger Aurora PostgreSQL Native S3 Bulk Import
# ----------------------------------------------------------------------
def get_db_credentials(secret_name):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

# Retrieve DB credentials securely
secret = get_db_credentials(db_secret_name)

# Connect to Aurora PostgreSQL DB Cluster
conn = psycopg2.connect(
    host=secret['host'],
    port=secret['port'],
    dbname=secret['dbname'],
    user=secret['username'],
    password=secret['password']
)
conn.autocommit = True
cursor = conn.cursor()

# Execute low-level parallel S3 import function inside Aurora
import_query = f"""
SELECT aws_s3.table_import_from_s3(
   'L2_Transaction',
   'acct_id, type_code, amount, bank_refno, cust_refno, txn_text, continuation_88_json, combined_88_text',
   '(FORMAT PARQUET)',
   aws_commons.create_s3_uri('{s3_bucket}', '{staging_s3_prefix}', 'us-east-1')
);
"""

print("Starting Aurora S3 Bulk Import execution...")
cursor.execute(import_query)
print("Aurora Bulk Import completed successfully!")

cursor.close()
conn.close()

job.commit()