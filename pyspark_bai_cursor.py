import sys
import psycopg2
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, split, trim

# 1. Initialize Glue and Spark Context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'S3_INPUT_PATH', 
    'S3_STAGING_OUTPUT_PREFIX', 
    'S3_BUCKET_NAME',
    'AURORA_HOST', 
    'AURORA_DB', 
    'AURORA_USER', 
    'AURORA_PASS',
    'AURORA_IAM_ROLE_ARN'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3_input_path = args['S3_INPUT_PATH']                 # e.g., s3://my-bucket/raw/bai_file.txt
s3_staging_prefix = args['S3_STAGING_OUTPUT_PREFIX']   # e.g., staging/bai_split/
s3_bucket = args['S3_BUCKET_NAME']                     # e.g., my-bucket

# 2. Read Raw BAI File from S3
raw_df = spark.read.text(s3_input_path)

# Filter out empty lines or file header/trailer records (01, 99, 98)
lines_df = raw_df.filter(
    (col("value").isNotNull()) & 
    (trim(col("value")) != "")
)

# Parse Line Code (First 2 characters of each line)
parsed_df = lines_df.withColumn("rec_type", trim(col("value").substr(1, 2))) \
                    .withColumn("raw_content", col("value"))

# -------------------------------------------------------------------------
# Step A: Process 03 Records (Account File)
# -------------------------------------------------------------------------
# BAI 03 Format: 03,acct_no,currency_code,type_code,amount,item_count,fund_type,...
df_03 = parsed_df.filter(col("rec_type") == "03") \
    .withColumn("fields", split(col("raw_content"), ",")) \
    .select(
        trim(col("fields").getItem(1)).alias("acct_no"),
        trim(col("fields").getItem(2)).alias("currency_code"),
        trim(col("fields").getItem(3)).alias("type_code"),
        trim(col("fields").getItem(4)).alias("amount"),
        trim(col("fields").getItem(5)).alias("acct_type"),
        trim(col("fields").getItem(6)).alias("bankcode")
    )

# Write 03 records to S3 Staging
s3_path_03 = f"s3://{s3_bucket}/{s3_staging_prefix}accounts/"
df_03.write.mode("overwrite").parquet(s3_path_03)

# -------------------------------------------------------------------------
# Step B: Process 16 Records (Transaction File)
# -------------------------------------------------------------------------
# BAI 16 Format: 16,type_code,amount,fund_type,bank_refno,cust_refno,text...
df_16 = parsed_df.filter(col("rec_type") == "16") \
    .withColumn("fields", split(col("raw_content"), ",")) \
    .select(
        trim(col("fields").getItem(1)).alias("type_code"),
        trim(col("fields").getItem(2)).alias("amount"),
        trim(col("fields").getItem(3)).alias("fund_type"),
        trim(col("fields").getItem(4)).alias("bank_refno"),
        trim(col("fields").getItem(5)).alias("cust_refno"),
        trim(col("fields").getItem(6)).alias("txn_text"),
        trim(col("fields").getItem(7)).alias("acct_no"),
        trim(col("fields").getItem(8)).alias("bankcode")
    )

# Write 16 records to S3 Staging
s3_path_16 = f"s3://{s3_bucket}/{s3_staging_prefix}transactions/"
df_16.write.mode("overwrite").parquet(s3_path_16)

# -------------------------------------------------------------------------
# Step C: Process 88 Records (Continuation / Text File)
# -------------------------------------------------------------------------
# BAI 88 Format: 88,continuation_text...
df_88 = parsed_df.filter(col("rec_type") == "88") \
    .withColumn("fields", split(col("raw_content"), ",")) \
    .select(
        trim(col("fields").getItem(1)).alias("bank_refno"),
        trim(col("fields").getItem(2)).alias("continuation_text")
    )

# Write 88 records to S3 Staging
s3_path_88 = f"s3://{s3_bucket}/{s3_staging_prefix}text/"
df_88.write.mode("overwrite").parquet(s3_path_88)

print("Parquet files successfully extracted and written to S3 staging directories.")

# -------------------------------------------------------------------------
# Step D: Execute Aurora Staging & Enrichment Pipeline via Direct Connection
# -------------------------------------------------------------------------
# Direct Cluster Endpoint (Bypasses RDS Proxy to enable S3 Extension)
conn = psycopg2.connect(
    host=args['AURORA_HOST'],
    dbname=args['AURORA_DB'],
    user=args['AURORA_USER'],
    password=args['AURORA_PASS'],
    port=5432
)
conn.autocommit = False
cursor = conn.cursor()

try:
    print("Truncating staging tables...")
    cursor.execute("TRUNCATE TABLE L2_Stage_Account, L2_Stage_P_Transaction, L2_Stage_P_Trans_Text;")

    # 1. Bulk Import 03 Records -> L2_Stage_Account
    print("Importing 03 records into L2_Stage_Account...")
    cursor.execute(f"""
        SELECT aws_s3.table_import_from_s3(
            'L2_Stage_Account',
            '',
            '(FORMAT PARQUET)',
            aws_commons.create_s3_uri('{s3_bucket}', '{s3_staging_prefix}accounts/', 'us-east-1'),
            aws_commons.create_aws_credentials('{args["AURORA_IAM_ROLE_ARN"]}')
        );
    """)

    # 2. Bulk Import 16 Records -> L2_Stage_P_Transaction
    print("Importing 16 records into L2_Stage_P_Transaction...")
    cursor.execute(f"""
        SELECT aws_s3.table_import_from_s3(
            'L2_Stage_P_Transaction',
            '',
            '(FORMAT PARQUET)',
            aws_commons.create_s3_uri('{s3_bucket}', '{s3_staging_prefix}transactions/', 'us-east-1'),
            aws_commons.create_aws_credentials('{args["AURORA_IAM_ROLE_ARN"]}')
        );
    """)

    # 3. Bulk Import 88 Records -> L2_Stage_P_Trans_Text
    print("Importing 88 records into L2_Stage_P_Trans_Text...")
    cursor.execute(f"""
        SELECT aws_s3.table_import_from_s3(
            'L2_Stage_P_Trans_Text',
            '',
            '(FORMAT PARQUET)',
            aws_commons.create_s3_uri('{s3_bucket}', '{s3_staging_prefix}text/', 'us-east-1'),
            aws_commons.create_aws_credentials('{args["AURORA_IAM_ROLE_ARN"]}')
        );
    """)

    # 4. Upsert Missing Accounts into L2_R_Account (Generates missing acct_id sequence)
    print("Upserting new account records into L2_R_Account...")
    cursor.execute("""
        INSERT INTO L2_R_Account (acct_no, acct_type, bankcode, currency_code)
        SELECT DISTINCT s.acct_no, s.acct_type, s.bankcode, s.currency_code
        FROM L2_Stage_Account s
        WHERE NOT EXISTS (
            SELECT 1 FROM L2_R_Account a
            WHERE a.acct_no = s.acct_no 
              AND a.bankcode = s.bankcode
        )
        ON CONFLICT (acct_no, bankcode) DO NOTHING;
    """)

    # 5. Atomic Multi-Table Load into Core Fact Tables (Enriching acct_id, bai_id, etc.)
    print("Executing atomic CTE insert into core fact tables...")
    cursor.execute("""
        WITH inserted_txns AS (
            INSERT INTO L2_P_Transaction (
                acct_id, 
                bai_id, 
                amount, 
                bank_refno, 
                cust_refno, 
                txn_text,
                stage_ref_id
            )
            SELECT 
                a.acct_id,
                b.bai_id,
                CAST(st.amount AS NUMERIC(18,2)),
                st.bank_refno,
                st.cust_refno,
                st.txn_text,
                st.bank_refno AS stage_ref_id
            FROM L2_Stage_P_Transaction st
            JOIN L2_R_Account a ON st.acct_no = a.acct_no AND st.bankcode = a.bankcode
            JOIN L2_R_BAI b     ON st.type_code = b.type_code
            RETURNING transaction_id, stage_ref_id
        )
        INSERT INTO L2_P_Trans_Text (transaction_id, continuation_text)
        SELECT 
            it.transaction_id,
            txt.continuation_text
        FROM inserted_txns it
        JOIN L2_Stage_P_Trans_Text txt ON it.stage_ref_id = txt.bank_refno;
    """)

    # Commit all database transformations atomically
    conn.commit()
    print("Aurora database staging, account generation, and transaction enrichment completed successfully!")

except Exception as e:
    conn.rollback()
    print(f"Pipeline failed. Rolling back transaction. Error: {str(e)}")
    raise e

finally:
    cursor.close()
    conn.close()

job.commit()
