# -------------------------------------------------------------------------
# Execute Aurora Stored Procedure (Best Practice Pattern)
# -------------------------------------------------------------------------
import psycopg2

conn = psycopg2.connect(
    host=args['AURORA_HOST'],
    dbname=args['AURORA_DB'],
    user=args['AURORA_USER'],
    password=args['AURORA_PASS'],
    port=5432
)

cursor = conn.cursor()

try:
    print("Invoking Aurora Stored Procedure for S3 Import & Data Enrichment...")
    
    # Calls stored procedure passing S3 path params & IAM Role
    cursor.execute(
        "CALL public.sp_process_bai_staging_import(%s, %s, %s);",
        (s3_bucket, s3_staging_prefix, args['AURORA_IAM_ROLE_ARN'])
    )
    
    conn.commit()
    print("Aurora pipeline completed successfully via Stored Procedure!")

except Exception as e:
    conn.rollback()
    print(f"Error executing Aurora Stored Procedure: {str(e)}")
    raise e

finally:
    cursor.close()
    conn.close()

job.commit()
