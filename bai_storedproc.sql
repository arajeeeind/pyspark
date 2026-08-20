CREATE OR REPLACE PROCEDURE public.sp_process_bai_staging_import(
    p_bucket_name         VARCHAR(255),
    p_staging_prefix      VARCHAR(255),
    p_iam_role_arn        VARCHAR(255)
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1. Truncate staging tables
    TRUNCATE TABLE L2_Stage_Account, L2_Stage_P_Transaction, L2_Stage_P_Trans_Text;

    -- 2. Bulk Import 03 Records (Accounts)
    PERFORM aws_s3.table_import_from_s3(
        'L2_Stage_Account',
        '',
        '(FORMAT PARQUET)',
        aws_commons.create_s3_uri(p_bucket_name, p_staging_prefix || 'accounts/', 'us-east-1'),
        aws_commons.create_aws_credentials(p_iam_role_arn)
    );

    -- 3. Bulk Import 16 Records (Transactions)
    PERFORM aws_s3.table_import_from_s3(
        'L2_Stage_P_Transaction',
        '',
        '(FORMAT PARQUET)',
        aws_commons.create_s3_uri(p_bucket_name, p_staging_prefix || 'transactions/', 'us-east-1'),
        aws_commons.create_aws_credentials(p_iam_role_arn)
    );

    -- 4. Bulk Import 88 Records (Trans Text)
    PERFORM aws_s3.table_import_from_s3(
        'L2_Stage_P_Trans_Text',
        '',
        '(FORMAT PARQUET)',
        aws_commons.create_s3_uri(p_bucket_name, p_staging_prefix || 'text/', 'us-east-1'),
        aws_commons.create_aws_credentials(p_iam_role_arn)
    );

    -- 5. Upsert missing accounts into L2_R_Account (Generates acct_id)
    INSERT INTO L2_R_Account (acct_no, acct_type, bankcode, currency_code)
    SELECT DISTINCT s.acct_no, s.acct_type, s.bankcode, s.currency_code
    FROM L2_Stage_Account s
    WHERE NOT EXISTS (
        SELECT 1 FROM L2_R_Account a
        WHERE a.acct_no = s.acct_no 
          AND a.bankcode = s.bankcode
    )
    ON CONFLICT (acct_no, bankcode) DO NOTHING;

    -- 6. Atomic CTE Load into Core Fact Tables
    WITH inserted_txns AS (
        INSERT INTO L2_P_Transaction (
            acct_id, bai_id, amount, bank_refno, cust_refno, txn_text, stage_ref_id
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

END;
$$;
