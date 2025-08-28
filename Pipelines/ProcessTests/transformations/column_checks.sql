CREATE OR REFRESH LIVE TABLE silver.checks.column_checks
as
SELECT 
 layer
, table_name
, df_key
, test_type
, nullif(trim(test_name), '') test_name
, column
, mandate
, run_date
, check_result
, check_score
FROM bronze.checks.column_checks