CREATE MATERIALIZED VIEW gold.checks.column_checks AS
SELECT 
uuid() AS test_key
  , layer
  , table_name AS dataset
  , df_key AS dataset_key
  , test_type
  , test_name
  , test_type || '-' || coalesce(test_name, column) AS test_performed
  , column AS column_tested
  , mandate
  , run_date
  , check_result
  , check_score

FROM silver.checks.column_checks