CREATE MATERIALIZED VIEW gold.checks.aggregated_checks_history AS
SELECT 
current_timestamp() AS ingest_ds
, test_name
, test_type
, layer
, table_name
, mandate
, run_date
, total_score
, rows_checked
, passing_rows
, failing_rows
FROM silver.checks.aggregated_checks_history