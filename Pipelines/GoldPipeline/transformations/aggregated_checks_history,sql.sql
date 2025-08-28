CREATE MATERIALIZED VIEW gold.checks.aggregated_checks_history AS
SELECT 
current_timestamp() AS ingest_ds
, test_name
, test_type
, layer
, mandate
, run_date
, total_score
, columns_checked
, passing_cols
, failing_cols
FROM silver.checks.aggregated_checks_history