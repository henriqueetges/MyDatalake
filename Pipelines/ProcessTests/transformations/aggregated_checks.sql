CREATE STREAMING table silver.checks.aggregated_checks_history
SELECT
  current_timestamp() AS ingest_ds,
  test_name,
  table_name,
  test_type,
  layer,
  run_date,
  mandate,
  AVG(check_score) AS total_score,
  SUM(1) AS columns_checked,
  SUM(CASE WHEN check_result = 'passed' THEN 1 ELSE 0 END) AS passing_cols,
  SUM(CASE WHEN check_result = 'passed' THEN 0 ELSE 1 END) AS failing_cols
FROM STREAM(silver.checks.column_checks)
GROUP BY test_name, table_name, test_type, layer, run_date, mandate


