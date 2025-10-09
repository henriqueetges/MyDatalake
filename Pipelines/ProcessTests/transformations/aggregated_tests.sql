CREATE LIVE TABLE append AS
SELECT
  test_name,
  table_name,
  test_type,
  layer,
  run_date,
  mandate,
  `column` as column_tested, 
  AVG(check_score) AS total_score,
  SUM(1) AS rows_checked,
  SUM(CASE WHEN check_result = 'passed' THEN 1 ELSE 0 END) AS passing_rows,
  SUM(CASE WHEN check_result = 'passed' THEN 0 ELSE 1 END) AS failing_rows        
FROM silver.checks.column_checks
where run_date > (SELECT max_dt FROM max_test_date)
GROUP BY test_name, table_name, test_type, layer, run_date, mandate, `column`;

CREATE LIVE TABLE cleaned AS
SELECT * FROM LIVE.aggregated_tests
WHERE run_date NOT IN (SELECT DISTINCT run_date FROM LIVE.append);

CREATE LIVE TABLE silver.checks.aggregated_tests AS
SELECT * FROM LIVE.cleaned
UNION ALL 
SELECT * FROM LIVE.append;



