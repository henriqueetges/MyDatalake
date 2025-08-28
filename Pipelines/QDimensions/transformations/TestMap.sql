CREATE MATERIALIZED VIEW silver.checks.test_mapping
AS

SELECT
name as dimension
, explode(tests) as tests_mapped

FROM bronze.checks.quality_dimensions
