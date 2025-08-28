CREATE MATERIALIZED VIEW silver.checks.quality_dimensions
AS

SELECT
name as dimension
, baseline
, threshold
, weight

FROM bronze.checks.quality_dimensions