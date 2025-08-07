

CREATE MATERIALIZED VIEW `gold`.`brapi`.`prices` AS
SELECT
    symbol
    , date
    , low   
    , close
    , high    
    , open
    , volume
    , adjustedClose
    , (close - open) AS intraday_change
    , (high - low) AS intraday_range
FROM silver.brapi.prices
