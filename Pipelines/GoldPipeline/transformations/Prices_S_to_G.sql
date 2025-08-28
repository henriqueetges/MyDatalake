

CREATE MATERIALIZED VIEW `gold`.`brapi`.`Fprices` AS
SELECT
    symbol
    , concat_ws('_',symbol,`date`) AS df_key
    , `date`
    , cast(round(low   , 2) AS DECIMAL(10,2)) AS low
    , cast(round(close, 2) AS DECIMAL(10,2)) AS close
    , cast(round(high    , 2) AS DECIMAL(10,2)) AS high
    , cast(round(open, 2) AS DECIMAL(10,2)) AS open
    , cast(round(`volume`, 2)AS DECIMAL(10,2)) AS `volume`
    , cast(round(adjustedClose, 2)AS DECIMAL(10,2)) AS adjustedClose
    , cast(round((close - open), 2) AS DECIMAL(10,2))AS intraday_change
    , cast(round((high - low), 2)AS DECIMAL(10,2)) AS intraday_range
FROM silver.brapi.prices
