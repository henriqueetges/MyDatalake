
  SELECT
 `symbol`
  , CAST(from_unixtime(cast(exploded.date as bigint)) AS DATE) as `date`
  , CAST(exploded.open as float) as `open`
  , cast(exploded.high as float) as `high`
  , cast(exploded.low  as float) as `low`
  , cast(exploded.close as float) as `close`
  , cast(exploded.volume as float) as `volume`
  , cast(exploded.adjustedClose as float) as `adjustedClose`
  , cast(current_timestamp() as date ) as `loaded_at` 
  FROM  `bronze`.`brapi`.`tickers`
  LATERAL VIEW explode(`historicalDataPrice`) as exploded 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY `symbol`, `date` ORDER BY `date` DESC) = 1

