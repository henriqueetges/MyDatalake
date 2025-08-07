 SELECT
 `symbol`
  , summaryProfile.city as  `city`
  , summaryProfile.zip  as `zip`
  , summaryProfile.state as `state`
  , summaryProfile.industry as  `industry`
  , summaryProfile.country as `country`
  , summaryProfile.sectorDisp as `sector`
  , summaryProfile.address1 as`address1`
  , summaryProfile.address2 as`address2`
  , summaryProfile.longBusinessSummary as `BusinessSummary`
  , cast(current_timestamp()  as date) as `loaded_at`
  
  FROM  `bronze`.`brapi`.`tickers`
QUALIFY row_number() OVER (PARTITION BY symbol ORDER BY current_timestamp() DESC) = 1