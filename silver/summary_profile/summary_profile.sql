WITH `main` AS (
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
),

`dedup` AS (
  SELECT DISTINCT
  `symbol`
  , `city`
  , `zip`
  , `state`
  , `industry`
  , `country`
  , `sector`
  , `address1`
  , `address2`
  , `BusinessSummary`
  , `loaded_at`
  , row_number() OVER (PARTITION BY `symbol` ORDER BY `loaded_at` desc) as `rank`
  
  , current_timestamp()
FROM `main`
)


SELECT 
  `symbol`
  , `city`
  , `zip`
  , `state`
  , `industry`
  , `country`
  , `sector`
  , `address1`
  , `address2`
  , `BusinessSummary`
  , `loaded_at`

FROM `dedup`
where `rank` = 1 
