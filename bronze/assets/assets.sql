SELECT
  stocks
  , cast(close as double) as close_price
  , cast(change as double) as change_price
  , cast(volume as double) as volume
  , cast(market_cap as double) as market_cap
  , logo
  , asset_type
  , cast(loaded_at as timestamp) as loaded_at
FROM view_assets
QUALIFY ROW_NUMBER() OVER (PARTITION BY stocks ORDER BY loaded_at DESC) = 1