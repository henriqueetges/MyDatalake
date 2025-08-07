

SELECT
  CAST(currency AS string) as currency
  , CAST(marketCap AS DOUBLE) as marketCap
  , CAST(shortName AS string) as shortName
  , CAST(longName AS string) as longName
  , CAST(regularMarketChange AS DOUBLE) as regularMarketChange
  , CAST(regularMarketChangePercent AS DOUBLE) as regularMarketChangePercent
  , CAST(regularMarketTime AS timestamp) as regularMarketTime
  , CAST(regularMarketPrice AS DOUBLE) as regularMarketPrice
  , CAST(regularMarketDayHigh AS DOUBLE) as regularMarketDayHigh
  , CAST(regularMarketDayRange AS string) as regularMarketDayRange
  , CAST(regularMarketDayLow AS DOUBLE) as regularMarketDayLow
  , CAST(regularMarketVolume AS BIGINT) as regularMarketVolume
  , CAST(regularMarketPreviousClose AS DOUBLE) as regularMarketPreviousClose
  , CAST(regularMarketOpen AS DOUBLE) as regularMarketOpen
  , CAST(fiftyTwoWeekRange AS string) as fiftyTwoWeekRange
  , CAST(fiftyTwoWeekLow AS DOUBLE) as fiftyTwoWeekLow
  , CAST(fiftyTwoWeekHigh AS DOUBLE) as fiftyTwoWeekHigh
  , CAST(symbol AS string) as symbol
  , CAST(logourl AS string) as logourl
  , historicalDataPrice 
  , summaryProfile
  , CAST(priceEarnings AS double) AS priceEarnings
  , CAST(earningsPerShare AS DOUBLE) AS earningsPerShare
  , CAST(loaded_at AS timestamp) AS loaded_at


FROM view_tickers
QUALIFY row_number() over (partition by symbol order by regularMarketTime desc) = 1
-- DBTITLE 1)
