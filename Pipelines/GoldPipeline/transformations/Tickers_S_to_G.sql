-- Please edit the sample below

CREATE MATERIALIZED VIEW gold.brapi.dTickers AS
SELECT
symbol
, city
, zip
, state
, industry
, country
, sector
, address1 + '-' + address2 as address
, BusinessSummary
, loaded_at
from silver.brapi.summary_profile