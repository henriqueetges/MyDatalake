-- Please edit the sample below

CREATE MATERIALIZED VIEW gold.brapi.Calendar AS
SELECT
    date_add(current_date(), cast(id as int)) as `date`
    , date_part('year', date_add(current_date(), cast(id as int))) as `year`
    , date_part('month', date_add(current_date(), cast(id as int))) as `month`
    , date_part('week',  date_add(current_date(), cast(id as int))) as `week`
    , date_format(date_add(current_date(), cast(id as int)), 'MMMM') as `month_name`
    , date_format(date_add(current_date(), cast(id as int)), 'MMM') as `month_name_abr`
    , date_part('day', date_add(current_date(), cast(id as int))) as `day`
    , datediff(date_add(current_date(), cast(id as int)) , current_date()) as `day_aging`
    , date_part('year', date_add(current_date(), cast(id as int))) - date_part('year', current_date()) as `year_aging`
    , date_part('week', date_add(current_date(), cast(id as int))) - date_part('week', current_date()) as `week_aging`
    , datediff(MONTH,current_date(), date_add(current_date(), cast(id as int)) ) as `month_aging`
    , date_part('DAYOFWEEK_ISO', date_add(current_date(), cast(id as int))) as `dayofweek`

    
FROM range(-730, 365, 1)
