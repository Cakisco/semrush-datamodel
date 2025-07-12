{{config(materialized='table')}}

SELECT generate_series AS metric_month
FROM GENERATE_SERIES(
    (SELECT MIN(active_from) FROM {{ref('int_payments_periodised')}}),
    (SELECT MAX(active_to) FROM {{ref('int_payments_periodised')}}),
    '1 month'::INTERVAL
) AS metric_month
WHERE generate_series <= '2017-12-01'::date --Max date for metric