{{config(materialized='table')}}

WITH date_frame AS (--Monthly date series
    SELECT generate_series AS metric_month
    FROM GENERATE_SERIES(
        (SELECT MIN(active_from) FROM {{ref('int_payments_periodised')}}),
        (SELECT MAX(active_to) FROM {{ref('int_payments_periodised')}}),
        '1 month'::INTERVAL
    ) AS metric_month
    WHERE generate_series <= '2017-12-01'::date --Max date for metric
)

SELECT df.metric_month,
p.billingCountry,
p.product,
COUNT(DISTINCT(p.userId)) AS active_subscribers,
SUM(p.amount_periodised) AS mrr
FROM date_frame df
LEFT JOIN {{ref('int_payments_periodised')}} p ON df.metric_month >= p.active_from AND df.metric_month <= p.active_to
GROUP BY df.metric_month, p.billingCountry, p.product
ORDER BY df.metric_month, p.billingCountry, p.product