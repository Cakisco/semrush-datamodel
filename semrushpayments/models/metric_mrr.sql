{{config(materialized='table')}}

SELECT p.metric_month,
p.billingCountry,
p.product,
COUNT(DISTINCT(p.userId)) AS active_subscribers,
SUM(p.amount_periodised) AS mrr
FROM {{ref('int_monthly_subs')}} p
GROUP BY p.metric_month, p.billingCountry, p.product
ORDER BY p.metric_month, p.billingCountry, p.product