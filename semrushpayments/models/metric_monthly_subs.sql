{{config(materialized='table')}}

SELECT s.metric_month,
s.product,
s.billingCountry,
COUNT(DISTINCT(s.userId)) AS no_customers,
SUM(s.amount_periodised) AS mrr
FROM {{ref('int_monthly_subs')}} s
GROUP BY s.metric_month, s.product, s.billingCountry
ORDER BY s.metric_month, s.product, s.billingCountry