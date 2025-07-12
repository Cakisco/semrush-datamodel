{{config(materialized='table')}}

SELECT df.metric_month,
p.billingCountry,
p.product,
COUNT(DISTINCT(p.userId)) AS active_subscribers,
SUM(p.amount_periodised) AS mrr
FROM {{ref('int_dim_months')}} df
LEFT JOIN {{ref('int_payments_periodised')}} p ON df.metric_month >= p.active_from AND df.metric_month <= p.active_to
GROUP BY df.metric_month, p.billingCountry, p.product
ORDER BY df.metric_month, p.billingCountry, p.product