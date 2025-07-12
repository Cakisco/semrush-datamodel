{{config(materialized='table')}}

SELECT current_period.metric_month, 
current_period.userId,
current_period.billingCountry,
current_period.product,
current_period.amount_periodised,
lead_period.product AS product_lead,
lead_period.amount_periodised AS amount_periodised_lead,
lag_period.product AS product_lag,
lag_period.amount_periodised AS amount_periodised_lag
FROM {{ref('int_monthly_subs')}} current_period
LEFT JOIN {{ref('int_monthly_subs')}} lead_period ON current_period.userId = lead_period.userId AND lead_period.metric_month = current_period.metric_month - '1 month'::interval
LEFT JOIN {{ref('int_monthly_subs')}} lag_period ON current_period.userId = lag_period.userId AND lag_period.metric_month = current_period.metric_month + '1 month'::interval
ORDER BY current_period.metric_month, current_period.userId