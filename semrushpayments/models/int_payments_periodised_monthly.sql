{{config(materialized='table')}}


SELECT dm.metric_month,
p.userId,
p.billingCountry,
p.product,
p.amount_periodised
FROM {{ref('int_dim_months')}} dm
LEFT JOIN {{ref('int_payments_periodised')}} p ON dm.metric_month >= p.active_from AND dm.metric_month <= p.active_to