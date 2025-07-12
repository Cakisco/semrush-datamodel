{{config(materialized='table')}}

SELECT p.payment_id, 
p.userId, 
p.billingCountry,
p.product, 
p.transactionMonth AS active_from,
p.transactionMonth + (p.period-1 || ' months')::INTERVAL AS active_to,
p.amount,
p.amount/p.period AS amount_periodised
FROM {{ref('stg_payments')}} p