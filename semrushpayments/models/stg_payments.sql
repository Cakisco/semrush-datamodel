{{config(materialized='table')}}

SELECT ROW_NUMBER() OVER (ORDER BY p.transactionTime, p.userId) AS payment_id,
p.userId, 
p.billingCountry, 
TO_TIMESTAMP(p.transactionTime) AS transactionTime,
DATE_TRUNC('month',TO_TIMESTAMP(p.transactionTime))::date AS transactionMonth,
p.product,
p.price,
p.amount,
p.period,
ROW_NUMBER() OVER(PARTITION BY p.userId ORDER BY p.transactionTime) AS payment_rank
FROM {{ref('src_payments')}} p