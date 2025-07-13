{{config(materialized='table')}}

WITH product_changes AS (--Customers who changed their subscription product from the prior month
    SELECT s.metric_month,
    s.userId,
    s.billingCountry,
    CASE
        WHEN s.product = 'BUSINESS' THEN 'Upgrade'
        WHEN s.product = 'PRO' THEN 'Downgrade'
        WHEN s.product = 'GURU' AND s.product_lead = 'PRO' THEN 'Upgrade'
        WHEN s.product = 'GURU' AND s.product_lead = 'BUSINESS' THEN 'Downgrade'
        ELSE 'UNKNOWN'
    END AS product_outcome
    FROM {{ref('int_monthly_subs_leadlag')}} s
    WHERE s.product_lead IS NOT NULL
    AND s.product_lead != s.product
)
SELECT pc.metric_month,
pc.product_outcome,
COUNT(DISTINCT(pc.userId)) AS no_customers
FROM product_changes pc
GROUP BY pc.metric_month, pc.product_outcome
ORDER BY pc.metric_month, pc.product_outcome