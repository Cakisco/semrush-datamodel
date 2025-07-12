{{config(materialized='table')}}

WITH acquired AS (--Number of customers who were not subscribed on the prior month
    SELECT s.metric_month,
    COUNT(DISTINCT(CASE WHEN s.product_lead IS NULL THEN s.userId ELSE NULL END)) AS customers_acquired
    FROM {{ref('int_monthly_subs_leadlag')}} s
    GROUP BY s.metric_month
),
cancelled AS (--Number of customers cancelling on a given month
    SELECT s.metric_month + '1 month'::interval AS metric_month,
    COUNT(DISTINCT(s.userId)) AS cancelled_customers
    FROM {{ref('int_monthly_subs_leadlag')}} s
    WHERE s.product_lag IS NULL
    GROUP BY s.metric_month
),
customerbase AS (--Number of customers at beginning of month
    SELECT s.metric_month + '1 month'::interval AS metric_month,
    COUNT(DISTINCT(s.userId)) AS customer_base
    FROM {{ref('int_monthly_subs_leadlag')}} s
    GROUP BY s.metric_month
)



SELECT a.metric_month, 
a.customers_acquired,
ca.cancelled_customers,
cu.customer_base
FROM acquired a
LEFT JOIN cancelled ca ON ca.metric_month = a.metric_month
LEFT JOIN customerbase cu ON cu.metric_month = a.metric_month
ORDER BY a.metric_month