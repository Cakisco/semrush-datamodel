{{config(materialized='table')}}

WITH acquired AS (--Number of customers who were not subscribed on the prior month
    SELECT s.metric_month,
    s.product,
    s.billingCountry,
    COUNT(DISTINCT(CASE WHEN s.product_lead IS NULL THEN s.userId ELSE NULL END)) AS customers_acquired
    FROM {{ref('int_monthly_subs_leadlag')}} s
    GROUP BY s.metric_month, s.product, s.billingCountry
),
cancelled AS (--Number of customers cancelling on a given month
    SELECT s.metric_month + '1 month'::interval AS metric_month,
    s.product,
    s.billingCountry,
    COUNT(DISTINCT(s.userId)) AS cancelled_customers
    FROM {{ref('int_monthly_subs_leadlag')}} s
    WHERE s.product_lag IS NULL
    GROUP BY s.metric_month, s.product, s.billingCountry
),
customerbase AS (--Number of customers at beginning of month
    SELECT s.metric_month + '1 month'::interval AS metric_month,
    s.product,
    s.billingCountry,
    COUNT(DISTINCT(s.userId)) AS customer_base
    FROM {{ref('int_monthly_subs_leadlag')}} s
    GROUP BY s.metric_month, s.product, s.billingCountry
)

SELECT a.metric_month,
a.product,
a.billingCountry,
a.customers_acquired,
ca.cancelled_customers,
cu.customer_base
FROM acquired a
LEFT JOIN cancelled ca ON ca.metric_month = a.metric_month AND ca.product = a.product AND ca.billingCountry = a.billingCountry
LEFT JOIN customerbase cu ON cu.metric_month = a.metric_month AND cu.product = a.product AND cu.billingCountry = a.billingCountry
ORDER BY a.metric_month