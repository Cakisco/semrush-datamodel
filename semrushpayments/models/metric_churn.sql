{{config(materialized='table')}}

SELECT s.metric_month + '1 month'::interval AS churn_month, --The current month represents the prior period, and churn is calculated for teh next month
SUM(s.amount_periodised) AS starting_revenue,
COUNT(DISTINCT(s.userId)) AS starting_customers,
SUM(s.amount_periodised_lag) AS retained_revenue,
COUNT(DISTINCT(CASE WHEN s.amount_periodised_lag IS NOT NULL THEN s.userId ELSE NULL END)) AS retained_customers
FROM {{ref('int_monthly_subs_leadlag')}} s
WHERE s.metric_month + '1 month'::interval < '2018-01-01'::date --Excluding 2018 figure
GROUP BY s.metric_month
ORDER BY s.metric_month