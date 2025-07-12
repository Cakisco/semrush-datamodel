import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
con = duckdb.connect("../semrushpayments/semrushpayments.duckdb")

### 1. Importing and exploring models ###

## Source data ##
data_source = con.execute("SELECT * FROM src_payments").fetchdf()
print('Source model')
print(data_source.head())

## Staging model ##
data_staging = con.execute("SELECT * FROM stg_payments").fetchdf()
print('Staging model')
print(data_staging.head())

## Periodised payments ##
data_periodised = con.execute("SELECT * FROM int_payments_periodised").fetchdf()
print('Periodised payments model')
print(data_periodised.head())

## Periodised monthly payments ##
data_periodised_monthly = con.execute("SELECT * FROM int_monthly_subs").fetchdf()
print('Monthly periodised payments model')
print(data_periodised_monthly.head())


## MRR ##
data_mrr = con.execute("SELECT * FROM metric_mrr").fetchdf()
data_mrr.to_csv('./datasets/mrr.csv', index=False)
print('MRR model')
print(data_mrr.head())



### 2. Metric visualisations ###
## MRR ##
data_mrr = data_mrr.groupby(['metric_month','billingCountry'])[['active_subscribers','mrr']].sum().reset_index()
data_mrr_mrr = data_mrr.pivot(index='metric_month', columns='billingCountry', values='mrr')
data_mrr_users = data_mrr.pivot(index='metric_month', columns='billingCountry', values='active_subscribers')
fig, ax = plt.subplots(figsize=[14,6], ncols=2, nrows=1)
data_mrr_mrr.plot(kind='area', stacked=True, ax=ax[0])
data_mrr_users.plot(kind='area', stacked=True, ax=ax[1])
#Formatting
for a in ax:
    a.set_xlabel('')
    a.tick_params(axis='x', labelrotation=90)
    a.legend(title='Country')
ax[0].set_ylabel('Monthly recurring revenue')
ax[1].set_ylabel('Monthly subscribers')
plt.suptitle('Monthly recurring revenue and subscribed users')
plt.tight_layout()
plt.savefig('./images/mrr.png')