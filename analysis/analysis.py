import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import PercentFormatter
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
data_monthly_subs = con.execute("SELECT * FROM int_monthly_subs").fetchdf()
print('Monthly subs models')
print(data_monthly_subs.head())

## Periodised monthly payments with lead and lag payments##
data_monthly_subs_leadlag = con.execute("SELECT * FROM int_monthly_subs_leadlag").fetchdf()
print('Monthly subs models with lead and lag')
print(data_monthly_subs_leadlag.head())


## MRR ##
data_mrr = con.execute("SELECT * FROM metric_mrr").fetchdf()
data_mrr.to_csv('./datasets/mrr.csv', index=False)
print('MRR model')
print(data_mrr.head())


## Churn ##
data_churn = con.execute("SELECT * FROM metric_churn").fetchdf()
data_churn.to_csv('./datasets/churn.csv', index=False)
print('Churn model')
print(data_churn.head())



### 2. Metric visualisations ###
## MRR ##
data_mrr = data_mrr.groupby(['metric_month','billingCountry'])[['active_subscribers','mrr']].sum().reset_index()
data_mrr_mrr = data_mrr.pivot(index='metric_month', columns='billingCountry', values='mrr')
data_mrr_users = data_mrr.pivot(index='metric_month', columns='billingCountry', values='active_subscribers')
#Plotting
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
plt.suptitle('Monthly recurring revenue and subscribed users',fontsize=15)
plt.tight_layout()
plt.savefig('./images/mrr.png')

## Churn ##
data_churn = data_churn.groupby(['churn_month'])[['starting_revenue','starting_customers','retained_revenue','retained_customers']].sum().reset_index()
data_churn['value_retention']=data_churn['retained_revenue']/data_churn['starting_revenue']
data_churn['volume_retention']=data_churn['retained_customers']/data_churn['starting_customers']
data_churn['value_churn'] = 1 - data_churn['value_retention']
data_churn['volume_churn'] = 1 - data_churn['volume_retention']
#Plotting
fig, ax = plt.subplots(figsize=[14,6], ncols=2, nrows=1, sharey=True)
sns.lineplot(data=data_churn, x='churn_month', y='value_churn', ax=ax[0], color='#ff622d', linewidth=2)
sns.lineplot(data=data_churn, x='churn_month', y='volume_churn', ax=ax[1], color='#ff622d', linewidth=2)
#Formatting
for a in ax:
    a.set_xlabel('')
    a.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    a.yaxis.grid(True)
ax[0].set_ylabel('Churn rate')
ax[0].set_title('Value Churn')
ax[1].set_title('Volume Churn')
a.yaxis.grid(True)
plt.suptitle('Monthly churn rates',fontsize=15)

plt.tight_layout()
plt.savefig('./images/churn.png')