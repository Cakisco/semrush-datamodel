import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import PercentFormatter
import matplotlib.dates as mdates
con = duckdb.connect("../semrushpayments/semrushpayments.duckdb")

### 1. Loading datasets and exporting as .csv ###
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

## Acquisitions ##
data_acquired = con.execute("SELECT * FROM metric_acquisitions").fetchdf()
data_acquired.to_csv('./datasets/acquisitions.csv', index=False)
print('Acquisitions')
print(data_acquired.head())

## Subscription products ##
data_monthly_subs = con.execute("SELECT * FROM metric_monthly_subs").fetchdf()
data_monthly_subs.to_csv('./datasets/monthly_subs.csv', index=False)
print('Monthly subs')
print(data_monthly_subs.head())

## Product upgrades and downgrades ##
data_upgrades_downgrades = con.execute("SELECT * FROM metric_upgrades_downgrades").fetchdf()
data_upgrades_downgrades.to_csv('./datasets/upgrades_downgrades.csv', index=False)
print('Product Upgrades/Downgrades')
print(data_upgrades_downgrades.head())

con.close() #Closing Connection



### 2. Metric visualisations ###
## 2.1 MRR ##
#Preparing data
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
#Saving
plt.savefig('./images/mrr.png')

## 2.2 Churn ##
#Preparing data
data_churn = data_churn.groupby(['churn_month'])[['starting_revenue','starting_customers','retained_revenue','retained_customers']].sum().reset_index()
data_churn['value_retention']=data_churn['retained_revenue']/data_churn['starting_revenue']
data_churn['volume_retention']=data_churn['retained_customers']/data_churn['starting_customers']
data_churn['value_churn'] = 1 - data_churn['value_retention']
data_churn['volume_churn'] = 1 - data_churn['volume_retention']
data_churn['churn_month']=pd.to_datetime(data_churn['churn_month'])
#Plotting
fig, ax = plt.subplots(figsize=[14,6], ncols=2, nrows=1, sharey=True)
sns.lineplot(data=data_churn, x='churn_month', y='value_churn', ax=ax[0], color='#ff622d', linewidth=2)
sns.lineplot(data=data_churn, x='churn_month', y='volume_churn', ax=ax[1], color='#ff622d', linewidth=2)
#Formatting
for a in ax:
    a.set_xlabel('')
    a.yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
    a.tick_params(axis='x', labelrotation=30)
    a.yaxis.grid(True)
    a.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
ax[0].set_ylabel('Churn rate')
ax[0].set_title('Value Churn')
ax[1].set_title('Volume Churn')
a.yaxis.grid(True)
plt.suptitle('Monthly churn rates',fontsize=15)
plt.tight_layout()
#Saving
plt.savefig('./images/churn.png')

## 2.3 Acquisitions ##
#Preparing data
data_acquired = data_acquired.groupby(['metric_month'])[['customers_acquired','cancelled_customers','customer_base']].sum().reset_index()
data_acquired['metric_month']=pd.to_datetime(data_acquired['metric_month'])
#Plotting
fig, ax = plt.subplots(figsize=[14,6])
sns.lineplot(data=data_acquired, x='metric_month', y='customers_acquired', ax=ax, color="#005b00", linewidth=2, label='Customers acquired')
sns.lineplot(data=data_acquired, x='metric_month', y='cancelled_customers', ax=ax, color="#7a0000", linewidth=2, label='Customers cancelled')
#Formatting
ax.set_xlabel('')
ax.set_ylabel('Number of customers')
ax.set_title('Monthly customer acquisitions and cancellations')
ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
plt.tight_layout()
#Saving
plt.savefig('./images/acquisitions.png')


## 2.4 Subscription products and upgrades/downgrades##
fig, ax = plt.subplots(figsize=[14,6], ncols=2, nrows=1)
# 2.4.1 Subs products #
#Preparing data
data_monthly_subs = data_monthly_subs.groupby(['metric_month','product'])[['no_customers','mrr']].sum().reset_index()
data_monthly_subs['metric_month']=pd.to_datetime(data_monthly_subs['metric_month'])
data_product_proportions = data_monthly_subs.pivot_table(index='metric_month', columns='product', values='no_customers')
data_product_proportions_norm = data_product_proportions.div(data_product_proportions .sum(axis=1), axis=0)
data_arppu=data_monthly_subs.groupby(['metric_month'])[['no_customers','mrr']].sum()
data_arppu['arppu']=data_arppu['mrr']/data_arppu['no_customers']
#Plotting
ax[0].stackplot(
    data_product_proportions_norm.index,
    [data_product_proportions_norm[col] for col in data_product_proportions_norm.columns],
    labels=data_product_proportions_norm.columns,
    alpha=0.8
)
ax_twin=ax[0].twinx()
ax_twin.plot(data_arppu.index, data_arppu['arppu'],color='red')
#Formatting
ax_twin.set_ylabel('ARPPU')
ax_twin.set_ylim(bottom=100, top=200)
ax[0].legend(loc='upper left')
ax[0].xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
ax[0].tick_params(axis='x', labelrotation=30)
ax[0].yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
ax[0].set_title('Distribution of monthly subscribers by product and ARPPU')

# 2.4.2 Upgrades and downgrades #
#Preparing data
data_upgrades_downgrades = data_upgrades_downgrades.groupby(['metric_month','product_outcome'])[['no_customers']].sum().reset_index()
data_upgrades_downgrades['metric_month']=pd.to_datetime(data_upgrades_downgrades['metric_month']).dt.strftime('%b %Y')
#Plotting
sns.barplot(data=data_upgrades_downgrades, x='metric_month', y='no_customers', hue='product_outcome', estimator='sum', ax=ax[1], palette=["green", "red"])
#Formatting
ax[1].set_xlabel('')
ax[1].tick_params(axis='x', labelrotation=90)
ax[1].set_ylabel('No. of customers')
ax[1].set_title('Monthly customers upgrading and downgrading products')
ax[1].legend(title='')
plt.tight_layout()
#Saving
plt.savefig('./images/products.png')