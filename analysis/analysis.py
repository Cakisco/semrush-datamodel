import duckdb
import pandas as pd
con = duckdb.connect("../semrushpayments/semrushpayments.duckdb")

#Source data
data_source = con.execute("SELECT * FROM src_payments").fetchdf()
print('Source model')
print(data_source.head())

#Staging model
data_staging = con.execute("SELECT * FROM stg_payments").fetchdf()
print('Staging model')
print(data_staging.head())

#Periodised payments
data_periodised = con.execute("SELECT * FROM int_payments_periodised").fetchdf()
print('Periodised payments model')
print(data_periodised.head(10))