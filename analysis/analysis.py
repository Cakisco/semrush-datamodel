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