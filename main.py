# import os
# import pandas as pd
import data_prep
import data_transform

# runs preparation of system imbalance data
data_prep.main()

df_data = data_transform.main()

# Setting of paramaters for plots creation
# TODO Monthly evaluation table
df_pnl_monthly = data_transform.pnl_monthly(df_data)

# TODO Monthly evaluation table plot
#monthly_sums(df, column)

#TODO predelat aby bylo ovladano prvky ve streamlitu
cols_b2c = ['pnl_balancing_b2c', 'deviation_b2c', 'system_imbalance','imbalance', 'counter_imbalance']
cols_b2b = ['pnl_balancing_b2b', 'deviation_b2b', 'system_imbalance','imbalance', 'counter_imbalance']
cols_prod = ['pnl_balancing_prod', 'deviation_prod', 'system_imbalance','imbalance', 'counter_imbalance']
cols_portfolio = ['pnl_balancing_portfolio', 'odchylka_mnozstvi', 'system_imbalance','imbalance', 'counter_imbalance']

units = ['[CZK]', '[MWh]', '[MWh]','[CZK/MWH]','[CZK/MWH]']
dt_from = '2023-01-01 00:00:00+01:00'
dt_to = '2023-01-31 00:00:00+01:00'
data_transform.hourly_ts_plots(df_data, cols_portfolio, units, dt_from, dt_to)

cols_b2c = ['pnl_balancing_b2c', 'deviation_b2c', 'system_imbalance', 'imbalance', 'counter_imbalance']
units = ['[CZK]', '[MWh]', '[MWh]','[CZK/MWH]','[CZK/MWH]']
dt_from = '2023-01-01 00:00:00+01:00'
dt_to = '2023-01-31 00:00:00+01:00'

data_transform.hourly_ts_plots(df_data, cols_b2c, units, dt_from, dt_to)