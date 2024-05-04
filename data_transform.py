# import libs
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource, HoverTool, LinearAxis, Range1d, Legend
from bokeh.io import output_notebook
from bokeh.palettes import Category10
from bokeh.layouts import gridplot

current_directory = os.path.dirname(os.path.abspath(__file__))
df_market_data_head = pd.read_csv(os.path.join(current_directory, r'data\out\market_data_head.csv'))
df_market_data_ts = pd.read_csv(os.path.join(current_directory, r'data\out\market_data_ts.csv'))
df_market_prices_head = pd.read_csv(os.path.join(current_directory, r'data\out\market_prices_head.csv'))
df_market_price_ts = pd.read_csv(os.path.join(current_directory, r'data\out\market_prices_ts.csv'))
df_portfolio_data_head = pd.read_csv(os.path.join(current_directory, r'data\out\portfolio_data_head.csv'))
df_portfolio_data_ts = pd.read_csv(os.path.join(current_directory, r'data\out\portfolio_data_ts.csv'))

dfs = [df_market_data_head, df_market_data_ts, df_market_prices_head, df_market_price_ts, df_portfolio_data_head, df_portfolio_data_ts]

transform_dict = {'df_market_data_head': ['head', df_market_data_head, ['timestamp']],
                   'df_market_data_ts': ['ts', df_market_data_ts ,['timestamp', 'delivery_start']],
                   'df_market_prices_head': ['head', df_market_prices_head,['timestamp']],
                   'df_market_price_ts': ['ts', df_market_price_ts,['timestamp', 'delivery_start']],
                   'df_portfolio_data_head': ['head', df_portfolio_data_head, ['timestamp']],
                   'df_portfolio_data_ts': ['ts', df_portfolio_data_ts, ['timestamp', 'delivery_datetime']]
                    }

tables = list(transform_dict.keys())

# func if col from datetime_tseries exist in dataframe then convert it to datetime and trim it so it contains just data Y-1 and Y+0
# data for Y-0 are not available because extractors were not created yet. Most current timeseries ends at datetime 31.12.2023 23:00:00
def df_dt_transform(df_name, df_type, df, dt_cols):
    # string to datetime
    for dt_col in dt_cols:
        df[dt_col] = pd.to_datetime(df[dt_col], utc=True).dt.tz_convert('Europe/Prague')
        print(f" Column '{dt_col}' from dataframe '{df_name}' was converted to datetime.")
    # trimm data in table of type ts, to contain data for Y-1 and Y+0
    if df_type == 'ts':
        first_hour_last_year = pytz.timezone('Europe/Prague').localize(datetime(datetime.now().year - 1, 1, 1, 0, 0, 0))
        last_hour_this_year = pytz.timezone('Europe/Prague').localize(datetime.now())
        df = df[(df[dt_cols[1]] >= first_hour_last_year) & (df[dt_cols[1]] <= last_hour_this_year)]
    elif df_type == 'head':
        pass
    else:
        print(f"Error: {df_type} in {df_name} is not specified correctly.")
    return df

for table in tables:
    transform_dict[table][1] = df_dt_transform(table, transform_dict[table][0], transform_dict[table][1], transform_dict[table][2])

for i, value in enumerate(list(transform_dict.items())):
    dfs[i] = list(transform_dict.items())[i][1][1]
del transform_dict

df_market_data_head, df_market_data_ts, df_market_prices_head, df_market_price_ts, df_portfolio_data_head, df_portfolio_data_ts = dfs
del dfs

# Vyfiltrování pouze relevantních dat
df_market_data_head = df_market_data_head[df_market_data_head['type'] == 'current']
df_market_prices_head = df_market_prices_head[(df_market_prices_head['commodity'] == 'EE') &
                                            (df_market_prices_head['name'].isin(['imbalance', 'counter_imbalance']))]

df_portfolio_data_head = df_portfolio_data_head[(df_portfolio_data_head['commodity'] == 'EE') &
                                                (df_portfolio_data_head['type'].isin(['Aktualni', 'prediction'])) &
                                                (df_portfolio_data_head['name'].isin(['spotreba_a+b+ztraty',
                                                                                      'vspot_b2b',
                                                                                      'spotreba_c',
                                                                                      'vspot_b2c',
                                                                                      'vyroba_a+b+c',
                                                                                      'vspot_b2b_prod',
                                                                                      'kladna_odchylka_mnozstvi',
                                                                                      'zaporna_odchylka_mnozstvi']))]

df_market_data = pd.merge(df_market_data_head, df_market_data_ts, on='id', how='inner', suffixes=('_head', '_ts'))
df_market_prices = pd.merge(df_market_prices_head, df_market_price_ts, on='id', how='inner', suffixes=('_head', '_ts'))

df_portfolio_data = pd.merge(df_portfolio_data_head, df_portfolio_data_ts, on='id', how='inner', suffixes=('_head', '_ts'))
df_portfolio_data = df_portfolio_data.rename(columns={'delivery_datetime': 'delivery_start'})

# Pivoting and joining tables
dfs_merged = [df_market_data, df_market_prices, df_portfolio_data]
for i, df in enumerate(dfs_merged):
    dfs_merged[i] = dfs_merged[i].pivot_table(index='delivery_start',
                                 columns = 'name',
                                 values='value',
                                 aggfunc='max')
    dfs_merged[i].columns.name = None

df_market_data, df_market_prices, df_portfolio_data = dfs_merged
del dfs_merged

# change sign for 'zaporna_odchylka_mnozstvi'
df_portfolio_data['zaporna_odchylka_mnozstvi'] = -1*df_portfolio_data['zaporna_odchylka_mnozstvi']

df_data = df_market_data.join([df_market_prices, df_portfolio_data], how='inner')
df_data = df_data.rename(columns={'spotreba_a+b+ztraty': 'metering_b2b',
                                  'spotreba_c': 'metering_b2c',
                                  'vyroba_a+b+c': 'metering_prod',
                                  'vspot_b2b_prod': 'vspot_prod'})
df_data = df_data.fillna(0)

# Výpočet odchylek pro jednotlivé segmenty
segments = ['_b2b', '_b2c', '_prod']
segments_dev = ['deviation' + s for s in segments]
segments_met = ['metering' + s for s in segments]
segments_vspot = ['vspot' + s for s in segments]

for segment_dev, segments_met, segment_vspot in zip(segments_dev, segments_met, segments_vspot):
    df_data[segment_dev] = df_data[segment_vspot] - df_data[segments_met]

df_data['pnl_balancing_portfolio'] = np.where(np.sign(df_data['kladna_odchylka_mnozstvi']) == np.sign(df_data['system_imbalance']),
                                              df_data['kladna_odchylka_mnozstvi'] * df_data['imbalance'],
                                              df_data['kladna_odchylka_mnozstvi'] * df_data['counter_imbalance']
                                             ) + \
                                     np.where(np.sign(df_data['zaporna_odchylka_mnozstvi']) == np.sign(df_data['system_imbalance']),
                                              df_data['zaporna_odchylka_mnozstvi'] * df_data['imbalance'],
                                              df_data['zaporna_odchylka_mnozstvi'] * df_data['counter_imbalance'])

pnl_bal_segments = ['pnl_balancing' + s for s in segments]

for pnl_bal_segment, segment_dev in zip(pnl_bal_segments, segments_dev):
    df_data[pnl_bal_segment] = np.where(np.sign(df_data[segment_dev]) == np.sign(df_data['system_imbalance']),
                                        df_data[segment_dev] * df_data['imbalance'],
                                        df_data[segment_dev] * df_data['counter_imbalance']
                                       )


df_data['odchylka_mnozstvi'] = df_data['kladna_odchylka_mnozstvi'] + df_data['zaporna_odchylka_mnozstvi']

# Vyhodncení po měsících - tabulka
pnl_bal_segments.append('pnl_balancing_portfolio')
df_pnl_monthly = df_data[pnl_bal_segments].resample('M').sum()

################################### GRAFY DEFINICE ####################################################################

#TODO predelat do plotly a pak do streamlitu
def monthly_sums(df, column):
    """Function to create bar chart for monthly sums of pnl_balancing
    :param df: dataframe
    :param column: string, name of the column to plot
    """
    df_plot = df_data[pnl_bal_segments].resample('M').sum()[column].reset_index()
    source = ColumnDataSource(df_plot)

    # Create a figure
    p = figure(x_axis_type='datetime', plot_width=900, plot_height=350, title=column,
               toolbar_location=None, tools="")

    # Add bars
    p.vbar(x='delivery_start', top=column, width=timedelta(days=20), source=df_plot,
           fill_color=Category10[3][0])

    # Customize the plot
    p.xaxis.axis_label = 'Delivery Start'
    p.yaxis.axis_label = column
    p.xgrid.grid_line_color = None
    p.xaxis.major_label_standoff = 5

    hover = HoverTool(tooltips=[('date', '@delivery_start{%Y-%m}'), ('Value', f'@{column}')],
                      formatters={'@delivery_start': 'datetime'}, mode='vline')
    p.add_tools(hover)

    # Show the plot
    show(p)

# Graf v hodinovém detailu
def hourly_ts_plots(df, columns, units, dt_from, dt_to):
    """Function to create Hourly time series plots
    :param df: dataframe
    :param columns: list of columns names to plot
    :param dt_from: beging of time serie to plot, string in format '%Y-%m-%d %H:%M:%S%z'
    :param dt_to: end of time serie to plot, string in format '%Y-%m-%d %H:%M:%S%z'
    """
    from bokeh.palettes import Category10
    df_plot = df[columns]
    source = ColumnDataSource(data=df_plot.loc[dt_from:dt_to, :])

    # Create a list to store figures and HoverTools
    figures = []
    hovertips = []

    # Define colors from Category20 palette
    colors = Category10[len(columns)]

    # Create figures and HoverTools dynamically
    for column, color, unit in zip(columns, colors, units):
        # Create a new figure
        fig = figure(x_axis_type="datetime", plot_width=900, plot_height=200,
                     title=f"Hourly Time Series - {column} {unit}")

        # Plot the data with respective color
        fig.line(x='delivery_start', y=column, source=source, legend_label=column, color=color)

        # Create HoverTool
        hover = HoverTool(tooltips=[("Time", "@delivery_start{%F %H:%M}"), ("Value", f"@{column}")],
                          formatters={"@delivery_start": "datetime"}, mode='vline')

        # Add HoverTool to the figure
        fig.add_tools(hover)

        # Add the figure to the list
        figures.append(fig)
        hovertips.append(hover)

        # axis names
        fig.xaxis.axis_label = 'Delivery Start'

    # Combine the plots into a grid
    grid = gridplot([[fig] for fig in figures])

    # Set the legend location outside the plot area
    for fig in figures:
        fig.legend.visible = False

    # Show the grid
    show(grid)

############################## NASTAVENI PARAMETRU A SPUSTENI FUNKCI PRO TOVRBU GRAFU ########################################
#TODO predelat aby bylo ovladano prvky ve streamlitu
cols_b2c = ['pnl_balancing_b2c', 'deviation_b2c', 'system_imbalance','imbalance', 'counter_imbalance']
cols_b2b = ['pnl_balancing_b2b', 'deviation_b2b', 'system_imbalance','imbalance', 'counter_imbalance']
cols_prod = ['pnl_balancing_prod', 'deviation_prod', 'system_imbalance','imbalance', 'counter_imbalance']
cols_portfolio = ['pnl_balancing_portfolio', 'odchylka_mnozstvi', 'system_imbalance','imbalance', 'counter_imbalance']

units = ['[CZK]', '[MWh]', '[MWh]','[CZK/MWH]','[CZK/MWH]']
dt_from = '2024-01-01 00:00:00+01:00'
dt_to = '2024-01-31 00:00:00+01:00'
hourly_ts_plots(df_data, cols_portfolio, units, dt_from, dt_to)

cols_b2c = ['pnl_balancing_b2c', 'deviation_b2c', 'system_imbalance','imbalance', 'counter_imbalance']
units = ['[CZK]', '[MWh]', '[MWh]','[CZK/MWH]','[CZK/MWH]']
dt_from = '2024-01-01 00:00:00+01:00'
dt_to = '2024-01-31 00:00:00+01:00'

hourly_ts_plots(df_data, cols_b2c, units, dt_from, dt_to)