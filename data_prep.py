# import libs
import os
import re
import hashlib
import pandas as pd

#TODO: add if __main__

# file and col names
column_names_sys_imbal = {'ote_odchylky_v0.csv': [
    'den',
    'hodina',
    'systemova_odchylka_mwh'],
    'ote_odchylky_v1.csv': [
        'den',
        'hodina',
        'systemova_odchylka_mwh'],
    'ote_odchylky_v2.csv': [
        'den',
        'hodina',
        'systemova_odchylka_mwh'],
}
source_id_to_name = {'systemova_odchylka_mwh': 'system_imbalance'}
source_id_to_unit = {'systemova_odchylka_mwh': 'Mwh'}

float_col_names = list(source_id_to_name.keys())
file_names = list(column_names_sys_imbal.keys())


# functions
def create_head_table():
    """ Function to create head table
    :return: dataframe
    """
    return pd.DataFrame(columns=['id',
                                 'source_id',
                                 'name',
                                 'balance_zone',
                                 'subzone',
                                 'class',
                                 'type',
                                 'source',
                                 'unit',
                                 'granularity'
                                 ])


def parse_ote_ts_data(df_ts, col_names, file_name):
    """ Function to fill head and time series table
    :param df_ts: dataframe
    :param col_names: column names to be imported to time series table into "value" column
    :param file_name: csv file name
    :return: dataframes
    """

    head = create_head_table()

    ote_version_name = 'V' + str(*re.findall(r'\d+', file_name)) if file_name in file_names else file_name

    head['source_id'] = float_col_names
    head.loc[:, 'name'] = [source_id_to_name[col_n] for col_n in float_col_names]
    head.loc[:, 'balance_zone'] = 'OTE'
    head.loc[:, 'subzone'] = 'OTE'
    head.loc[:, 'class'] = ''
    head.loc[:, 'type'] = ote_version_name
    head.loc[:, 'source'] = 'OTE'
    head.loc[:, 'unit'] = [source_id_to_unit[col_n] for col_n in float_col_names]
    head.loc[:, 'granularity'] = 'H',
    head.loc[:, 'id'] = head.apply(lambda x: hashlib.md5((x['name'] + x['balance_zone'] + x['subzone'] + x['class'] + x[
        'type'] + x['source'] + x['granularity']).encode()).hexdigest(), axis=1)

    # fill TS table
    if file_name == 'current':
        datetime_index = df_ts['delivery_start']
    else:
        datetime_index = pd.date_range(start=pd.to_datetime(df_ts['den'], format='%d.%m.%Y').min(),
                                       periods=len(df_ts['den']), freq='h', tz='Europe/Prague')
    df_ts['delivery_start'] = datetime_index
    ts = df_ts[['delivery_start'] + float_col_names].set_index(['delivery_start']).stack().reset_index()
    ts.columns = ['delivery_start', 'source_id', 'value']
    ts = ts.merge(head[['id', 'source_id']], on=['source_id'], how='left')[
        ['id', 'delivery_start', 'value']].sort_values('delivery_start')
    return head, ts


def create_current_version_df(filenames, input_folder):
    """ Function to create df with most current values from v0, v1, v2
    :param filenames: list of csv files names (v0, v1, v2)
    :return: dataframe
    """
    df_list = []
    for i, file in enumerate(filenames):
        file_path = os.path.join(input_folder, file)
        df_temp = pd.read_csv(file_path, delimiter=';', usecols=column_names_sys_imbal[file])
        datetime_index = pd.date_range(start=pd.to_datetime(df_temp['den'], format='%d.%m.%Y').min(),
                                       periods=len(df_temp['den']), freq='h', tz='Europe/Prague')
        df_temp['delivery_start'] = datetime_index
        df_temp = df_temp.drop(columns=['den', 'hodina'])
        df_list.append(df_temp)
    del df_temp

    # Merge da taframe
    merged_df = pd.merge(df_list[0], df_list[1], on=['delivery_start'], how='outer', suffixes=('_v0', '_v1'))
    merged_df = pd.merge(merged_df, df_list[2], on=['delivery_start'], how='outer')
    merged_df.set_index(['delivery_start'], inplace=True)
    merged_df.sort_index(inplace=True)

    # Create current_version dataframe
    for col in float_col_names:
        merged_df = merged_df.rename(columns={col: col + '_v2'})
        merged_df[col + '_current'] = [v2 if not pd.isna(v2) else v1 if not pd.isna(v1) else v0
                                       for v0, v1, v2 in
                                       zip(merged_df[col + '_v0'], merged_df[col + '_v1'], merged_df[col + '_v2'])]
    merged_df.reset_index(inplace=True)

    current_version_df = pd.DataFrame(columns=['delivery_start'])

    current_version_df[['delivery_start']] = pd.DataFrame(merged_df[['delivery_start']])

    for col in float_col_names:
        current_version_df[col] = merged_df[col + '_current']
    return current_version_df


#TODO: add if __main__

# Main
current_directory = os.path.dirname(os.path.abspath(__file__))
out_head_file = os.path.join(current_directory, 'data\out\market_data_head.csv')
out_ts_file = os.path.join(current_directory, 'data\out\market_data_ts.csv')
data_in_folder = os.path.join(current_directory, r'data\in\\')

# v0, v1, v2
for i, file in enumerate(column_names_sys_imbal.keys()):
    first = i == 0
    file_path = os.path.join(data_in_folder, file)
    df = pd.read_csv(file_path, delimiter=';', usecols=column_names_sys_imbal[file])
    df.columns = [col.lower() for col in df.columns]
    f_head, f_ts = parse_ote_ts_data(df, float_col_names, file)
    if first:
        mode_param = 'w'
    else:
        mode_param = 'a'
    f_head.to_csv(out_head_file, index=False, mode=mode_param, header=first, sep=',')
    f_ts.to_csv(out_ts_file, index=False, mode=mode_param, header=first, sep=',')

    print(f'File: {file} parsed and saved')

# current
df = create_current_version_df(file_names, data_in_folder)
df.columns = [col.lower() for col in df.columns]
f_head, f_ts = parse_ote_ts_data(df, float_col_names, 'current')
f_head.to_csv(out_head_file, index=False, mode='a', header=first, sep=',')
f_ts.to_csv(out_ts_file, index=False, mode='a', header=first, sep=',')

print('ote_imbalances_current parsed and save')
