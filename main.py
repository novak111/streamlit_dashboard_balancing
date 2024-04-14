import os
import pandas as pd
import data_prep

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
    f_head, f_ts = data_prep.parse_ote_ts_data(df, float_col_names, file)
    if first:
        mode_param = 'w'
    else:
        mode_param = 'a'
    f_head.to_csv(out_head_file, index=False, mode=mode_param, header=first, sep=',')
    f_ts.to_csv(out_ts_file, index=False, mode=mode_param, header=first, sep=',')

    print(f'File: {file} parsed and saved')

# current
df = data_prep.create_current_version_df(file_names, data_in_folder)
df.columns = [col.lower() for col in df.columns]
f_head, f_ts = data_prep.parse_ote_ts_data(df, float_col_names, 'current')
f_head.to_csv(out_head_file, index=False, mode='a', header=first, sep=',')
f_ts.to_csv(out_ts_file, index=False, mode='a', header=first, sep=',')

print('ote_imbalances_current parsed and save')