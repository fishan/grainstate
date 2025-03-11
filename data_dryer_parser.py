import pandas as pd
import re

# Путь к вашему файлу (замените на актуальный путь)
input_file_path = 'dryer_data.csv'
output_dir = 'output'

# Прочитайте файл с текущим разделителем (запятая)
df = pd.read_csv(input_file_path)

# Удалите столбец measured, если он существует
if 'measured' in df.columns:
    df = df.drop('measured', axis=1)

# Объедините строки с одинаковым временем в одну строку
data_dict = {}
for index, row in df.iterrows():
    timestamp = row['timestamp']
    if timestamp > '2024-08-30 14:22:53.979':
        continue  # Пропустите записи после указанного времени
    
    var_name = row['var_name']
    var_data = row['var_data']
    
    if timestamp not in data_dict:
        data_dict[timestamp] = {}
    
    data_dict[timestamp][var_name] = var_data

df = pd.DataFrame(data_dict).T

# Добавьте столбец timestamp как обычный столбец
df['timestamp'] = df.index

# Переставьте столбцы так, чтобы timestamp был первым
cols = ['timestamp'] + [col for col in df.columns if col != 'timestamp']
df = df[cols]

# Преобразуйте формат времени PT40S в секунды
def convert_time_to_seconds(x):
    if isinstance(x, str) and x.startswith('PT'):
        match = re.search(r'(\d+)M', x)
        minutes = 0
        if match:
            minutes = int(match.group(1))
        
        match = re.search(r'(\d+)S', x)
        seconds = 0
        if match:
            seconds = int(match.group(1))
        
        return minutes * 60 + seconds
    return x

for col in df.columns:
    if col != 'timestamp':
        df[col] = df[col].apply(convert_time_to_seconds)

# Разделите данные по разным файлам
# temps.csv
temps_df = df[['timestamp', 'DROPS_SCORE', 'SET_BURNERS_TEMP', 'ACTUAL_BURNERS_TEMP', 'TOP_TEMP', 'MID_TEMP', 'BOTTOM_TEMP']]
temps_df.to_csv(f'{output_dir}/temps.csv', sep=',', index=False)

# moistures.csv
moistures_df = df[['timestamp', 'GRAIN_TYPE', 'DROPS_SCORE', 'DRY_MOISTURE', 'DRY_TEMP', 'DRY_NATURE', 'WET_MOISTURE', 'WET_TEMP', 'WET_NATURE']]
moistures_df.to_csv(f'{output_dir}/moistures.csv', sep=',', index=False)

# moistures_temps.csv
moistures_temps_df = df[['timestamp', 'GRAIN_TYPE', 'DROPS_SCORE', 'ACTUAL_BURNERS_TEMP', 'TOP_TEMP', 'MID_TEMP', 'BOTTOM_TEMP', 'DRY_MOISTURE', 'DRY_TEMP', 'DRY_NATURE', 'WET_MOISTURE', 'WET_TEMP', 'WET_NATURE']]

# Удалите повторяющиеся значения DROPS_SCORE
moistures_temps_df = moistures_temps_df.drop_duplicates(subset='DROPS_SCORE', keep='first')

moistures_temps_df.to_csv(f'{output_dir}/moistures_temps.csv', sep=',', index=False)

# settings.csv
settings_df = df[['timestamp', 'DROPS_SCORE', 'DROPS_SET_TIMER', 'SET_BURNERS_TEMP', 'COOLING_TIME', 'BOTTOM_TEMP_LIMIT', 'UPPER_FAN_SET_HZ', 'LOWER_FAN_SET_HZ', 'MID_TEMP_LIMIT']]
settings_df.to_csv(f'{output_dir}/settings.csv', sep=',', index=False)

# mode.csv
mode_df = df[['timestamp', 'DROPS_SCORE', 'FILLING', 'DRYING', 'RECYCLING', 'EMPTY', 'SHUTDOWN', 'STOP', 'COOLING', 'MANUAL']]
mode_df.to_csv(f'{output_dir}/mode.csv', sep=',', index=False)

# alarms.csv
alarms_df = df[['timestamp', 'DROPS_SCORE', 'MIDDLE_LEVEL_ALARM', 'HIGH_LEVEL_ALARM', 'BURNER_HIGH_ALARM', 'HOPPER_FULL_ALARM', 'LOW_AIR_PRESSURE_ALARM', 'GENERAL_ALARM', 'AIR_OVERHEATED']]
alarms_df.to_csv(f'{output_dir}/alarms.csv', sep=',', index=False)

print("Данные успешно разделены и сохранены в отдельные файлы.")
