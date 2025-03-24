import pandas as pd
from pathlib import Path

# Пути к файлам
input_dir = Path('input')
output_dir = Path('output')

moistures_file = input_dir / 'moistures_temps_v1.csv'
perten_file = input_dir / 'perten_data_v3.csv'
mode_file = input_dir / 'mode_optimized.csv'
output_file = output_dir / 'moistures_temps_mass.csv'

# Создаем выходную директорию
output_dir.mkdir(parents=True, exist_ok=True)

# Загружаем данные
moistures_df = pd.read_csv(moistures_file)
perten_df = pd.read_csv(perten_file)
mode_df = pd.read_csv(mode_file)

# Функция преобразования даты и времени
def parse_time(df):
    return pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%d-%m-%Y %H:%M:%S', errors='coerce')

# Преобразуем дату-время
moistures_df['DateTime'] = parse_time(moistures_df)
perten_df['DateTime'] = parse_time(perten_df)
mode_df['DateTime'] = parse_time(mode_df)

# Удаляем некорректные даты и сортируем
moistures_df = moistures_df.dropna(subset=['DateTime']).sort_values('DateTime').reset_index(drop=True)
perten_df = perten_df.dropna(subset=['DateTime']).sort_values('DateTime').reset_index(drop=True)
mode_df = mode_df.dropna(subset=['DateTime']).sort_values('DateTime').reset_index(drop=True)

# Переименовываем колонки в perten_df
perten_df = perten_df.rename(columns={
    'Grain': 'perten_Grain',
    '%mois': 'perten_Moisture',
    'TW': 'perten_Nature',
    'Temp': 'perten_Temperature'
})

# Функция классификации зерна
def classify_grain(row):
    grain = row['perten_Grain'].lower() if isinstance(row['perten_Grain'], str) else ''
    moisture = row['perten_Moisture']

    if grain == 'raps':
        if moisture > 9.5:
            return 'wet'
        elif 7 <= moisture <= 9.5:
            return 'dry'
        else:
            return 'overdry'
    else:
        if moisture > 14.5:
            return 'wet'
        elif 12 <= moisture <= 14.5:
            return 'dry'
        else:
            return 'overdry'

# Создаем копию moistures_df
combined_df = moistures_df.copy()

# Добавляем колонки для сухого и влажного зерна
for col in ['Grain', 'Moisture', 'Nature', 'Temperature']:
    combined_df[f'perten_dry_{col}'] = ''
    combined_df[f'perten_wet_{col}'] = ''

combined_df['tested'] = ''  # Добавляем колонку tested

# Отслеживаем использованные индексы
used_perten = set()

# Подстановка данных Perten
for p_idx, p_row in perten_df.iterrows():
    if p_idx in used_perten:
        continue

    available = combined_df[combined_df['perten_dry_Grain'] == '']
    if available.empty:
        break

    time_diff = (available['DateTime'] - p_row['DateTime']).abs()
    closest_idx = time_diff.idxmin()

    grain_status = classify_grain(p_row)

    if grain_status in ['dry', 'overdry']:
        combined_df.at[closest_idx, 'perten_dry_Grain'] = p_row['perten_Grain']
        combined_df.at[closest_idx, 'perten_dry_Moisture'] = p_row['perten_Moisture']
        combined_df.at[closest_idx, 'perten_dry_Nature'] = p_row['perten_Nature']
        combined_df.at[closest_idx, 'perten_dry_Temperature'] = p_row['perten_Temperature']
    else:  # wet
        combined_df.at[closest_idx, 'perten_wet_Grain'] = p_row['perten_Grain']
        combined_df.at[closest_idx, 'perten_wet_Moisture'] = p_row['perten_Moisture']
        combined_df.at[closest_idx, 'perten_wet_Nature'] = p_row['perten_Nature']
        combined_df.at[closest_idx, 'perten_wet_Temperature'] = p_row['perten_Temperature']

    combined_df.at[closest_idx, 'tested'] = 'real'
    used_perten.add(p_idx)

# Заполнение пропущенных значений интерполяцией
for col in ['perten_dry_Moisture', 'perten_dry_Nature', 'perten_dry_Temperature',
            'perten_wet_Moisture', 'perten_wet_Nature', 'perten_wet_Temperature']:
    combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
    combined_df[col] = combined_df[col].interpolate(method='linear', limit_direction='both')

# Округляем до 3 знаков после запятой
for col in ['perten_dry_Moisture', 'perten_dry_Nature', 'perten_dry_Temperature',
            'perten_wet_Moisture', 'perten_wet_Nature', 'perten_wet_Temperature']:
    combined_df[col] = combined_df[col].round(3)

# Заполняем `tested` у интерполированных значений
combined_df['tested'] = combined_df['tested'].fillna('calculated')

# Добавляем колонку `mode`
combined_df = combined_df.merge(mode_df[['DateTime', 'FILLING', 'DRYING', 'RECYCLING', 'EMPTY',
                                         'SHUTDOWN', 'STOP', 'COOLING', 'MANUAL']],
                                on='DateTime', how='left')

# Определяем `mode` по первому `True`
def get_mode(row):
    for col in ['FILLING', 'DRYING', 'RECYCLING', 'EMPTY', 'SHUTDOWN', 'STOP', 'COOLING', 'MANUAL']:
        if row[col] is True:
            return col
    return ''

combined_df['mode'] = combined_df.apply(get_mode, axis=1)

# Удаляем временные столбцы mode-файла
combined_df.drop(columns=['FILLING', 'DRYING', 'RECYCLING', 'EMPTY', 'SHUTDOWN', 'STOP', 'COOLING', 'MANUAL'],
                 inplace=True)

# Вычисляем массу drop (nature * 497.2)
combined_df['drop_mass'] = (combined_df['perten_dry_Nature'] * 497.2).round(3)

# Удаляем `DateTime` перед сохранением
combined_df = combined_df.drop(columns=['DateTime'])

# Сохраняем результат
combined_df.to_csv(output_file, index=False)
print(f"Данные сохранены в {output_file}")