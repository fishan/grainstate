import pandas as pd
from pathlib import Path
from datetime import timedelta

# Пути к файлам
input_dir = Path('input')
output_dir = Path('output')

moistures_file = input_dir / 'moistures_temps_v1.csv'
perten_file = input_dir / 'perten_data_v3.csv'
mode_file = input_dir / 'mode_optimized.csv'
output_file = output_dir / 'moistures_temps_mass3.csv'

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
        return 'wet' if moisture > 9.5 else 'dry' if 7 <= moisture <= 9.5 else 'overdry'
    else:
        return 'wet' if moisture > 14.5 else 'dry' if 12 <= moisture <= 14.5 else 'overdry'

# Создаем копию moistures_df
combined_df = moistures_df.copy()

# Добавляем колонки perten
for col in ['Grain', 'Moisture', 'Nature', 'Temperature']:
    combined_df[f'perten_dry_{col}'] = ''
    combined_df[f'perten_wet_{col}'] = ''
combined_df['tested'] = ''
combined_df['dry_mass'] = 0.0

# Отслеживаем использованные индексы и последние данные
used_perten = set()
last_grain = None
last_time = None

# Подстановка данных Perten
for p_idx, p_row in perten_df.iterrows():
    if p_idx in used_perten:
        continue

    available = combined_df[combined_df['perten_dry_Grain'] == '']
    if available.empty:
        break

    time_diff = (available['DateTime'] - p_row['DateTime']).abs()
    closest_idx = time_diff.idxmin()

    # Проверка на 2 часа между разными типами зерна
    if (last_grain and last_grain != p_row['perten_Grain'].lower() and 
        last_time and (p_row['DateTime'] - last_time) < timedelta(hours=2)):
        continue

    grain_status = classify_grain(p_row)
    prefix = 'perten_dry' if grain_status in ['dry', 'overdry'] else 'perten_wet'

    combined_df.at[closest_idx, f'{prefix}_Grain'] = p_row['perten_Grain']
    combined_df.at[closest_idx, f'{prefix}_Moisture'] = p_row['perten_Moisture']
    combined_df.at[closest_idx, f'{prefix}_Nature'] = p_row['perten_Nature']
    combined_df.at[closest_idx, f'{prefix}_Temperature'] = p_row['perten_Temperature']
    combined_df.at[closest_idx, 'tested'] = 'real'

    used_perten.add(p_idx)
    last_grain = p_row['perten_Grain'].lower()
    last_time = p_row['DateTime']

# Интерполяция числовых значений
for col in ['perten_dry_Moisture', 'perten_dry_Nature', 'perten_dry_Temperature',
            'perten_wet_Moisture', 'perten_wet_Nature', 'perten_wet_Temperature']:
    combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
    combined_df[col] = combined_df[col].interpolate(method='linear', limit_direction='both').round(3)

# Заполняем 'tested' как 'calculated' для интерполированных значений
combined_df['tested'] = combined_df['tested'].replace('', 'calculated')

# Расчёт массы для всех строк с dry/overdry и изменением DROPS_SCORE
for idx in combined_df.index:
    if idx == 0:  # Пропускаем первую строку, так как нет предыдущего drop
        continue
    
    current_drop = combined_df.at[idx, 'DROPS_SCORE']
    prev_drop = combined_df.at[idx - 1, 'DROPS_SCORE']
    moisture = combined_df.at[idx, 'perten_dry_Moisture']
    nature = combined_df.at[idx, 'perten_dry_Nature']
    grain = combined_df.at[idx, 'perten_dry_Grain'].lower() if combined_df.at[idx, 'perten_dry_Grain'] else ''

    # Проверка условий: dry/overdry, изменение drop, не ноль
    is_dry_or_overdry = (grain == 'raps' and 0 < moisture <= 9.5) or (grain != 'raps' and 0 < moisture <= 14.5)
    if (is_dry_or_overdry and prev_drop is not None and current_drop != prev_drop and current_drop != 0 and nature):
        combined_df.at[idx, 'dry_mass'] = round(nature * 497.2, 3)

# Заполняем GRAIN_TYPE на основе perten_dry_Grain и perten_wet_Grain с учётом непрерывности
def determine_grain_type(row):
    if row['perten_dry_Grain']:
        return row['perten_dry_Grain']
    elif row['perten_wet_Grain']:
        return row['perten_wet_Grain']
    return None  # Если нет данных, временно ставим None

# Применяем функцию для получения базовых значений
combined_df['GRAIN_TYPE_temp'] = combined_df.apply(determine_grain_type, axis=1)

# Заполняем GRAIN_TYPE с учётом непрерывности (ffill)
combined_df['GRAIN_TYPE'] = combined_df['GRAIN_TYPE_temp'].fillna(method='ffill').fillna('')

# Удаляем временный столбец
combined_df.drop(columns=['GRAIN_TYPE_temp'], inplace=True)

# Объединяем с mode_df по DateTime
combined_df = combined_df.merge(
    mode_df[['DateTime', 'FILLING', 'DRYING', 'RECYCLING', 'EMPTY', 'SHUTDOWN', 'STOP', 'COOLING', 'MANUAL']],
    on='DateTime', how='left'
)

# Функция для определения режима
def get_mode(row):
    mode_columns = ['FILLING', 'DRYING', 'RECYCLING', 'EMPTY', 'SHUTDOWN', 'STOP', 'COOLING', 'MANUAL']
    for col in mode_columns:
        if row[col] is True:
            return col
    return ''

# Добавляем столбец mode
combined_df['mode'] = combined_df.apply(get_mode, axis=1)

# Удаляем временные столбцы из mode_df
combined_df.drop(columns=['FILLING', 'DRYING', 'RECYCLING', 'EMPTY', 'SHUTDOWN', 'STOP', 'COOLING', 'MANUAL'], inplace=True)

# Определяем порядок колонок
column_order = [
    'Date', 'Time', 'GRAIN_TYPE', 'DROPS_SCORE', 'ACTUAL_BURNERS_TEMP', 'TOP_TEMP', 'MID_TEMP', 'BOTTOM_TEMP',
    'DRY_MOISTURE', 'DRY_TEMP', 'DRY_NATURE', 'WET_MOISTURE', 'WET_TEMP', 'WET_NATURE',
    'perten_dry_Moisture', 'perten_dry_Nature', 'perten_dry_Temperature',
    'perten_wet_Moisture', 'perten_wet_Nature', 'perten_wet_Temperature',
    'tested', 'dry_mass', 'mode'
]

# Сохраняем результат с заданным порядком колонок
combined_df.drop(columns=['DateTime'])[column_order].to_csv(output_file, index=False)
print(f"Данные сохранены в {output_file}")