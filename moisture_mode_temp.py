import pandas as pd
from pathlib import Path

# Пути к файлам
input_dir = Path('input')
output_dir = Path('output')

perten_file = input_dir / 'perten_data_v3.csv'
moistures_file = input_dir / 'moistures_temps_v1.csv'
mode_file = input_dir / 'mode_optimized.csv'
output_file = output_dir / 'moistures_temps_mass1.csv'

# Создаем выходную директорию
output_dir.mkdir(parents=True, exist_ok=True)

# Загружаем данные
moistures_df = pd.read_csv(moistures_file)
perten_df = pd.read_csv(perten_file)
mode_df = pd.read_csv(mode_file)

# Преобразование времени в datetime
def parse_time(df):
    return pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%d-%m-%Y %H:%M:%S', errors='coerce')

moistures_df['DateTime'] = parse_time(moistures_df)
perten_df['DateTime'] = parse_time(perten_df)
mode_df['DateTime'] = parse_time(mode_df)

# Очищаем данные от некорректных временных меток
moistures_df = moistures_df.dropna(subset=['DateTime']).sort_values('DateTime').reset_index(drop=True)
perten_df = perten_df.dropna(subset=['DateTime']).sort_values('DateTime').reset_index(drop=True)
mode_df = mode_df.dropna(subset=['DateTime']).sort_values('DateTime').reset_index(drop=True)

# Переименовываем колонки Perten
perten_df = perten_df.rename(columns={
    'Grain': 'perten_Grain',
    '%mois': 'perten_Moisture',
    'TW': 'perten_Nature',
    'Temp': 'perten_Temperature'
})

# Функция для определения статуса зерна
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

# Создаем копию moistures_df для обработки
combined_df = moistures_df.copy()

# Добавляем пустые колонки
for col in ['perten_dry_grain', 'perten_dry_moisture', 'perten_dry_nature', 'perten_dry_temp',
            'perten_wet_grain', 'perten_wet_moisture', 'perten_wet_nature', 'perten_wet_temp', 'mode']:
    combined_df[col] = ''

# Подстановка данных Perten
used_perten = set()

for p_idx, p_row in perten_df.iterrows():
    if p_idx in used_perten:
        continue
    
    available = combined_df[combined_df['perten_dry_moisture'] == '']
    if available.empty:
        break

    time_diff = (available['DateTime'] - p_row['DateTime']).abs()
    closest_idx = time_diff.idxmin()

    status = classify_grain(p_row)
    
    if status in ['dry', 'overdry']:
        combined_df.at[closest_idx, 'perten_dry_grain'] = p_row['perten_Grain']
        combined_df.at[closest_idx, 'perten_dry_moisture'] = p_row['perten_Moisture']
        combined_df.at[closest_idx, 'perten_dry_nature'] = p_row['perten_Nature']
        combined_df.at[closest_idx, 'perten_dry_temp'] = p_row['perten_Temperature']
    else:  
        combined_df.at[closest_idx, 'perten_wet_grain'] = p_row['perten_Grain']
        combined_df.at[closest_idx, 'perten_wet_moisture'] = p_row['perten_Moisture']
        combined_df.at[closest_idx, 'perten_wet_nature'] = p_row['perten_Nature']
        combined_df.at[closest_idx, 'perten_wet_temp'] = p_row['perten_Temperature']

    used_perten.add(p_idx)

# Подстановка данных из mode_optimized.csv
for m_idx, m_row in mode_df.iterrows():
    available = combined_df[combined_df['mode'] == '']
    if available.empty:
        break

    time_diff = (available['DateTime'] - m_row['DateTime']).abs()
    closest_idx = time_diff.idxmin()

    # Выбираем первый True
    mode_columns = ['FILLING', 'DRYING', 'RECYCLING', 'EMPTY', 'SHUTDOWN', 'STOP', 'COOLING', 'MANUAL']
    active_mode = next((col for col in mode_columns if m_row.get(col, False)), '')

    combined_df.at[closest_idx, 'mode'] = active_mode

# Удаляем временной столбец
combined_df = combined_df.drop(columns=['DateTime'])

# Сохраняем результат
combined_df.to_csv(output_file, index=False)
print(f"Данные сохранены в {output_file}")