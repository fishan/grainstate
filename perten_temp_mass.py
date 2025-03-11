import pandas as pd
from pathlib import Path

# Пути к файлам
input_dir = Path('input')
output_dir = Path('output')
perten_file = input_dir / 'perten_data_v3.csv'
moistures_file = input_dir / 'moistures_temps_v1.csv'
output_file = output_dir / 'moistures_temps_mass.csv'

# Создаем выходную директорию
output_dir.mkdir(parents=True, exist_ok=True)

# Загружаем данные
moistures_df = pd.read_csv(moistures_file)
perten_df = pd.read_csv(perten_file)

# Преобразование времени в datetime
def parse_time(df):
    return pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%d-%m-%Y %H:%M:%S', errors='coerce')

moistures_df['DateTime'] = parse_time(moistures_df)
perten_df['DateTime'] = parse_time(perten_df)

# Очищаем данные от некорректных временных меток
moistures_df = moistures_df.dropna(subset=['DateTime']).sort_values('DateTime').reset_index(drop=True)
perten_df = perten_df.dropna(subset=['DateTime']).sort_values('DateTime').reset_index(drop=True)

# Подготавливаем колонки Perten
perten_df = perten_df.rename(columns={
    'Grain': 'perten_Grain',
    '%mois': 'perten_Moisture',
    'TW': 'perten_Nature',
    'Temp': 'perten_Temperature'
})

# Создаем копию moistures_df для обработки
combined_df = moistures_df.copy()

# Добавляем пустые колонки Perten
combined_df['perten_Grain'] = ''
combined_df['perten_Moisture'] = ''
combined_df['perten_Nature'] = ''
combined_df['perten_Temperature'] = ''

# Отслеживаем использованные индексы
used_moistures = set()
used_perten = set()

# Подстановка данных
for p_idx, p_row in perten_df.iterrows():
    if p_idx in used_perten:
        continue  # Пропускаем уже использованные записи Perten
    
    # Фильтруем только незаполненные строки
    available = combined_df[combined_df['perten_Grain'] == '']
    
    if available.empty:
        break  # Нет доступных строк
    
    # Находим ближайшую по времени
    time_diff = (available['DateTime'] - p_row['DateTime']).abs()
    closest_idx = time_diff.idxmin()
    
    # Обновляем данные
    combined_df.at[closest_idx, 'perten_Grain'] = p_row['perten_Grain']
    combined_df.at[closest_idx, 'perten_Moisture'] = p_row['perten_Moisture']
    combined_df.at[closest_idx, 'perten_Nature'] = p_row['perten_Nature']
    combined_df.at[closest_idx, 'perten_Temperature'] = p_row['perten_Temperature']
    
    # Помечаем как использованные
    used_moistures.add(closest_idx)
    used_perten.add(p_idx)

# Удаляем временной столбец
combined_df = combined_df.drop(columns=['DateTime'])

# Сохраняем результат
combined_df.to_csv(output_file, index=False)
print(f"Данные сохранены в {output_file}")