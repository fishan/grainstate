import pandas as pd
from pathlib import Path

# Пути к файлам
input_dir = Path('output')  # Где лежат ваши текущие файлы
perten_file = Path('perten_data.csv')
output_file = input_dir / 'moistures_temps_combined.csv'

# Загрузка данных
moistures_df = pd.read_csv(input_dir / 'moistures_temps.csv')
perten_df = pd.read_csv(perten_file)

# Преобразование даты и времени в datetime
def parse_datetime(df):
    return pd.to_datetime(df['Date'] + ' ' + df['Time'], format='%d-%m-%Y %H:%M:%S')

moistures_df['datetime'] = parse_datetime(moistures_df)
perten_df['datetime'] = pd.to_datetime(perten_df['Date'] + ' ' + perten_df['Time'], 
                                      format='%d-%m-%Y %H:%M:%S')

# Сортировка по времени (обязательно для merge_asof)
moistures_df = moistures_df.sort_values('datetime').reset_index(drop=True)
perten_df = perten_df.sort_values('datetime').reset_index(drop=True)

# Создаем столбец для отслеживания использования записей из perten_data.csv
perten_df['used'] = False

# Объединение данных
for i, row in moistures_df.iterrows():
    if perten_df.empty:
        break  # Если все записи из perten_data.csv использованы, завершаем цикл
    
    # Находим ближайшую неподставленную запись
    nearest_idx = ((perten_df['datetime'] - row['datetime']).abs()).idxmin()
    nearest_row = perten_df.loc[nearest_idx]
    
    # Проверяем, что запись ещё не использована
    if not nearest_row['used']:
        # Подставляем данные
        moistures_df.at[i, 'perten_Grain'] = nearest_row['Grain']
        moistures_df.at[i, 'perten_moisture'] = nearest_row['%mois']
        moistures_df.at[i, 'perten_TW'] = nearest_row['TW']
        moistures_df.at[i, 'perten_Temp'] = nearest_row['Temp']
        
        # Отмечаем запись как использованную
        perten_df.at[nearest_idx, 'used'] = True

# Удаляем временные колонки
moistures_df = moistures_df.drop(columns=['datetime'])

# Сохранение результата
moistures_df.to_csv(output_file, index=False)
print(f"Объединенные данные сохранены в {output_file}")