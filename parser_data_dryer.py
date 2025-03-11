import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("parser_data_dryer.log"),  # Лог в файл
        logging.StreamHandler()  # Лог в консоль
    ]
)

input_file_path = 'dryer_data.csv'
output_dir = 'output'

# Создаем выходную директорию
Path(output_dir).mkdir(parents=True, exist_ok=True)
logging.info("Создана выходная директория")

# Прочитайте файл с текущим разделителем (запятая)
try:
    df = pd.read_csv(input_file_path)
    logging.info(f"Файл {input_file_path} успешно загружен")
except Exception as e:
    logging.error(f"Ошибка при чтении файла: {e}")
    raise

# Удалите столбец measured, если он существует
if 'measured' in df.columns:
    df = df.drop('measured', axis=1)
    logging.info("Столбец 'measured' удален")

# Обработка временной метки
def process_timestamp(ts):
    try:
        # Удаляем микросекунды
        if '.' in ts:
            ts = ts.split('.')[0]
        dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
        return {
            'Date': dt.strftime('%d-%m-%Y'),
            'Time': dt.strftime('%H:%M:%S'),
            'DateTime': dt  # Для сортировки
        }
    except Exception as e:
        logging.error(f"Ошибка при обработке временной метки '{ts}': {e}")
        return None

# Функция для преобразования ISO-времени в секунды
def convert_iso_to_seconds(value):
    if isinstance(value, str):
        try:
            # Убираем все кавычки и пробелы
            value = value.strip().strip('"')
            
            # Проверяем, что формат начинается с "PT"
            if not value.startswith('PT'):
                logging.warning(f"Некорректный формат ISO-времени: '{value}' (не начинается с 'PT')")
                return None
            
            # Убираем префикс "PT"
            value = value[2:]
            
            # Разделяем минуты и секунды
            minutes = 0
            seconds = 0
            if 'M' in value:
                parts = value.split('M')
                minutes = int(parts[0])  # Минуты
                if 'S' in parts[1]:
                    seconds = int(parts[1].replace('S', ''))  # Секунды
            elif 'S' in value:
                seconds = int(value.replace('S', ''))  # Только секунды
            
            # Возвращаем общее количество секунд
            return minutes * 60 + seconds
        except Exception as e:
            logging.warning(f"Некорректный формат ISO-времени '{value}': {e}")
            return None
    return value

# Объединение данных
data_dict = {}
for _, row in df.iterrows():
    raw_ts = row['timestamp']
    
    # Проверяем, что временная метка корректна
    try:
        processed_ts = process_timestamp(raw_ts)
        if not processed_ts:
            logging.warning(f"Пропущена некорректная временная метка: {raw_ts}")
            continue
        
        # Преобразуем в datetime для сравнения
        timestamp_dt = processed_ts['DateTime']
        
        # Пропускаем записи до указанного времени
        cutoff_time = datetime(2024, 8, 7, 22, 54, 11)
        if timestamp_dt < cutoff_time:
            logging.debug(f"Пропущена запись до {cutoff_time}: {raw_ts}")
            continue
        
        # Пропускаем записи после указанного времени
        if timestamp_dt > datetime(2024, 8, 30, 14, 22, 53):
            logging.debug(f"Пропущена запись после 2024-08-30 14:22:53: {raw_ts}")
            continue
    except Exception as e:
        logging.error(f"Ошибка при обработке временной метки '{raw_ts}': {e}")
        continue
    
    key = f"{processed_ts['Date']} {processed_ts['Time']}"
    
    var_name = row['var_name']
    var_data = row['var_data']
    
    if key not in data_dict:
        data_dict[key] = {'Date': processed_ts['Date'], 
                         'Time': processed_ts['Time'],
                         'DateTime': processed_ts['DateTime']}  # Для сортировки
    
    # Специальная обработка DROPS_SET_TIMER
    if var_name == 'DROPS_SET_TIMER':
        var_data = convert_iso_to_seconds(var_data)
    
    data_dict[key][var_name] = var_data

logging.info("Данные успешно объединены в словарь")

# Создаем DataFrame
try:
    df = pd.DataFrame.from_dict(data_dict, orient='index')
    logging.info("DataFrame успешно создан из словаря")
except Exception as e:
    logging.error(f"Ошибка при создании DataFrame: {e}")
    raise

# Сортируем данные по DateTime
try:
    df = df.sort_values(by='DateTime').drop(columns=['DateTime'])
    logging.info("Данные успешно отсортированы по времени")
except Exception as e:
    logging.error(f"Ошибка при сортировке данных: {e}")
    raise

# Преобразуем DROPS_SCORE в числовой формат
try:
    df['DROPS_SCORE'] = pd.to_numeric(df['DROPS_SCORE'], errors='coerce').astype('Int64')
    logging.info("Столбец DROPS_SCORE успешно преобразован в числовой формат")
except Exception as e:
    logging.error(f"Ошибка при преобразовании DROPS_SCORE: {e}")
    raise

# Сохраняем в файлы с требуемыми колонками
def save_dataframe(columns, filename):
    try:
        # Берем только существующие колонки
        existing_cols = [col for col in columns if col in df.columns]
        # Добавляем Date и Time в начало
        final_cols = ['Date', 'Time'] + [col for col in existing_cols if col not in ['Date', 'Time']]
        # Создаем DataFrame
        output_df = df[final_cols].copy()  # Используем .copy() для избежания SettingWithCopyWarning
        
        # Заполняем пропуски в зависимости от типа данных
        for col in output_df.columns:
            if output_df[col].dtype == 'Int64':  # Для целочисленных столбцов
                output_df.loc[:, col] = output_df[col].fillna(pd.NA)  # Используем .loc
            elif output_df[col].dtype == 'float64':  # Для числовых столбцов
                output_df.loc[:, col] = output_df[col].fillna(0).astype(float)  # Заменяем NaN на 0
            else:  # Для остальных столбцов
                output_df.loc[:, col] = output_df[col].fillna('').astype(str)  # Заменяем NaN на пустую строку
        
        # Сохраняем файл
        output_path = Path(output_dir) / filename
        output_df.to_csv(output_path, index=False)
        logging.info(f"Файл {filename} успешно сохранен в {output_path}")
    except Exception as e:
        logging.error(f"Ошибка при сохранении файла {filename}: {e}")
        raise

# Сохраняем все файлы
save_dataframe(['Date', 'Time', 'DROPS_SCORE', 'SET_BURNERS_TEMP', 
                'ACTUAL_BURNERS_TEMP', 'TOP_TEMP', 'MID_TEMP', 'BOTTOM_TEMP'], 'temps.csv')

save_dataframe(['Date', 'Time', 'GRAIN_TYPE', 'DROPS_SCORE', 'DRY_MOISTURE', 
                'DRY_TEMP', 'DRY_NATURE', 'WET_MOISTURE', 'WET_TEMP', 'WET_NATURE'], 'moistures.csv')

save_dataframe(['Date', 'Time', 'GRAIN_TYPE', 'DROPS_SCORE', 'ACTUAL_BURNERS_TEMP', 
                'TOP_TEMP', 'MID_TEMP', 'BOTTOM_TEMP', 'DRY_MOISTURE', 'DRY_TEMP', 
                'DRY_NATURE', 'WET_MOISTURE', 'WET_TEMP', 'WET_NATURE'], 'moistures_temps.csv')

save_dataframe(['Date', 'Time', 'DROPS_SCORE', 'DROPS_SET_TIMER', 'SET_BURNERS_TEMP', 
                'COOLING_TIME', 'BOTTOM_TEMP_LIMIT', 'UPPER_FAN_SET_HZ', 
                'LOWER_FAN_SET_HZ', 'MID_TEMP_LIMIT'], 'settings.csv')

save_dataframe(['Date', 'Time', 'DROPS_SCORE', 'FILLING', 'DRYING', 'RECYCLING', 
                'EMPTY', 'SHUTDOWN', 'STOP', 'COOLING', 'MANUAL'], 'mode.csv')

save_dataframe(['Date', 'Time', 'DROPS_SCORE', 'MIDDLE_LEVEL_ALARM', 
                'HIGH_LEVEL_ALARM', 'BURNER_HIGH_ALARM', 'HOPPER_FULL_ALARM', 
                'LOW_AIR_PRESSURE_ALARM', 'GENERAL_ALARM', 'AIR_OVERHEATED'], 'alarms.csv')

logging.info("Все файлы успешно обработаны и сохранены")