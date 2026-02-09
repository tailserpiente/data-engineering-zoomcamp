#https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet

import pandas as pd
import requests
import os
from io import BytesIO
from datetime import datetime, timedelta
import duckdb 

def generate_monthly_urls(start_year_month, num_months):
    """
    Генерирует список URL для указанного количества месяцев.
    Формат: 'https://.../yellow_tripdata_YYYY-MM.parquet'
    """
    # Используем альтернативный URL (NYC TLC Trip Data)
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}.parquet"
    urls = []
    current_date = datetime.strptime(start_year_month, "%Y-%m")
    
    for i in range(num_months):
        date_str = current_date.strftime("%Y-%m")
        urls.append(base_url.format(date_str))
        # Переходим к следующему месяцу
        next_month = (current_date.replace(day=1) + timedelta(days=32)).replace(day=1)
        current_date = next_month
    return urls

def download_and_save_data():
    """
    Скачивает данные за указанные месяцы, сохраняет в каталог ./data и подсчитывает строки.
    """
    # Конфигурация
    start_month = "2024-01"  # Начальный месяц
    num_months = 6           # Количество месяцев для загрузки
    data_dir = "./data"      # Каталог для сохранения
    total_rows = 0           # Общее количество строк
    
    # Создаем каталог, если его нет
    os.makedirs(data_dir, exist_ok=True)
    
    # Генерируем список URL для скачивания
    urls_to_download = generate_monthly_urls(start_month, num_months)
    
    print(f"Загрузка данных за {num_months} месяцев, начиная с {start_month}...")
    
    for url in urls_to_download:
        try:
            # Получаем имя файла из URL
            filename = url.split("/")[-1]
            filepath = os.path.join(data_dir, filename)
            print(f"Обработка: {filename}")
            
            # Скачиваем файл
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()  # Проверяем на ошибки HTTP
            
            # Проверяем размер файла
            if int(response.headers.get('Content-Length', 0)) == 0:
                print(f"  -> Предупреждение: файл {filename} пустой или недоступен")
                continue
            
            # Читаем Parquet файл прямо из памяти
            df = pd.read_parquet(BytesIO(response.content))
            
            # Сохраняем на диск
            df.to_parquet(filepath)
            
            # Подсчитываем строки в текущем файле
            rows_in_file = len(df)
            total_rows += rows_in_file
            print(f"  -> Успешно сохранено в {filepath}, строк: {rows_in_file:,}")
            
        except requests.exceptions.RequestException as e:
            print(f"  -> Ошибка сети при загрузке {filename}: {e}")
        except Exception as e:
            print(f"  -> Неожиданная ошибка при обработке {filename}: {e}")
    
    # Итоговый отчет
    print("\n" + "="*50)
    print(f"ОБЩИЙ ИТОГ:")
    print(f"Всего файлов обработано: {len([f for f in os.listdir(data_dir) if f.endswith('.parquet')])}")
    print(f"Общее количество строк во всех файлах: {total_rows:,}")
    print(f"Файлы сохранены в каталог: {os.path.abspath(data_dir)}")
    print("="*50)
 
# Запускаем загрузку
if __name__ == "__main__":
    """try:
        download_and_save_data()
    except Exception as e:
        print(f"Основной метод загрузки не сработал: {e}")
    """

    # Создаем подключение (или используем in-memory)
    conn = duckdb.connect()

    # Вариант 1: Чтение всех файлов
    query_all = """
    SELECT 
        COUNT(1)  
    FROM read_parquet('./data/*.parquet') where fare_amount =0;
    """
  
    # Выполняем запрос
    result = conn.execute(query_all).fetchall()
    print(f"Всего строк: {result}, {type(result)}") 
