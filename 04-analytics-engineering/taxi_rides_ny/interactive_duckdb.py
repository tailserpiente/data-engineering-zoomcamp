import duckdb
import code

# Подключаемся к базе
conn = duckdb.connect('taxi_rides_ny.duckdb')
print("✅ DuckDB подключен к 'taxi_rides_ny.duckdb'")
print("🐥 Используйте conn.execute('SQL').fetchall() для запросов")
print("📝 Например: conn.execute('SELECT 1').fetchall()")
print("🚪 exit() для выхода\n")

# Создаем интерактивную консоль с conn в пространстве имен
vars = globals().copy()
vars.update(locals())
shell = code.InteractiveConsole(vars)
shell.interact()
