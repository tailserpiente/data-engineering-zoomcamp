import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .config("spark.sql.adaptive.enabled", "false") \
    .getOrCreate()

input_file = "yellow_tripdata_2025-11.parquet"

print(f"Spark version: {spark.version}")

df = spark.read.parquet(input_file)
total_count = df.count()
print(f"\nОбщее количество строк в файле: {total_count}")

df_repartitioned = df.repartition(4)

# Проверяем количество партиций
print(f"Количество партиций после repartition: {df_repartitioned.rdd.getNumPartitions()}")

# Сохраняем в 4 партиции (как parquet)
output_path = "yellow_tripdata_2025-11_partitioned"
df_repartitioned.write.mode("overwrite").parquet(output_path)


spark.stop()
