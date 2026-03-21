 df_with_duration = df.withColumn( "trip_duration_seconds",  unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime")))

pickup_counts = df.groupBy("PULocationID") \
.agg(count("*").alias("trip_count")) \
.orderBy(asc("trip_count"))

