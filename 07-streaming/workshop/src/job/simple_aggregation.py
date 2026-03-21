# simple_aggregation.py - без окон, просто вставка
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# Источник Kafka
t_env.execute_sql("""
    CREATE TABLE events (
        PULocationID INTEGER,
        DOLocationID INTEGER,
        trip_distance DOUBLE,
        total_amount DOUBLE,
        lpep_pickup_datetime BIGINT,
        event_timestamp AS TO_TIMESTAMP_LTZ(lpep_pickup_datetime, 3)
    ) WITH (
        'connector' = 'kafka',
        'properties.bootstrap.servers' = 'redpanda:29092',
        'topic' = 'green-trips',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
""")

# Sink в PostgreSQL
t_env.execute_sql("""
    CREATE TABLE postgres_sink (
        PULocationID INTEGER,
        total_amount DOUBLE,
        event_time TIMESTAMP(3)
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = 'simple_events',
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver',
        'sink.buffer-flush.max-rows' = '1'
    )
""")

# Просто вставляем все данные без агрегации
t_env.execute_sql("""
    INSERT INTO postgres_sink
    SELECT 
        PULocationID,
        total_amount,
        event_timestamp
    FROM events
""").wait()

print("Done!")
