import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.window import Session
from pyflink.table.expressions import col, lit

def create_sessionized_events_sink_postgres(t_env):
    table_name = 'sessionized_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            num_events BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            event_time VARCHAR,
            host VARCHAR,
            url VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name

def sessionize_data():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Define source and sink tables
    source_table = create_events_source_kafka(t_env)
    sink_table = create_sessionized_events_sink_postgres(t_env)

    # Perform sessionization
    t_env.from_path(source_table)\
        .window(Session.with_gap(lit(5).minutes).on(col("event_timestamp")).alias("session_window"))\
        .group_by(col("session_window"), col("ip"), col("host"))\
        .select(
            col("ip"),
            col("host"),
            col("session_window").start.alias("session_start"),
            col("session_window").end.alias("session_end"),
            lit(1).count.alias("num_events")
        )\
        .execute_insert(sink_table)\
        .wait()

if __name__ == '__main__':
    sessionize_data()
