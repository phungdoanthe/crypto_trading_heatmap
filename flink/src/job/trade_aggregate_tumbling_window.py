from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
import os

def create_trade_sink_postgres(t_env):
    table_name = 'trade_agg_1min'
    db_host = os.getenv("POSTGRES_HOST")
    db_port = os.getenv("POSTGRES_PORT")
    db_name = os.getenv("POSTGRES_DB")
    db_user = os.getenv("POSTGRES_USER")
    db_password = os.getenv("POSTGRES_PASSWORD")
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"    
    
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            symbol STRING,
            total_qty DOUBLE,
            vwap DOUBLE,
            buy_qty DOUBLE,
            sell_qty DOUBLE,
            PRIMARY KEY (window_start, symbol) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{jdbc_url}',
            'table-name' = '{table_name}',
            'username' = '{db_user}',
            'password' = '{db_password}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_trade_source_kafka(t_env):
    table_name = "trade_events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            symbol STRING,
            price DOUBLE,
            qty DOUBLE,
            side STRING,
            timestamp BIGINT,
            event_timestamp AS TO_TIMESTAMP(FROM_UNIXTIME(timestamp / 1000)),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'raw_trade',
            'group.id' = 'trade-agg-group',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def log_trade_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)


    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_trade_source_kafka(t_env)
        sink_table = create_trade_sink_postgres(t_env)

        t_env.execute_sql(
            f"""
                INSERT INTO {sink_table}
                SELECT
                    window_start,
                    symbol,
                    SUM(qty) AS total_qty,
                    SUM(price * qty) / SUM(qty) AS vwap,
                    SUM(CASE WHEN side='buy' THEN qty ELSE 0 END) AS buy_qty,
                    SUM(CASE WHEN side='sell' THEN qty ELSE 0 END) AS sell_qty
                FROM TABLE(
                    TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '1' MINUTE)
                )
                GROUP BY window_start, symbol
            """
        ).wait()

    except Exception as e:
        print("Writing trade aggregated records to PostgreSQL failed:", str(e))


if __name__ == '__main__':
    log_trade_processing()