import os

INSERT_SQL_BATCH_SIZE = 100
DEFAULT_PARALLELISM = 10
ENV_NAME = os.environ.get('RUN_ENV', 'development')

# PG configuration
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'DavidDB')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'david')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'Omen123456789')
POSTGRES_JDBC_DRIVER = "org.postgresql.Driver"


# Tha Kafka configuration
KAFKA_SERVERS = os.environ.get('KAFKA_SERVERS', 'localhost:9092')


JAR_PATH_KSQL = "./lib/flink-sql-connector-kafka_2.11-1.11.6.jar"
JAR_PATH_POSTGRES = "./lib/postgresql-42.7.6.jar"
JAR_PATH_JDBC = "./lib/flink-connector-jdbc_2.11-1.11.6.jar"