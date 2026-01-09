from minio import Minio
from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy
from kafka import KafkaConsumer
from typing import Tuple
from clickhouse_driver import Client
import json
import logging


logger = logging.getLogger(__name__)

MINIO_CONFIG = {
    'endpoint': 'host.docker.internal:9000',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin',
    'bucket': 'click-analysis'
}

CASSANDRA_CONFIG = {
    'hosts': ['host.docker.internal'],
    'port': 9042,
    'keyspace': 'clickstream',
    'table': 'click_events',
    'local_datacenter': 'DC1'
}

KAFKA_CONFIG = {
    'bootstrap_servers': ['host.docker.internal:9092', 'host.docker.internal:9094'],
    'topic': 'app.messages',
    'group_id': 'app-messages-0',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': False,
    'max_poll_records': 1000,
    'session_timeout_ms': 30000,
    'heartbeat_interval_ms': 10000
}

CLICKHOUSE_CONFIG = {
    'host': 'host.docker.internal',
    'port': 9002,
    'user': 'admin',
    'password': 'admin',
    'database': 'analytics'
}


def get_minio_client():
    try:
        client = Minio(
            MINIO_CONFIG['endpoint'],
            access_key=MINIO_CONFIG['access_key'],
            secret_key=MINIO_CONFIG['secret_key'],
            secure=False
        )
        logger.info("Successfully created MinIO client")
        return client
    except Exception as e:
        logger.error(f"Failed to create MinIO client: {e}")
        raise

def get_cassandra_session() -> Tuple[Session, Cluster]:
    try:
        cluster = Cluster(
            contact_points=CASSANDRA_CONFIG['hosts'],
            port=CASSANDRA_CONFIG['port'],
            load_balancing_policy=DCAwareRoundRobinPolicy(
                local_dc=CASSANDRA_CONFIG['local_datacenter']
            ),
            protocol_version=4,
            connect_timeout=30
        )
        session = cluster.connect(CASSANDRA_CONFIG['keyspace'])

        session.set_keyspace(CASSANDRA_CONFIG['keyspace'])

        logger.info("Successfully connected to Cassandra")
        return session, cluster
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {e}")
        raise

def get_kafka_consumer() -> KafkaConsumer:
    try:
        consumer = KafkaConsumer(
            KAFKA_CONFIG['topic'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            group_id=KAFKA_CONFIG['group_id'],
            auto_offset_reset=KAFKA_CONFIG['auto_offset_reset'],
            enable_auto_commit=KAFKA_CONFIG['enable_auto_commit'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            max_poll_records=KAFKA_CONFIG['max_poll_records'],
            session_timeout_ms=KAFKA_CONFIG['session_timeout_ms'],
            heartbeat_interval_ms=KAFKA_CONFIG['heartbeat_interval_ms'],
            consumer_timeout_ms=10000
        )

        logger.info(f"Successfully created Kafka consumer for topic: {KAFKA_CONFIG['topic']}")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        raise

def get_clickhouse_client():
    try:
        client = Client(
            host=CLICKHOUSE_CONFIG['host'],
            port=CLICKHOUSE_CONFIG['port'],
            user=CLICKHOUSE_CONFIG['user'],
            password=CLICKHOUSE_CONFIG['password'],
            database=CLICKHOUSE_CONFIG['database']
        )
        logger.info("Successfully connected to ClickHouse")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        raise