from datetime import datetime, timedelta
from airflow.decorators import dag, task
from typing import Dict, List, Optional, Tuple
import logging
import json
import uuid
from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from dataclasses import dataclass, field
from enum import Enum
import hashlib

logger = logging.getLogger(__name__)

CASSANDRA_CONFIG = {
    'hosts': ['host.docker.internal'],
    'port': 9042,
    'source_keyspace': 'clickstream',
    'target_keyspace': 'analytics',
    'local_datacenter': 'DC1'
}

CLEAN_EVENTS_TTL = 86400 * 365  # 1 год
INVALID_EVENTS_TTL = 86400 * 7  # 7 дней


class ValidationStatus(Enum):
    VALID = "VALID"
    INVALID = "INVALID"
    SUSPICIOUS = "SUSPICIOUS"


@dataclass
class RawClickEvent:
    user_id: Optional[int] = None
    created_at: Optional[datetime] = None
    id: Optional[str] = None
    type: Optional[str] = None
    received_at: Optional[datetime] = None
    session_id: Optional[str] = None
    ip: Optional[str] = None
    url: Optional[str] = None
    referrer: Optional[str] = None
    device_type: Optional[str] = None
    user_agent: Optional[str] = None
    event_title: Optional[str] = None
    element_id: Optional[str] = None
    x: Optional[int] = None
    y: Optional[int] = None
    element_text: Optional[str] = None
    element_class: Optional[str] = None
    page_title: Optional[str] = None
    viewport_width: Optional[int] = None
    viewport_height: Optional[int] = None
    scroll_position: Optional[float] = None
    timestamp_offset: Optional[int] = None
    metadata: Dict[str, str] = field(default_factory=dict)


@dataclass
class CleanClickEvent:
    id: str
    type: str
    created_at: datetime
    session_id: str
    user_id: int
    url: str
    device_type: Optional[str]
    element_id: Optional[str]
    x: Optional[int]
    y: Optional[int]
    event_title: Optional[str]

    def get_deduplication_hash(self) -> str:
        if self.type.upper() == "CLICK":
            key = f"{self.id}_{self.user_id}_{self.session_id}_{self.type}_{self.created_at}_{self.x or 0}_{self.y or 0}"
        else:
            key = f"{self.id}_{self.user_id}_{self.session_id}_{self.type}_{self.created_at}"

        return hashlib.md5(key.encode()).hexdigest()


@dataclass
class InvalidEvent:
    user_id: Optional[int]
    id: Optional[str]
    type: Optional[str]
    session_id: Optional[str]
    event_time: Optional[datetime]
    processed_at: datetime
    validation_error: str


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
        session = cluster.connect()

        logger.info(f"Successfully connected to Cassandra at {CASSANDRA_CONFIG['hosts']}")
        return session, cluster

    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {e}")
        raise


class CassandraEventProcessor:
    VALID_DEVICE_TYPES = ['DESKTOP', 'MOBILE', 'TABLET', 'ANDROID', 'IOS']
    VALID_EVENT_TYPES = ['VIEW', 'CLICK']

    @staticmethod
    def validate_event(event: RawClickEvent) -> Tuple[ValidationStatus, List[str]]:
        errors = []

        # Проверка обязательных полей
        if not event.id:
            errors.append("Event ID is required")

        if not event.type:
            errors.append("Event type is required")
        elif event.type.upper() not in CassandraEventProcessor.VALID_EVENT_TYPES:
            errors.append(f"Invalid event type: {event.type}")

        if event.user_id is None:
            errors.append("User ID is required")
        elif event.user_id <= 0:
            errors.append(f"User ID must be positive: {event.user_id}")

        if not event.session_id:
            errors.append("Session ID is required")

        if not event.url:
            errors.append("URL is required")

        if not event.user_agent:
            errors.append("User agent is required")

        # Проверка временных меток
        if not event.created_at:
            errors.append("Created at timestamp is required")
        elif event.created_at > datetime.now():
            errors.append(f"Created at timestamp is in the future: {event.created_at}")

        if event.received_at and event.received_at > datetime.now():
            errors.append(f"Received at timestamp is in the future: {event.received_at}")

        # Проверка типа устройства
        if event.device_type and event.device_type.upper() not in CassandraEventProcessor.VALID_DEVICE_TYPES:
            errors.append(f"Invalid device type: {event.device_type}")

        # Проверка для CLICK событий
        if event.type and event.type.upper() == 'CLICK' and not event.element_id:
            errors.append("Element ID is required for CLICK events")

        # Определение статуса
        if not errors:
            return ValidationStatus.VALID, errors

        # Критичные ошибки
        critical_keywords = ['required', 'Invalid event type', 'User ID must be positive']
        if any(any(keyword in error for keyword in critical_keywords) for error in errors):
            return ValidationStatus.INVALID, errors

        return ValidationStatus.SUSPICIOUS, errors

    @staticmethod
    def prepare_clean_event_params(event: CleanClickEvent) -> tuple:
        return (
            event.user_id,
            event.created_at,
            event.id,
            event.type,
            event.session_id,
            event.url,
            event.device_type,
            event.event_title,
            event.element_id,
            event.x,
            event.y
        )

    @staticmethod
    def prepare_invalid_event_params(event: InvalidEvent) -> tuple:
        return (
            event.user_id,
            event.id,
            event.type,
            event.session_id,
            event.event_time,
            event.processed_at,
            event.validation_error
        )

    @staticmethod
    def execute_batch_insert(session: Session, prepared_stmt, batch_params: List[tuple]) -> Tuple[int, int]:
        try:
            results = execute_concurrent_with_args(
                session,
                prepared_stmt,
                batch_params,
                concurrency=5
            )

            success_count = 0
            failed_count = 0

            for success, result in results:
                if success:
                    success_count += 1
                else:
                    failed_count += 1
                    logger.warning(f"Failed to insert: {result}")

            return success_count, failed_count

        except Exception as e:
            logger.error(f"Error in batch execution: {e}")
            raise


@dag(
    'cassandra_to_cassandra_etl',
    description='ETL pipeline from Cassandra to Cassandra (different keyspaces)',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['cassandra', 'etl', 'clickstream'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    }
)
def cassandra_to_cassandra_pipeline():
    @task
    def extract_and_transform(**context) -> str:
        try:
            logger.info("Starting extraction and transformation")

            session, cluster = get_cassandra_session()
            processor = CassandraEventProcessor()

            data_interval_start = datetime.now() - timedelta(days=1)
            data_interval_end = datetime.now()

            query = f"""
                SELECT * FROM {CASSANDRA_CONFIG['source_keyspace']}.click_events 
                WHERE created_at >= ? AND created_at < ?
                ALLOW FILTERING
            """

            prepared_query = session.prepare(query)

            logger.info(f"Extracting events from {data_interval_start} to {data_interval_end}")
            rows = session.execute(prepared_query, (data_interval_start, data_interval_end))

            valid_events = []
            invalid_events = []

            seen_hashes = set()

            for row in rows:
                try:
                    event = RawClickEvent(
                        user_id=row.user_id,
                        created_at=row.created_at,
                        id=row.id,
                        type=row.type,
                        received_at=row.received_at,
                        session_id=row.session_id,
                        ip=row.ip,
                        url=row.url,
                        referrer=row.referrer,
                        device_type=row.device_type,
                        user_agent=row.user_agent,
                        event_title=row.event_title,
                        element_id=row.element_id,
                        x=row.x,
                        y=row.y,
                        element_text=row.element_text,
                        element_class=row.element_class,
                        page_title=row.page_title,
                        viewport_width=row.viewport_width,
                        viewport_height=row.viewport_height,
                        scroll_position=row.scroll_position,
                        timestamp_offset=row.timestamp_offset,
                        metadata=row.metadata if row.metadata else {}
                    )

                    status, errors = processor.validate_event(event)

                    if status == ValidationStatus.VALID:
                        clean_event = CleanClickEvent(
                            id=event.id or str(uuid.uuid4()),
                            type=event.type,
                            created_at=event.created_at,
                            session_id=event.session_id,
                            user_id=event.user_id,
                            url=event.url,
                            device_type=event.device_type,
                            element_id=event.element_id,
                            x=event.x,
                            y=event.y,
                            event_title=event.event_title
                        )

                        event_hash = clean_event.get_deduplication_hash()
                        if event_hash not in seen_hashes:
                            seen_hashes.add(event_hash)
                            valid_events.append(clean_event)
                        else:
                            logger.debug(f"Skipped duplicate event: {event.id}")

                    elif status == ValidationStatus.SUSPICIOUS:
                        clean_event = CleanClickEvent(
                            id=event.id or str(uuid.uuid4()),
                            type=event.type,
                            created_at=event.created_at,
                            session_id=event.session_id,
                            user_id=event.user_id,
                            url=event.url,
                            device_type=event.device_type,
                            element_id=event.element_id,
                            x=event.x,
                            y=event.y,
                            event_title=event.event_title
                        )

                        event_hash = clean_event.get_deduplication_hash()
                        if event_hash not in seen_hashes:
                            seen_hashes.add(event_hash)
                            valid_events.append(clean_event)

                        invalid_event = InvalidEvent(
                            user_id=event.user_id,
                            id=event.id,
                            type=event.type,
                            session_id=event.session_id,
                            event_time=event.created_at,
                            processed_at=datetime.now(),
                            validation_error="; ".join(errors)
                        )
                        invalid_events.append(invalid_event)

                    else:
                        invalid_event = InvalidEvent(
                            user_id=event.user_id,
                            id=event.id,
                            type=event.type,
                            session_id=event.session_id,
                            event_time=event.created_at,
                            processed_at=datetime.now(),
                            validation_error="; ".join(errors)
                        )
                        invalid_events.append(invalid_event)

                except Exception as e:
                    logger.error(f"Error processing row: {e}")
                    continue

            session.shutdown()
            cluster.shutdown()

            result = {
                'valid_events': [
                    {
                        'id': e.id,
                        'type': e.type,
                        'created_at': e.created_at.isoformat() if e.created_at else None,
                        'session_id': e.session_id,
                        'user_id': e.user_id,
                        'url': e.url,
                        'device_type': e.device_type,
                        'element_id': e.element_id,
                        'x': e.x,
                        'y': e.y,
                        'event_title': e.event_title
                    }
                    for e in valid_events
                ],
                'invalid_events': [
                    {
                        'user_id': e.user_id,
                        'id': e.id,
                        'type': e.type,
                        'session_id': e.session_id,
                        'event_time': e.event_time.isoformat() if e.event_time else None,
                        'processed_at': e.processed_at.isoformat(),
                        'validation_error': e.validation_error
                    }
                    for e in invalid_events
                ]
            }

            logger.info(f"Extraction complete: {len(valid_events)} valid, {len(invalid_events)} invalid events")

            return json.dumps(result, default=str)

        except Exception as e:
            logger.error(f"Error in extract_and_transform: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return json.dumps({'valid_events': [], 'invalid_events': []})

    @task
    def load_clean_events(**context) -> Dict[str, int]:
        try:
            logger.info("Starting clean events load")

            task_instance = context['ti']
            events_json = task_instance.xcom_pull(task_ids='extract_and_transform')

            if not events_json:
                logger.info("No events to load")
                return {'loaded': 0, 'failed': 0, 'total': 0}

            events_data = json.loads(events_json)
            valid_events_data = events_data.get('valid_events', [])

            if not valid_events_data:
                logger.info("No valid events to load")
                return {'loaded': 0, 'failed': 0, 'total': 0}

            session, cluster = get_cassandra_session()
            processor = CassandraEventProcessor()

            insert_query = f"""
                INSERT INTO {CASSANDRA_CONFIG['target_keyspace']}.clean_click_events (
                    user_id, created_at, id, type, session_id,
                    url, device_type, event_title, element_id, x, y
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                USING TTL {CLEAN_EVENTS_TTL}
            """

            prepared_stmt = session.prepare(insert_query)
            prepared_stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM

            total_loaded = 0
            total_failed = 0
            batch_size = 100

            for i in range(0, len(valid_events_data), batch_size):
                batch_data = valid_events_data[i:i + batch_size]
                batch_params = []

                for event_dict in batch_data:
                    try:
                        clean_event = CleanClickEvent(
                            id=event_dict['id'],
                            type=event_dict['type'],
                            created_at=datetime.fromisoformat(event_dict['created_at']) if event_dict[
                                'created_at'] else None,
                            session_id=event_dict['session_id'],
                            user_id=event_dict['user_id'],
                            url=event_dict['url'],
                            device_type=event_dict.get('device_type'),
                            element_id=event_dict.get('element_id'),
                            x=event_dict.get('x'),
                            y=event_dict.get('y'),
                            event_title=event_dict.get('event_title')
                        )

                        params = processor.prepare_clean_event_params(clean_event)
                        batch_params.append(params)

                    except Exception as e:
                        logger.error(f"Error preparing clean event params: {e}")
                        total_failed += 1
                        continue

                if batch_params:
                    try:
                        loaded, failed = processor.execute_batch_insert(session, prepared_stmt, batch_params)
                        total_loaded += loaded
                        total_failed += failed

                        if (i // batch_size + 1) % 10 == 0:
                            logger.info(f"Processed {i + len(batch_data)} clean events")

                    except Exception as e:
                        logger.error(f"Error executing clean events batch: {e}")
                        total_failed += len(batch_params)

            session.shutdown()
            cluster.shutdown()

            result = {
                'loaded': total_loaded,
                'failed': total_failed,
                'total': len(valid_events_data)
            }

            logger.info(f"Clean events load complete: {result}")
            return result

        except Exception as e:
            logger.error(f"Error loading clean events: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {'loaded': 0, 'failed': 1, 'total': 0}

    @task
    def load_invalid_events(**context) -> Dict[str, int]:
        try:
            logger.info("Starting invalid events load")

            task_instance = context['ti']
            events_json = task_instance.xcom_pull(task_ids='extract_and_transform')

            if not events_json:
                logger.info("No events to load")
                return {'loaded': 0, 'failed': 0, 'total': 0}

            events_data = json.loads(events_json)
            invalid_events_data = events_data.get('invalid_events', [])

            if not invalid_events_data:
                logger.info("No invalid events to load")
                return {'loaded': 0, 'failed': 0, 'total': 0}

            session, cluster = get_cassandra_session()
            processor = CassandraEventProcessor()

            insert_query = f"""
                INSERT INTO {CASSANDRA_CONFIG['target_keyspace']}.invalid_events (
                    user_id, id, type, session_id, event_time, processed_at, validation_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                USING TTL {INVALID_EVENTS_TTL}
            """

            prepared_stmt = session.prepare(insert_query)
            prepared_stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM

            total_loaded = 0
            total_failed = 0
            batch_size = 100

            for i in range(0, len(invalid_events_data), batch_size):
                batch_data = invalid_events_data[i:i + batch_size]
                batch_params = []

                for event_dict in batch_data:
                    try:
                        invalid_event = InvalidEvent(
                            user_id=event_dict['user_id'],
                            id=event_dict['id'],
                            type=event_dict['type'],
                            session_id=event_dict['session_id'],
                            event_time=datetime.fromisoformat(event_dict['event_time']) if event_dict[
                                'event_time'] else None,
                            processed_at=datetime.fromisoformat(event_dict['processed_at']),
                            validation_error=event_dict['validation_error']
                        )

                        params = processor.prepare_invalid_event_params(invalid_event)
                        batch_params.append(params)

                    except Exception as e:
                        logger.error(f"Error preparing invalid event params: {e}")
                        total_failed += 1
                        continue

                if batch_params:
                    try:
                        loaded, failed = processor.execute_batch_insert(session, prepared_stmt, batch_params)
                        total_loaded += loaded
                        total_failed += failed

                    except Exception as e:
                        logger.error(f"Error executing invalid events batch: {e}")
                        total_failed += len(batch_params)

            session.shutdown()
            cluster.shutdown()

            result = {
                'loaded': total_loaded,
                'failed': total_failed,
                'total': len(invalid_events_data)
            }

            logger.info(f"Invalid events load complete: {result}")
            return result

        except Exception as e:
            logger.error(f"Error loading invalid events: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {'loaded': 0, 'failed': 1, 'total': 0}

    @task
    def log_results(clean_results: Dict[str, int], invalid_results: Dict[str, int]):
        try:
            logger.info("=" * 60)
            logger.info("CASSANDRA TO CASSANDRA ETL PIPELINE RESULTS")
            logger.info("=" * 60)

            logger.info("CLEAN EVENTS:")
            logger.info(f"  Total processed: {clean_results.get('total', 0)}")
            logger.info(f"  Successfully loaded: {clean_results.get('loaded', 0)}")
            logger.info(f"  Failed to load: {clean_results.get('failed', 0)}")

            if clean_results.get('total', 0) > 0:
                success_rate = (clean_results.get('loaded', 0) / clean_results.get('total', 0)) * 100
                logger.info(f"  Success rate: {success_rate:.2f}%")

            logger.info("INVALID EVENTS:")
            logger.info(f"  Total processed: {invalid_results.get('total', 0)}")
            logger.info(f"  Successfully loaded: {invalid_results.get('loaded', 0)}")
            logger.info(f"  Failed to load: {invalid_results.get('failed', 0)}")

            if invalid_results.get('total', 0) > 0:
                success_rate = (invalid_results.get('loaded', 0) / invalid_results.get('total', 0)) * 100
                logger.info(f"  Success rate: {success_rate:.2f}%")

            total_events = clean_results.get('total', 0) + invalid_results.get('total', 0)
            total_loaded = clean_results.get('loaded', 0) + invalid_results.get('loaded', 0)
            total_failed = clean_results.get('failed', 0) + invalid_results.get('failed', 0)

            logger.info("=" * 30)
            logger.info("OVERALL:")
            logger.info(f"  Total events: {total_events}")
            logger.info(f"  Total loaded: {total_loaded}")
            logger.info(f"  Total failed: {total_failed}")

            if total_events > 0:
                overall_success_rate = (total_loaded / total_events) * 100
                logger.info(f"  Overall success rate: {overall_success_rate:.2f}%")

            logger.info("=" * 60)

            if clean_results.get('failed', 0) > 0:
                logger.warning(f"Failed to load {clean_results.get('failed', 0)} clean events")

            if invalid_results.get('failed', 0) > 0:
                logger.warning(f"Failed to load {invalid_results.get('failed', 0)} invalid events")

        except Exception as e:
            logger.error(f"Error logging results: {e}")

    # Определение зависимостей задач
    extract_transform_task = extract_and_transform()
    load_clean_task = load_clean_events()
    load_invalid_task = load_invalid_events()
    log_results_task = log_results(load_clean_task, load_invalid_task)

    # Порядок выполнения задач
    extract_transform_task >> [load_clean_task, load_invalid_task] >> log_results_task


dag = cassandra_to_cassandra_pipeline()