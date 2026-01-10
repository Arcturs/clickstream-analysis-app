from datetime import datetime, timedelta
from airflow.decorators import dag, task
from typing import Dict, Any, List, Optional
import logging
import json
import uuid
from cassandra.cluster import Session
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.data.data_classes import ClickEvent
from src.repository.sessions import get_cassandra_session, get_kafka_consumer, KAFKA_CONFIG, CASSANDRA_CONFIG

logger = logging.getLogger(__name__)


class KafkaEventProcessor:

    @staticmethod
    def process_kafka_message(message: Dict[str, Any]) -> Optional[ClickEvent]:
        try:
            click_event = ClickEvent.from_dict(message)
            logger.debug(f"Successfully processed event: {click_event.id}")
            return click_event
        except Exception as e:
            logger.error(f"Failed to process Kafka message: {e}")
            return None

    @staticmethod
    def prepare_cassandra_params(click_event: ClickEvent) -> tuple:
        metadata = {}
        if click_event.payload and click_event.payload.metadata:
            metadata = {k: str(v) for k, v in click_event.payload.metadata.items()}

        created_at = click_event.created_at if click_event.created_at else datetime.now()
        received_at = click_event.received_at if click_event.received_at else datetime.now()

        return (
            click_event.user_id,
            created_at,
            click_event.id or str(uuid.uuid4()),
            click_event.type,
            received_at,
            click_event.session_id,
            click_event.ip,
            click_event.url,
            click_event.referrer,
            click_event.device_type,
            click_event.user_agent,
            click_event.payload.event_title if click_event.payload else None,
            click_event.payload.element_id if click_event.payload else None,
            click_event.payload.x if click_event.payload else None,
            click_event.payload.y if click_event.payload else None,
            click_event.payload.element_text if click_event.payload else None,
            click_event.payload.element_class if click_event.payload else None,
            click_event.payload.page_title if click_event.payload else None,
            click_event.payload.viewport_width if click_event.payload else None,
            click_event.payload.viewport_height if click_event.payload else None,
            click_event.payload.scroll_position if click_event.payload else None,
            click_event.payload.timestamp_offset if click_event.payload else None,
            metadata
        )

    @staticmethod
    def execute_cassandra_batch(session: Session, prepared_stmt, batch: List[tuple]) -> tuple[int, int]:
        try:
            results = execute_concurrent_with_args(
                session,
                prepared_stmt,
                batch,
                concurrency=5
            )

            success_count = 0
            failed_count = 0
            errors = []

            for (success, result) in results:
                if success:
                    success_count += 1
                else:
                    failed_count += 1
                    errors.append(str(result))

            if errors:
                logger.warning(
                    f"Batch insert had {failed_count} failures. First error: {errors[0] if errors else 'Unknown'}")

            return success_count, failed_count

        except Exception as e:
            logger.error(f"Error in batch execution: {e}")
            raise


@dag(
    'kafka_to_cassandra_pipeline',
    description='Pipeline to load click events from Kafka to Cassandra',
    schedule=timedelta(minutes=1),
    is_paused_upon_creation=False,
    catchup=False,
    max_active_runs=1,
    tags=['clickstream', 'etl', 'cassandra', 'kafka'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(seconds=30),
    }
)
def kafka_to_cassandra_pipeline():
    @task
    def extract_from_kafka(**context) -> str:
        try:
            logger.info(f"Starting Kafka extraction at {datetime.now()}")
            logger.info(f"Kafka config: {KAFKA_CONFIG}")

            consumer = get_kafka_consumer()
            processor = KafkaEventProcessor()

            events_data = []
            messages_processed = 0
            max_messages = 1000
            max_time_seconds = 120

            start_time = datetime.now()

            try:
                logger.info(f"Subscribed to topic: {KAFKA_CONFIG['topic']}")

                while messages_processed < max_messages:
                    elapsed_time = (datetime.now() - start_time).seconds
                    if elapsed_time > max_time_seconds:
                        logger.info(f"Reached time limit of {max_time_seconds} seconds")
                        break

                    messages = consumer.poll(timeout_ms=2000, max_records=50)

                    if not messages:
                        logger.debug("No messages received in poll, continuing...")
                        if elapsed_time > max_time_seconds - 5:
                            break
                        continue

                    for tp, message_batch in messages.items():
                        logger.debug(f"Processing {len(message_batch)} messages from partition {tp.partition}")

                        for message in message_batch:
                            if messages_processed >= max_messages:
                                logger.info(f"Reached message limit of {max_messages}")
                                break

                            try:
                                click_event = processor.process_kafka_message(message.value)
                                if click_event:
                                    event_dict = {
                                        'id': click_event.id,
                                        'type': click_event.type,
                                        'created_at': click_event.created_at.isoformat() if click_event.created_at else None,
                                        'received_at': click_event.received_at.isoformat() if click_event.received_at else None,
                                        'session_id': click_event.session_id,
                                        'ip': click_event.ip,
                                        'user_id': click_event.user_id,
                                        'url': click_event.url,
                                        'referrer': click_event.referrer,
                                        'device_type': click_event.device_type,
                                        'user_agent': click_event.user_agent,
                                    }

                                    if click_event.payload:
                                        event_dict['payload'] = {
                                            'event_title': click_event.payload.event_title,
                                            'element_id': click_event.payload.element_id,
                                            'x': click_event.payload.x,
                                            'y': click_event.payload.y,
                                            'element_text': click_event.payload.element_text,
                                            'element_class': click_event.payload.element_class,
                                            'page_title': click_event.payload.page_title,
                                            'viewport_width': click_event.payload.viewport_width,
                                            'viewport_height': click_event.payload.viewport_height,
                                            'scroll_position': click_event.payload.scroll_position,
                                            'timestamp_offset': click_event.payload.timestamp_offset,
                                            'metadata': click_event.payload.metadata
                                        }

                                    events_data.append(event_dict)
                                    messages_processed += 1

                                    if messages_processed % 500 == 0:
                                        logger.info(f"Processed {messages_processed} messages so far")

                            except Exception as e:
                                logger.warning(f"Error processing individual message: {e}")
                                continue

                    if messages_processed >= max_messages:
                        break

                try:
                    consumer.commit()
                    logger.info(f"Committed offsets for {messages_processed} messages")
                except Exception as e:
                    logger.warning(f"Failed to commit offsets: {e}")

            finally:
                consumer.close()
                logger.info(f"Closed Kafka consumer. Total processed: {messages_processed}")

            logger.info(f"Successfully extracted {len(events_data)} events from Kafka")

            if events_data:
                events_json = json.dumps(events_data, default=str)
                logger.info(f"Returning {len(events_data)} events as JSON")
                return events_json
            else:
                logger.info("No events extracted from Kafka - topic might be empty")
                return ""

        except Exception as e:
            logger.error(f"Failed to extract from Kafka: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return ""

    @task
    def load_to_cassandra(**context) -> Dict[str, int]:
        try:
            logger.info("Starting Cassandra load process")

            task_instance = context['ti']
            events_json = task_instance.xcom_pull(task_ids='extract_from_kafka')

            if not events_json:
                logger.info("No events to load from XCom - either topic was empty or extraction failed")
                return {'loaded': 0, 'failed': 0, 'total': 0}

            events_data = json.loads(events_json)

            if not events_data:
                logger.info("Empty events data")
                return {'loaded': 0, 'failed': 0, 'total': 0}

            logger.info(f"Loading {len(events_data)} events to Cassandra...")

            session, cluster = get_cassandra_session()
            processor = KafkaEventProcessor()

            insert_query = f"""
                INSERT INTO {CASSANDRA_CONFIG['keyspace']}.{CASSANDRA_CONFIG['table']} (
                    user_id, created_at, id, type, received_at, session_id,
                    ip, url, referrer, device_type, user_agent,
                    event_title, element_id, x, y, element_text, element_class,
                    page_title, viewport_width, viewport_height, scroll_position,
                    timestamp_offset, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            prepared_stmt = session.prepare(insert_query)
            prepared_stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM

            total_loaded = 0
            total_failed = 0
            batch_size = 50

            for i in range(0, len(events_data), batch_size):
                batch_events = events_data[i:i + batch_size]
                batch_params = []

                for event_dict in batch_events:
                    try:
                        click_event = ClickEvent.from_dict(event_dict)
                        params = processor.prepare_cassandra_params(click_event)
                        batch_params.append(params)
                    except Exception as e:
                        logger.error(f"Error preparing parameters for event: {e}")
                        total_failed += 1
                        continue

                if batch_params:
                    try:
                        loaded, failed = processor.execute_cassandra_batch(session, prepared_stmt, batch_params)
                        total_loaded += loaded
                        total_failed += failed
                        logger.info(f"Batch {i // batch_size + 1}: loaded {loaded}, failed {failed}")
                    except Exception as e:
                        logger.error(f"Error executing batch: {e}")
                        total_failed += len(batch_params)

            session.shutdown()
            cluster.shutdown()

            result = {
                'loaded': total_loaded,
                'failed': total_failed,
                'total': len(events_data)
            }

            logger.info(f"Cassandra load completed: {result}")
            return result

        except Exception as e:
            logger.error(f"Failed to load to Cassandra: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {'loaded': 0, 'failed': 1, 'total': 0}

    @task
    def log_results(results: Dict[str, int]):
        try:
            logger.info("=" * 50)
            logger.info("KAFKA TO CASSANDRA PIPELINE RESULTS")
            logger.info("=" * 50)
            logger.info(f"Total events processed: {results.get('total', 0)}")
            logger.info(f"Successfully loaded: {results.get('loaded', 0)}")
            logger.info(f"Failed to load: {results.get('failed', 0)}")

            if results.get('total', 0) > 0:
                success_rate = (results.get('loaded', 0) / results.get('total', 0)) * 100
                logger.info(f"Success rate: {success_rate:.2f}%")

            logger.info("=" * 50)

            if results.get('failed', 0) > 0:
                logger.warning(f"Some events failed to load: {results.get('failed', 0)} failures")

        except Exception as e:
            logger.error(f"Error logging results: {e}")

    extract_task = extract_from_kafka()
    load_task = load_to_cassandra()
    log_task = log_results(load_task)

    extract_task >> load_task >> log_task


dag = kafka_to_cassandra_pipeline()