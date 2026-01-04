from datetime import datetime, timedelta
from airflow.decorators import dag, task
import logging
import json
import uuid
from typing import List, Optional, Dict, Any
from minio import Minio
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy

from data_classes import ClickEventPayload, ClickEvent

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

logger = logging.getLogger(__name__)


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


def get_cassandra_session():
    try:
        cluster = Cluster(
            CASSANDRA_CONFIG['hosts'],
            port=CASSANDRA_CONFIG['port'],
            load_balancing_policy=RoundRobinPolicy(),
            protocol_version=4
        )
        session = cluster.connect(CASSANDRA_CONFIG['keyspace'])

        session.set_keyspace(CASSANDRA_CONFIG['keyspace'])

        logger.info("Successfully connected to Cassandra")
        return session, cluster
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {e}")
        raise


class CSVParser:

    CSV_HEADERS = [
        "id", "type", "createdAt", "receivedAt", "sessionId", "ip", "userId",
        "url", "referrer", "deviceType", "userAgent", "payload.eventTitle",
        "payload.elementId", "payload.x", "payload.y", "payload.elementText",
        "payload.elementClass", "payload.pageTitle", "payload.viewportWidth",
        "payload.viewportHeight", "payload.scrollPosition", "payload.timestampOffset",
        "payload.metadata"
    ]

    @staticmethod
    def parse_csv_line(line: str) -> List[str]:
        result = []
        current_field = []
        in_quotes = False
        quote_char = None

        i = 0
        while i < len(line):
            char = line[i]

            if char in ('"', "'"):
                if not in_quotes:
                    in_quotes = True
                    quote_char = char
                elif quote_char == char:
                    if i + 1 < len(line) and line[i + 1] == quote_char:
                        current_field.append(char)
                        i += 1
                    else:
                        in_quotes = False
                        quote_char = None
                else:
                    current_field.append(char)
            elif char == ',' and not in_quotes:
                result.append(''.join(current_field).strip())
                current_field = []
            elif char == '\\' and i + 1 < len(line):
                current_field.append(line[i + 1])
                i += 1
            else:
                current_field.append(char)

            i += 1

        result.append(''.join(current_field).strip())
        return result

    @staticmethod
    def parse_datetime(value: str) -> datetime:
        if not value or value.lower() == 'null' or value == '':
            return datetime.now()
        try:
            value = value.strip('"\'')
            for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]:
                try:
                    return datetime.strptime(value, fmt)
                except ValueError:
                    continue
            return datetime.now()
        except Exception as e:
            logger.warning(f"Failed to parse datetime '{value}': {e}")
            return datetime.now()

    @staticmethod
    def parse_int(value: str) -> Optional[int]:
        if not value or value.lower() == 'null' or value == '':
            return None
        try:
            value = value.strip('"\'')
            return int(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def parse_float(value: str) -> Optional[float]:
        if not value or value.lower() == 'null' or value == '':
            return None
        try:
            value = value.strip('"\'')
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def parse_string(value: str) -> Optional[str]:
        if not value or value.lower() == 'null' or value == '':
            return None
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or \
                (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        value = value.replace('""', '"').replace("''", "'")
        return value if value != '' else None

    @staticmethod
    def parse_metadata(metadata_str: str) -> Dict[str, Any]:
        if not metadata_str or metadata_str.lower() == 'null' or metadata_str == '':
            return {}

        try:
            metadata_str = metadata_str.strip()
            if (metadata_str.startswith('"') and metadata_str.endswith('"')) or \
                    (metadata_str.startswith("'") and metadata_str.endswith("'")):
                metadata_str = metadata_str[1:-1]

            metadata_str = metadata_str.replace('""', '"')

            metadata = json.loads(metadata_str)

            result = {}
            for key, value in metadata.items():
                if value is None:
                    result[key] = "null"
                elif isinstance(value, (int, float, bool)):
                    result[key] = str(value)
                else:
                    result[key] = str(value)
            return result

        except (json.JSONDecodeError, TypeError) as e:
            logger.warning(f"Failed to parse metadata '{metadata_str[:50]}...': {e}")
            return {}


class ClickEventTransformer:

    @staticmethod
    def transform_csv_to_clickevent(values: List[str]) -> ClickEvent:
        if len(values) != len(CSVParser.CSV_HEADERS):
            raise ValueError(
                f"Expected {len(CSVParser.CSV_HEADERS)} columns, got {len(values)}. "
                f"Headers: {CSVParser.CSV_HEADERS}"
            )

        metadata = CSVParser.parse_metadata(values[22])

        # Создаем payload
        payload = ClickEventPayload(
            event_title=CSVParser.parse_string(values[11]),  # payload.eventTitle
            element_id=CSVParser.parse_string(values[12]),  # payload.elementId
            x=CSVParser.parse_int(values[13]),  # payload.x
            y=CSVParser.parse_int(values[14]),  # payload.y
            element_text=CSVParser.parse_string(values[15]),  # payload.elementText
            element_class=CSVParser.parse_string(values[16]),  # payload.elementClass
            page_title=CSVParser.parse_string(values[17]),  # payload.pageTitle
            viewport_width=CSVParser.parse_int(values[18]),  # payload.viewportWidth
            viewport_height=CSVParser.parse_int(values[19]),  # payload.viewportHeight
            scroll_position=CSVParser.parse_float(values[20]),  # payload.scrollPosition
            timestamp_offset=CSVParser.parse_int(values[21]),  # payload.timestampOffset
            metadata=metadata
        )

        event_id = values[0].strip()
        if not event_id or event_id.lower() == 'null' or event_id == '':
            event_id = str(uuid.uuid4())
        else:
            event_id = CSVParser.parse_string(event_id)

        event_type = CSVParser.parse_string(values[1]) or ''
        event_type = event_type.lower()

        return ClickEvent(
            id=event_id,
            type=event_type,
            created_at=CSVParser.parse_datetime(values[2]),  # createdAt
            received_at=CSVParser.parse_datetime(values[3]),  # receivedAt
            session_id=CSVParser.parse_string(values[4]),  # sessionId
            ip=CSVParser.parse_string(values[5]),  # ip
            user_id=CSVParser.parse_int(values[6]) or 0,  # userId
            url=CSVParser.parse_string(values[7]),  # url
            referrer=CSVParser.parse_string(values[8]),  # referrer
            device_type=CSVParser.parse_string(values[9]) or '',  # deviceType
            user_agent=CSVParser.parse_string(values[10]) or '',  # userAgent
            payload=payload if any([
                payload.event_title, payload.element_id, payload.x, payload.y,
                payload.element_text, payload.element_class, payload.page_title,
                payload.viewport_width, payload.viewport_height, payload.scroll_position,
                payload.timestamp_offset, payload.metadata
            ]) else None
        )


@dag(
    'minio_to_cassandra_pipeline',
    description='Pipeline to load data from MinIO to Cassandra',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=['clickstream', 'etl', 'cassandra', 'minio'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    }
)
def minio_to_cassandra_pipeline():
    @task
    def list_minio_files(**context):
        try:
            minio_client = get_minio_client()
            bucket = MINIO_CONFIG['bucket']

            objects = minio_client.list_objects(bucket, recursive=True)
            files = [obj.object_name for obj in objects if obj.object_name.lower().endswith('.csv')]

            if not files:
                all_objects = minio_client.list_objects(bucket, recursive=True)
                all_files = [obj.object_name for obj in all_objects]
                logger.info(f"All files in bucket: {all_files}")

                files = [f for f in all_files if not f.lower().endswith(('.txt', '.json', '.parquet'))]

            logger.info(f"Found {len(files)} files in MinIO bucket {bucket}")

            if files:
                for file in files[:10]:
                    logger.info(f"  - {file}")
                if len(files) > 10:
                    logger.info(f"  ... and {len(files) - 10} more files")

            context['ti'].xcom_push(key='minio_files', value=files)

            return len(files)

        except Exception as e:
            logger.error(f"Failed to list MinIO files: {e}")
            raise

    @task
    def extract_and_transform_data(**context):
        try:
            minio_client = get_minio_client()
            bucket = MINIO_CONFIG['bucket']

            files = context['ti'].xcom_pull(key='minio_files', task_ids='list_minio_files')

            if not files:
                logger.warning("No files to process")
                return 0

            all_events = []
            processed_files = 0

            for file_path in files:
                logger.info(f"Processing file: {file_path}")

                try:
                    response = minio_client.get_object(bucket, file_path)
                    content = response.read().decode('utf-8')
                    response.close()
                    response.release_conn()

                    lines = content.strip().split('\n')
                    if not lines:
                        logger.warning(f"File {file_path} is empty")
                        continue

                    headers_line = lines[0]
                    expected_headers = CSVParser.CSV_HEADERS

                    parsed_headers = CSVParser.parse_csv_line(headers_line)
                    logger.info(f"File headers ({len(parsed_headers)}): {parsed_headers}")
                    logger.info(f"Expected headers ({len(expected_headers)}): {expected_headers}")

                    if len(parsed_headers) != len(expected_headers):
                        logger.warning(
                            f"Header count mismatch in {file_path}: "
                            f"expected {len(expected_headers)}, got {len(parsed_headers)}"
                        )

                    start_index = 1
                    file_events_count = 0

                    for line_num, line in enumerate(lines[start_index:], start=start_index + 1):
                        if not line.strip():
                            continue

                        try:
                            values = CSVParser.parse_csv_line(line)

                            if line_num == start_index + 1:
                                logger.info(f"First data line values ({len(values)}): {values}")
                                if len(values) > 22:
                                    logger.info(f"Metadata string: {values[22][:100]}...")

                            event = ClickEventTransformer.transform_csv_to_clickevent(values)

                            if (event.user_id and event.user_id > 0 and
                                    event.type in ['view', 'click']):
                                all_events.append(event)
                                file_events_count += 1
                            else:
                                logger.debug(
                                    f"Skipped event at line {line_num}: "
                                    f"user_id={event.user_id}, type={event.type}"
                                )

                        except Exception as e:
                            logger.warning(
                                f"Error processing line {line_num} in file {file_path}: {e}\n"
                                f"Line: {line[:100]}..."
                            )
                            continue

                    logger.info(f"Processed {len(lines) - 1} lines from {file_path}, "
                                f"extracted {file_events_count} valid events")
                    processed_files += 1

                except Exception as e:
                    logger.error(f"Error processing file {file_path}: {e}")
                    continue

            logger.info(f"Total: processed {processed_files} files, "
                        f"extracted {len(all_events)} valid events")

            events_data = []
            for event in all_events:
                event_dict = {
                    'id': event.id,
                    'type': event.type,
                    'created_at': event.created_at.isoformat(),
                    'received_at': event.received_at.isoformat(),
                    'session_id': event.session_id,
                    'ip': event.ip,
                    'user_id': event.user_id,
                    'url': event.url,
                    'referrer': event.referrer,
                    'device_type': event.device_type,
                    'user_agent': event.user_agent,
                }

                if event.payload:
                    event_dict['payload'] = {
                        'event_title': event.payload.event_title,
                        'element_id': event.payload.element_id,
                        'x': event.payload.x,
                        'y': event.payload.y,
                        'element_text': event.payload.element_text,
                        'element_class': event.payload.element_class,
                        'page_title': event.payload.page_title,
                        'viewport_width': event.payload.viewport_width,
                        'viewport_height': event.payload.viewport_height,
                        'scroll_position': event.payload.scroll_position,
                        'timestamp_offset': event.payload.timestamp_offset,
                        'metadata': event.payload.metadata
                    }

                events_data.append(event_dict)

            context['ti'].xcom_push(key='click_events', value=json.dumps(events_data))
            logger.info(f"Successfully extracted and transformed {len(all_events)} events")

            return len(all_events)

        except Exception as e:
            logger.error(f"Failed to extract and transform data: {e}")
            raise

    @task
    def load_to_cassandra(**context):
        try:
            events_json = context['ti'].xcom_pull(key='click_events', task_ids='extract_and_transform_data')

            if not events_json:
                logger.warning("No events to load")
                return 0

            events_data = json.loads(events_json)

            if not events_data:
                logger.warning("Empty events data")
                return 0

            logger.info(f"Loading {len(events_data)} events to Cassandra...")

            session, cluster = get_cassandra_session()

            insert_query = f"""
                INSERT INTO {CASSANDRA_CONFIG['table']} (
                    user_id, created_at, id, type, received_at, session_id,
                    ip, url, referrer, device_type, user_agent,
                    event_title, element_id, x, y, element_text, element_class,
                    page_title, viewport_width, viewport_height, scroll_position,
                    timestamp_offset, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            prepared = session.prepare(insert_query)

            prepared.consistency_level = 1

            batch_size = 50
            inserted_count = 0
            failed_count = 0

            for i in range(0, len(events_data), batch_size):
                batch = events_data[i:i + batch_size]
                batch_statements = []

                for event_dict in batch:
                    try:
                        created_at = event_dict['created_at']
                        if isinstance(created_at, str):
                            created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))

                        received_at = event_dict['received_at']
                        if isinstance(received_at, str):
                            received_at = datetime.fromisoformat(received_at.replace('Z', '+00:00'))

                        user_id = event_dict['user_id']
                        event_id = event_dict['id']
                        event_type = event_dict['type']
                        session_id = event_dict['session_id']
                        ip = event_dict['ip']
                        url = event_dict['url']
                        referrer = event_dict['referrer']
                        device_type = event_dict['device_type']
                        user_agent = event_dict['user_agent']

                        payload_data = event_dict.get('payload', {})
                        event_title = payload_data.get('event_title')
                        element_id = payload_data.get('element_id')
                        x = payload_data.get('x')
                        y = payload_data.get('y')
                        element_text = payload_data.get('element_text')
                        element_class = payload_data.get('element_class')
                        page_title = payload_data.get('page_title')
                        viewport_width = payload_data.get('viewport_width')
                        viewport_height = payload_data.get('viewport_height')
                        scroll_position = payload_data.get('scroll_position')
                        timestamp_offset = payload_data.get('timestamp_offset')
                        metadata = payload_data.get('metadata', {})

                        bound_statement = prepared.bind((
                            user_id,
                            created_at,
                            event_id,
                            event_type,
                            received_at,
                            session_id,
                            ip,
                            url,
                            referrer,
                            device_type,
                            user_agent,
                            event_title,
                            element_id,
                            x,
                            y,
                            element_text,
                            element_class,
                            page_title,
                            viewport_width,
                            viewport_height,
                            scroll_position,
                            timestamp_offset,
                            metadata
                        ))

                        batch_statements.append(bound_statement)

                    except Exception as e:
                        logger.error(f"Error preparing event {event_dict.get('id', 'unknown')}: {e}")
                        failed_count += 1
                        continue

                if batch_statements:
                    try:
                        for statement in batch_statements:
                            session.execute(statement)

                        inserted_count += len(batch_statements)
                        logger.info(f"Inserted batch of {len(batch_statements)} events (total: {inserted_count})")

                    except Exception as e:
                        logger.error(f"Error inserting batch: {e}")
                        for statement in batch_statements:
                            try:
                                session.execute(statement)
                                inserted_count += 1
                            except Exception as single_e:
                                logger.error(f"Error inserting single event: {single_e}")
                                failed_count += 1

            session.shutdown()
            cluster.shutdown()

            logger.info(f"Successfully loaded {inserted_count} events to Cassandra "
                        f"({failed_count} failed)")

            return inserted_count

        except Exception as e:
            logger.error(f"Failed to load data to Cassandra: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

    list_files_task = list_minio_files()
    transform_task = extract_and_transform_data()
    load_task = load_to_cassandra()

    list_files_task >> transform_task >> load_task


dag = minio_to_cassandra_pipeline()