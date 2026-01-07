import pytest
from datetime import datetime, timedelta
import uuid
import sys, os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.dag.cassandra_to_cassandra_dag import CassandraEventProcessor, ValidationStatus
from src.data.data_classes import RawClickEvent, CleanClickEvent, InvalidEvent


@pytest.fixture
def processor():
    return CassandraEventProcessor()


@pytest.fixture
def now():
    return datetime.now()


@pytest.fixture
def valid_raw_event(now):
    return RawClickEvent(
        id=str(uuid.uuid4()),
        type='CLICK',
        user_id=123,
        session_id='session_123',
        url='https://example.com',
        user_agent='Mozilla/5.0',
        created_at=now - timedelta(hours=1),
        device_type='DESKTOP',
        element_id='button_1'
    )


class TestCassandraEventProcessorWithFixtures:

    def test_validate_event_valid(self, processor, valid_raw_event):
        status, errors = processor.validate_event(valid_raw_event)

        assert status == ValidationStatus.VALID
        assert len(errors) == 0

    def test_validate_event_missing_required_fields(self, processor):
        event = RawClickEvent(
            id=None,
            type=None,
            user_id=None,
            session_id=None,
            url=None,
            user_agent=None,
            created_at=None
        )

        status, errors = processor.validate_event(event)

        assert status == ValidationStatus.INVALID
        assert len(errors) > 0
        assert "Event ID is required" in errors
        assert "Event type is required" in errors
        assert "User ID is required" in errors
        assert "Session ID is required" in errors
        assert "URL is required" in errors
        assert "User agent is required" in errors
        assert "Created at timestamp is required" in errors

    def test_validate_event_invalid_event_type(self, now, processor):
        event = RawClickEvent(
            id=str(uuid.uuid4()),
            type='INVALID_TYPE',
            user_id=123,
            session_id='session_123',
            url='https://example.com',
            user_agent='Mozilla/5.0',
            created_at=now - timedelta(hours=1)
        )

        status, errors = processor.validate_event(event)

        assert status == ValidationStatus.INVALID
        assert "Invalid event type: INVALID_TYPE" in errors

    def test_validate_event_negative_user_id(self, now, processor):
        event = RawClickEvent(
            id=str(uuid.uuid4()),
            type='CLICK',
            user_id=-1,
            session_id='session_123',
            url='https://example.com',
            user_agent='Mozilla/5.0',
            created_at=now - timedelta(hours=1)
        )

        status, errors = processor.validate_event(event)

        assert status == ValidationStatus.INVALID
        assert "User ID must be positive: -1" in errors

    def test_validate_event_invalid_device_type(self, now, processor):
        event = RawClickEvent(
            id=str(uuid.uuid4()),
            type='CLICK',
            user_id=123,
            session_id='session_123',
            url='https://example.com',
            user_agent='Mozilla/5.0',
            created_at=now - timedelta(hours=1),
            device_type='INVALID_DEVICE'
        )

        status, errors = processor.validate_event(event)

        assert status == ValidationStatus.INVALID
        assert "Invalid device type: INVALID_DEVICE" in errors

    def test_validate_event_missing_element_id_for_event(self, now, processor):
        event = RawClickEvent(
            id=str(uuid.uuid4()),
            type='CLICK',
            user_id=123,
            session_id='session_123',
            url='https://example.com',
            user_agent='Mozilla/5.0',
            created_at=now - timedelta(hours=1),
            device_type='DESKTOP',
            element_id=None
        )

        status, errors = processor.validate_event(event)

        assert status == ValidationStatus.INVALID
        assert "Element ID is required for events" in errors

    def test_prepare_clean_event_params(self, now, processor):
        event = CleanClickEvent(
            id='test_id',
            type='CLICK',
            created_at=now,
            session_id='session_123',
            user_id=123,
            url='https://example.com',
            device_type='DESKTOP',
            element_id='button_1',
            x=100,
            y=200,
            event_title='Test Event'
        )

        params = processor.prepare_clean_event_params(event)

        assert params == (
            123,  # user_id
            now,  # created_at
            'test_id',  # id
            'CLICK',  # type
            'session_123',  # session_id
            'https://example.com',  # url
            'DESKTOP',  # device_type
            'Test Event',  # event_title
            'button_1',  # element_id
            100,  # x
            200  # y
        )

    def test_prepare_invalid_event_params(self, now, processor):
        event = InvalidEvent(
            user_id=123,
            id='test_id',
            type='CLICK',
            session_id='session_123',
            event_time=now,
            processed_at=now,
            validation_error='Test error'
        )

        params = processor.prepare_invalid_event_params(event)

        assert params == (
            123,  # user_id
            'test_id',  # id
            'CLICK',  # type
            'session_123',  # session_id
            now,  # event_time
            now,  # processed_at
            'Test error'  # validation_error
        )


class TestValidationStatus:

    def test_validation_status_values(self):
        assert ValidationStatus.VALID.value == "VALID"
        assert ValidationStatus.INVALID.value == "INVALID"
        assert ValidationStatus.SUSPICIOUS.value == "SUSPICIOUS"

    def test_validation_status_membership(self):
        assert "VALID" in [status.value for status in ValidationStatus]
        assert "INVALID" in [status.value for status in ValidationStatus]
        assert "SUSPICIOUS" in [status.value for status in ValidationStatus]