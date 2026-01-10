import json
import uuid
from datetime import datetime
import pytest
import sys, os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.dag.minio_to_cassandra_dag import CSVParser, ClickEventTransformer


class TestCSVParser:

    @pytest.fixture
    def parser(self):
        return CSVParser

    @pytest.mark.parametrize("line,expected", [
        ('value1,value2,value3', ['value1', 'value2', 'value3']),
        ('"value,1","value""2","value\'3"', ['value,1', 'value"2', "value'3"]),
        ('"value""1","value,2",normal', ['value"1', 'value,2', 'normal']),
        ('value1,,value3,', ['value1', '', 'value3', '']),
        ('"test,comma","test""quote",normal', ['test,comma', 'test"quote', 'normal']),
    ])
    def test_parse_csv_line(self, parser, line, expected):
        result = parser.parse_csv_line(line)
        assert result == expected

    @pytest.mark.parametrize("invalid_input", [
        None,
        '',
        'null',
        'invalid_date',
        '2023-13-45',
        'not-a-date',
        '   ',
    ])
    def test_parse_datetime_invalid(self, parser, invalid_input):
        result = parser.parse_datetime(invalid_input)
        assert isinstance(result, datetime)

    @pytest.mark.parametrize("input_str,expected", [
        ("123", 123),
        ("456", 456),
        ("-789", -789),
        ("0", 0),
        ("  42  ", 42),
    ])
    def test_parse_int_valid(self, parser, input_str, expected):
        result = parser.parse_int(input_str)
        assert result == expected

    @pytest.mark.parametrize("invalid_input", [
        None,
        '',
        'null',
        'abc',
        '123.45',
        '12a34',
        '   ',
        '--123',
    ])
    def test_parse_int_invalid(self, parser, invalid_input):
        result = parser.parse_int(invalid_input)
        assert result is None

    @pytest.mark.parametrize("input_str,expected", [
        ("123.45", 123.45),
        ('"456.78"', 456.78),
        ("-789.12", -789.12),
        ("0.0", 0.0),
        ("3.14", 3.14),
        ("  -2.5  ", -2.5),
    ])
    def test_parse_float_valid(self, parser, input_str, expected):
        result = parser.parse_float(input_str)
        assert result == pytest.approx(expected)

    @pytest.mark.parametrize("invalid_input", [
        None,
        '',
        'null',
        'abc',
        '1.2.3',
        '   ',
    ])
    def test_parse_float_invalid(self, parser, invalid_input):
        result = parser.parse_float(invalid_input)
        assert result is None

    @pytest.mark.parametrize("input_str,expected", [
        ("normal", "normal"),
        ('"quoted"', "quoted"),
        ("'single'", "single"),
        ('""double""', '"double"'),
        ("''single''", "'single'"),
        ("  trimmed  ", "trimmed"),
        ('"  quoted with spaces  "', "  quoted with spaces  "),
    ])
    def test_parse_string_valid(self, parser, input_str, expected):
        result = parser.parse_string(input_str)
        assert result == expected

    @pytest.mark.parametrize("invalid_input,expected", [
        (None, None),
        ('', None),
        ('null', None),
        ('   ', None),
    ])
    def test_parse_string_invalid(self, parser, invalid_input, expected):
        result = parser.parse_string(invalid_input)
        assert result == expected

    @pytest.mark.parametrize("metadata_json,expected", [
        (
                '{"key1": "value1", "key2": 123, "key3": true}',
                {"key1": "value1", "key2": "123", "key3": "True"}
        ),
        (
                '{"nested": {"key": "value"}, "list": [1, 2, 3]}',
                {"nested": "{'key': 'value'}", "list": "[1, 2, 3]"}
        ),
        (
                '{"bool_true": true, "bool_false": false, "null_val": null}',
                {"bool_true": "True", "bool_false": "False", "null_val": "null"}
        ),
        (
                '{"float": 123.45, "int": 42, "string": "test"}',
                {"float": "123.45", "int": "42", "string": "test"}
        ),
        (
                '{}',
                {}
        ),
    ])
    def test_parse_metadata_valid(self, parser, metadata_json, expected):
        result = parser.parse_metadata(metadata_json)
        assert result == expected

    @pytest.mark.parametrize("invalid_input", [
        None,
        '',
        'null',
        'invalid json',
        '{"unclosed":',
        '[not_object]',
        '   '
    ])
    def test_parse_metadata_invalid(self, parser, invalid_input):
        result = parser.parse_metadata(invalid_input)
        assert result == {}


class TestClickEventTransformer:

    @pytest.fixture
    def transformer(self):
        return ClickEventTransformer

    @pytest.fixture
    def sample_values(self):
        return [
            "event_123",  # id
            "click",  # type
            "2023-01-15T10:30:00",  # createdAt
            "2023-01-15T10:30:05",  # receivedAt
            "session_456",  # sessionId
            "192.168.1.1",  # ip
            "789",  # userId
            "https://example.com",  # url
            "https://google.com",  # referrer
            "desktop",  # deviceType
            "Mozilla/5.0",  # userAgent
            "Button Click",  # payload.eventTitle
            "btn-submit",  # payload.elementId
            "100",  # payload.x
            "200",  # payload.y
            "Submit",  # payload.elementText
            "btn btn-primary",  # payload.elementClass
            "Example Page",  # payload.pageTitle
            "1920",  # payload.viewportWidth
            "1080",  # payload.viewportHeight
            "150.5",  # payload.scrollPosition
            "5000",  # payload.timestampOffset
            '{"browser": "chrome", "version": "120"}',  # payload.metadata
        ]

    def test_transform_csv_to_clickevent_valid(self, transformer, sample_values):
        event = transformer.transform_csv_to_clickevent(sample_values)

        assert event.id == "event_123"
        assert event.type == "click"
        assert event.created_at == datetime(2023, 1, 15, 10, 30, 0)
        assert event.received_at == datetime(2023, 1, 15, 10, 30, 5)
        assert event.session_id == "session_456"
        assert event.ip == "192.168.1.1"
        assert event.user_id == 789
        assert event.url == "https://example.com"
        assert event.referrer == "https://google.com"
        assert event.device_type == "desktop"
        assert event.user_agent == "Mozilla/5.0"

        assert event.payload is not None
        assert event.payload.event_title == "Button Click"
        assert event.payload.element_id == "btn-submit"
        assert event.payload.x == 100
        assert event.payload.y == 200
        assert event.payload.element_text == "Submit"
        assert event.payload.element_class == "btn btn-primary"
        assert event.payload.page_title == "Example Page"
        assert event.payload.viewport_width == 1920
        assert event.payload.viewport_height == 1080
        assert event.payload.scroll_position == 150.5
        assert event.payload.timestamp_offset == 5000
        assert event.payload.metadata == {"browser": "chrome", "version": "120"}

    @pytest.mark.parametrize("column_count", [1, 5, 10, 22, 24])
    def test_transform_csv_to_clickevent_wrong_column_count(self, transformer, column_count):
        values = ["value"] * column_count

        with pytest.raises(ValueError) as exc_info:
            transformer.transform_csv_to_clickevent(values)

        assert "Expected 23 columns" in str(exc_info.value)

    def test_transform_csv_to_clickevent_empty_id(self, transformer, sample_values):
        sample_values[0] = ""

        event = transformer.transform_csv_to_clickevent(sample_values)

        assert event.id is not None
        try:
            uuid.UUID(event.id)
            is_valid_uuid = True
        except ValueError:
            is_valid_uuid = False

        assert is_valid_uuid

    def test_transform_csv_to_clickevent_null_values(self, transformer, sample_values):
        sample_values[6] = "null"  # userId
        sample_values[11] = "null"  # payload.eventTitle
        sample_values[22] = "null"  # payload.metadata

        event = transformer.transform_csv_to_clickevent(sample_values)

        assert event.user_id == 0
        assert event.payload.event_title is None
        assert event.payload.metadata == {}

    def test_transform_csv_to_clickevent_empty_payload(self, transformer, sample_values):
        for i in range(11, 23):
            sample_values[i] = ""

        event = transformer.transform_csv_to_clickevent(sample_values)

        assert event.payload is None

    @pytest.mark.parametrize("input_type,expected_type", [
        ("CLICK", "click"),
        ("Click", "click"),
        ("CLICK ", "click"),
        (" click ", "click"),
        ("VIEW", "view"),
        ("View", "view"),
        ("SCROLL", "scroll"),
        ("UNKNOWN", "unknown"),
    ])
    def test_transform_csv_to_clickevent_type_normalization(
            self, transformer, sample_values, input_type, expected_type
    ):
        sample_values[1] = input_type

        event = transformer.transform_csv_to_clickevent(sample_values)

        assert event.type == expected_type

    def test_transform_csv_to_clickevent_malformed_metadata(self, transformer, sample_values):
        sample_values[22] = '{"malformed": json'

        event = transformer.transform_csv_to_clickevent(sample_values)

        assert event.payload.metadata == {}

    def test_transform_csv_to_clickevent_negative_coordinates(self, transformer, sample_values):
        sample_values[13] = "-50"
        sample_values[14] = "-100"

        event = transformer.transform_csv_to_clickevent(sample_values)

        assert event.payload.x == -50
        assert event.payload.y == -100


class TestEdgeCases:

    @pytest.fixture
    def parser(self):
        return CSVParser

    @pytest.mark.parametrize("metadata,expected_key,expected_value", [
        (
                {"nested": {"key": "value"}},
                "nested",
                "{'key': 'value'}"
        ),
        (
                {"list": [1, 2, 3]},
                "list",
                "[1, 2, 3]"
        ),
        (
                {"null_value": None},
                "null_value",
                "null"
        ),
        (
                {"bool_true": True, "bool_false": False},
                "bool_true",
                "True"
        ),
        (
                {"number": 123.45},
                "number",
                "123.45"
        )
    ])
    def test_parse_metadata_complex(self, parser, metadata, expected_key, expected_value):
        metadata_json = json.dumps(metadata)
        result = parser.parse_metadata(metadata_json)

        assert expected_key in result
        assert result[expected_key] == expected_value

    def test_parse_csv_line_with_tabs_and_spaces(self, parser):
        line = '  value1  ,\tvalue2\t, " value3 " '
        result = parser.parse_csv_line(line)

        assert result == ['value1', 'value2', 'value3']

    def test_parse_datetime_with_timezone(self, parser):
        input_str = "2023-01-15T10:30:00+03:00"
        result = parser.parse_datetime(input_str)

        assert isinstance(result, datetime)

    @pytest.mark.parametrize("malformed_csv", [
        'value1,"unclosed quote,value2',
        'value1,value2"extra quote"',
        ',',
        '',
        ' ',
    ])
    def test_parse_csv_line_malformed(self, parser, malformed_csv):
        result = parser.parse_csv_line(malformed_csv)
        assert isinstance(result, list)


class TestIntegrationScenarios:

    @pytest.fixture
    def transformer(self):
        return ClickEventTransformer

    def test_full_pipeline_with_various_data(self, transformer):
        test_cases = [
            {
                "input": [
                    "test_1", "click", "2023-01-01T12:00:00", "2023-01-01T12:00:01",
                    "session_1", "127.0.0.1", "100", "https://test.com", "",
                    "mobile", "Chrome", "Home", "logo", "50", "60",
                    "Logo", "image", "Home Page", "375", "667", "0",
                    "1000", '{"test": "data"}'
                ],
                "expected_user_id": 100,
                "expected_url": "https://test.com"
            },
            {
                "input": [
                    "", "view", "2023-02-01T15:30:00", "",
                    "session_2", "", "null", "", "",
                    "", "", "", "", "", "",
                    "", "", "", "", "", "",
                    "", "null"
                ],
                "expected_user_id": 0,
                "expected_url": None
            },
        ]

        for test_case in test_cases:
            event = transformer.transform_csv_to_clickevent(test_case["input"])

            assert event.user_id == test_case["expected_user_id"]
            if test_case["expected_url"]:
                assert event.url == test_case["expected_url"]
            else:
                assert event.url is None

            assert event is not None
            assert isinstance(event.type, str)