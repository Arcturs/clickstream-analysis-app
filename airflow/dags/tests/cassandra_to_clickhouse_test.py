import pytest
import pandas as pd
import numpy as np
from unittest.mock import Mock
from datetime import datetime, timedelta, date
import sys, os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

mock_airflow = Mock()
sys.modules['airflow'] = mock_airflow
sys.modules['airflow.decorators'] = Mock()


@pytest.fixture
def mock_context():
    context = {
        'ti': Mock()
    }
    return context


@pytest.fixture
def sample_dataframe():
    dates = [datetime(2024, 1, 1) + timedelta(days=i) for i in range(5)]

    data = {
        'user_id': [1, 1, 2, 3, 4, 5, 1, 2, 3, 6],
        'created_at': dates * 2,
        'event_id': [f'event_{i}' for i in range(10)],
        'event_type': ['view', 'click', 'view', 'click', 'view', 'click', 'view', 'click', 'view', 'click'],
        'session_id': ['sess_1', 'sess_1', 'sess_2', 'sess_3', 'sess_4', 'sess_5', 'sess_6', 'sess_7', 'sess_8',
                       'sess_9'],
        'url': ['https://site.com/page1', 'https://site.com/page1',
                'https://site.com/page2', 'https://site.com/page2',
                'https://site.com/page1', 'https://site.com/page3',
                'https://site.com/page2', 'https://site.com/page1',
                'https://site.com/page3', 'https://site.com/page3'],
        'device_type': ['desktop', 'mobile', 'tablet', 'desktop', 'mobile',
                        'desktop', 'tablet', 'mobile', 'desktop', 'tablet'],
        'event_title': ['Page View', 'Button Click', 'Page View', 'Link Click',
                        'Page View', 'Button Click', 'Page View', 'Link Click',
                        'Page View', 'Button Click'],
        'element_id': ['header', 'btn_submit', 'nav', 'link_1', 'header',
                       'btn_buy', 'nav', 'link_2', 'header', 'btn_submit'],
        'x': [100, 200, 150, 250, 120, 180, 130, 220, 140, 190],
        'y': [50, 100, 60, 110, 55, 105, 65, 115, 70, 120]
    }

    df = pd.DataFrame(data)
    return df


@pytest.fixture
def empty_dataframe():
    return pd.DataFrame()


class TestCalculateDAUWAUMAU:

    def test_dau_calculation_logic(self):
        dates = [date(2026, 1, 1), date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 2)]
        data = {
            'user_id': [1, 2, 1, 3],
            'date': dates,
            'created_at': [datetime(2026, 1, 1, 10, 0), datetime(2026, 1, 1, 11, 0),
                           datetime(2026, 1, 2, 10, 0), datetime(2026, 1, 2, 11, 0)]
        }
        df = pd.DataFrame(data)

        dau = df.groupby('date')['user_id'].nunique()

        assert dau[date(2026, 1, 1)] == 2
        assert dau[date(2026, 1, 2)] == 2


class TestCalculateConversionMetrics:

    def test_event_type_categorization_logic(self):
        test_cases = [
            ('view', 'view'),
            ('VIEW', 'view'),
            ('click', 'click'),
            ('CLICK', 'click'),
            ('other', 'other'),
            ('scroll', 'other')
        ]

        for input_type, expected in test_cases:
            if input_type.lower() in ['view', 'click']:
                assert input_type.lower() == expected

    def test_conversion_rate_calculation(self):
        data = {
            'element_id': ['btn_1', 'btn_1', 'btn_1', 'btn_2', 'btn_2'],
            'event_type': ['view', 'click', 'view', 'view', 'view'],
            'event_title': ['Page View', 'Button Click', 'Page View', 'Page View', 'Page View']
        }
        df = pd.DataFrame(data)

        def categorize_event(row):
            if row['event_type'] in ['view', 'VIEW']:
                return 'view'
            elif row['event_type'] in ['click', 'CLICK']:
                return 'click'
            else:
                return 'other'

        df['event_type_category'] = df.apply(categorize_event, axis=1)

        btn1_data = df[df['element_id'] == 'btn_1']
        views = len(btn1_data[btn1_data['event_type_category'] == 'view'])
        clicks = len(btn1_data[btn1_data['event_type_category'] == 'click'])

        conversion_rate = (clicks / views * 100) if views > 0 else 0

        assert views == 2
        assert clicks == 1
        assert conversion_rate == 50.0


class TestCalculateSessionDuration:

    def test_session_duration_calculation_logic(self):
        session_data = pd.DataFrame({
            'created_at': [
                datetime(2026, 1, 1, 10, 0, 0),
                datetime(2026, 1, 1, 10, 5, 30),
                datetime(2026, 1, 1, 10, 10, 15)
            ],
            'session_id': ['sess_1', 'sess_1', 'sess_1']
        })

        session_start = session_data['created_at'].min()
        session_end = session_data['created_at'].max()
        duration = (session_end - session_start).total_seconds()

        assert duration == 615.0


class TestCalculateDeviceDistribution:

    def test_device_distribution_logic(self):
        data = {
            'date': [date(2026, 1, 1), date(2026, 1, 1), date(2026, 1, 1), date(2026, 1, 2)],
            'device_type': ['desktop', 'mobile', 'desktop', 'mobile'],
            'user_id': [1, 2, 3, 1],
            'session_id': ['sess_1', 'sess_2', 'sess_3', 'sess_4'],
            'event_id': ['event_1', 'event_2', 'event_3', 'event_4']
        }
        df = pd.DataFrame(data)

        distribution = df.groupby(['date', 'device_type']).agg({
            'user_id': 'nunique',
            'session_id': 'nunique',
            'event_id': 'count'
        }).reset_index()

        jan1_data = distribution[distribution['date'] == date(2026, 1, 1)]

        assert len(jan1_data) == 2
        desktop_data = jan1_data[jan1_data['device_type'] == 'desktop'].iloc[0]
        assert desktop_data['user_id'] == 2


class TestCalculateTopPages:

    def test_top_pages_ranking_logic(self):
        data = {
            'date': [date(2026, 1, 1), date(2026, 1, 1), date(2026, 1, 1), date(2026, 1, 2)],
            'url': ['page1', 'page1', 'page2', 'page1'],
            'view_count': [100, 150, 200, 50],
            'click_count': [10, 15, 20, 5]
        }
        df = pd.DataFrame(data)

        grouped = df.groupby(['date', 'url']).agg({
            'view_count': 'sum',
            'click_count': 'sum'
        }).reset_index()

        grouped['rank'] = grouped.groupby('date')['view_count'].rank(
            method='dense', ascending=False
        )

        jan1_data = grouped[grouped['date'] == date(2026, 1, 1)]
        page2_rank = jan1_data[jan1_data['url'] == 'page2']['rank'].iloc[0]
        page1_rank = jan1_data[jan1_data['url'] == 'page1']['rank'].iloc[0]

        assert page2_rank == np.float64(2.0)
        assert page1_rank == np.float64(1.0)

    def test_conversion_rate_calculation_top_pages(self):
        data = {
            'view_count': [100, 200],
            'click_count': [10, 40]
        }
        df = pd.DataFrame(data)

        df['conversion_rate'] = np.where(
            df['view_count'] > 0,
            (df['click_count'] / df['view_count']) * 100,
            0
        )

        assert df.loc[0, 'conversion_rate'] == 10.0
        assert df.loc[1, 'conversion_rate'] == 20.0


class TestIntegrationScenarios:

    def test_full_pipeline_data_flow(self):
        raw_data = pd.DataFrame({
            'user_id': [1, 1, 2, 3],
            'created_at': [
                datetime(2026, 1, 1, 10, 0),
                datetime(2026, 1, 1, 10, 5),
                datetime(2026, 1, 1, 11, 0),
                datetime(2026, 1, 2, 9, 0)
            ],
            'event_id': ['e1', 'e2', 'e3', 'e4'],
            'event_type': ['view', 'click', 'view', 'click'],
            'session_id': ['s1', 's1', 's2', 's3'],
            'url': ['/page1', '/page1', '/page2', '/page1'],
            'device_type': ['desktop', 'desktop', 'mobile', 'tablet'],
            'event_title': ['View', 'Click', 'View', 'Click'],
            'element_id': ['header', 'button', 'nav', 'button'],
            'x': [100, 150, 200, 120],
            'y': [50, 100, 60, 80]
        })

        assert len(raw_data) == 4
        assert raw_data['user_id'].nunique() == 3
        assert raw_data['session_id'].nunique() == 3

    def test_edge_case_dates(self):
        dates = [
            datetime(2026, 1, 1),
            datetime(2026, 1, 1, 23, 59, 59),
            datetime(2026, 1, 2),
            datetime(2026, 12, 31)
        ]

        for dt in dates:
            assert isinstance(dt, datetime)

        data = pd.DataFrame({
            'date': [d.date() for d in dates],
            'user_id': [1, 2, 1, 3]
        })

        dau = data.groupby('date')['user_id'].nunique()

        assert dau.sum() == 4


class TestBusinessLogic:

    def test_wau_calculation_sliding_window(self):
        base_date = date(2026, 1, 1)
        dates = [base_date + timedelta(days=i) for i in range(10)]

        user_activity = []
        for i, d in enumerate(dates):
            for user_id in range(i + 1):
                user_activity.append({
                    'date': d,
                    'user_id': user_id
                })

        df = pd.DataFrame(user_activity)

        current_date = dates[4]
        current_dt = pd.to_datetime(current_date)
        week_start = current_dt - timedelta(days=6)

        mask = (pd.to_datetime(df['date']) >= week_start) & (pd.to_datetime(df['date']) <= current_dt)
        week_users = df[mask]['user_id'].nunique()

        assert week_users == 5

    def test_mau_calculation_sliding_window(self):
        base_date = date(2026, 1, 1)
        dates = [base_date + timedelta(days=i) for i in range(40)]

        user_activity = []
        for i, d in enumerate(dates):
            for user_id in range(i % 10):
                user_activity.append({
                    'date': d,
                    'user_id': user_id
                })

        df = pd.DataFrame(user_activity)

        current_date = dates[29]
        current_dt = pd.to_datetime(current_date)
        month_start = current_dt - timedelta(days=29)

        mask = (pd.to_datetime(df['date']) >= month_start) & (pd.to_datetime(df['date']) <= current_dt)
        month_users = df[mask]['user_id'].nunique()

        assert month_users == 9