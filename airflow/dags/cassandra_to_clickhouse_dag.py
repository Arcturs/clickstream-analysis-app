from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd
import numpy as np
import logging
from clickhouse_driver import Client
from cassandra.cluster import Cluster

CASSANDRA_CONFIG = {
    'hosts': ['host.docker.internal'],
    'port': 9042,
    'keyspace': 'analytics'
}

CLICKHOUSE_CONFIG = {
    'host': 'host.docker.internal',
    'port': 9002,
    'user': 'admin',
    'password': 'admin',
    'database': 'analytics'
}

logger = logging.getLogger(__name__)

def get_cassandra_session():
    try:
        cluster = Cluster(
            CASSANDRA_CONFIG['hosts'],
            port=CASSANDRA_CONFIG['port']
        )

        session = cluster.connect(CASSANDRA_CONFIG['keyspace'])
        logger.info("Successfully connected to Cassandra")
        return session, cluster
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {e}")
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

@dag(
    'clickstream_analytics_cassandra_to_clickhouse',
    description='ETL pipeline for clickstream analytics: Cassandra → ClickHouse',
    schedule='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['clickstream', 'analytics', 'cassandra', 'clickhouse', 'etl']
)
def cassandra_to_clickhouse_load():
    @task
    def extract_data_from_cassandra(**context):
        days_back = 10
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        try:
            session, cluster = get_cassandra_session()
            query = """
                    SELECT user_id, \
                           created_at, \
                           id, \
                           type, \
                           session_id, \
                           url, \
                           device_type, \
                           event_title, \
                           element_id, \
                           x, \
                           y
                    FROM analytics.clean_click_events
                    WHERE created_at >= %s \
                      AND created_at <= %s ALLOW FILTERING \
                    """

            rows = session.execute(query, (start_date, end_date))
            data = []
            for row in rows:
                data.append({
                    'user_id': row.user_id,
                    'created_at': row.created_at,
                    'event_id': row.id,
                    'event_type': row.type,
                    'session_id': row.session_id,
                    'url': row.url,
                    'device_type': row.device_type,
                    'event_title': row.event_title,
                    'element_id': row.element_id,
                    'x': row.x if row.x else None,
                    'y': row.y if row.y else None
                })

            df = pd.DataFrame(data)
            logger.info(f"Extracted {len(df)} rows from Cassandra")
            logger.info(f"Date range: {df['created_at'].min()} to {df['created_at'].max()}")
            logger.info(f"Unique users: {df['user_id'].nunique()}")
            logger.info(f"Unique sessions: {df['session_id'].nunique()}")

            if isinstance(df['created_at'].iloc[0], str):
                df['created_at'] = pd.to_datetime(df['created_at'])

            context['ti'].xcom_push(key='raw_data', value=df.to_json(date_format='iso'))

            session.shutdown()
            cluster.shutdown()

            return df.shape[0]

        except Exception as e:
            logger.error(f"Failed to extract data from Cassandra: {e}")
            raise

    @task
    def calculate_dau_wau_mau(**context):
        try:
            raw_data_json = context['ti'].xcom_pull(key='raw_data', task_ids='extract_data_from_cassandra')
            df = pd.read_json(raw_data_json, convert_dates=['created_at'])

            if df.empty:
                logger.warning("No data for DAU/WAU/MAU calculation")
                return 0

            df['date'] = df['created_at'].dt.date

            # DAU
            dau_df = df.groupby('date')['user_id'].nunique().reset_index()
            dau_df.columns = ['date', 'dau']

            # Для WAU и MAU используем скользящее окно
            dates = sorted(df['date'].unique())

            # WAU (Weekly Active Users) - скользящее окно 7 дней
            wau_data = []
            for current_date in dates:
                current_dt = pd.to_datetime(current_date)
                week_start = current_dt - timedelta(days=6)

                mask = (pd.to_datetime(df['date']) >= week_start) & (pd.to_datetime(df['date']) <= current_dt)
                week_users = df[mask]['user_id'].nunique()
                wau_data.append({'date': current_date, 'wau': week_users})

            wau_df = pd.DataFrame(wau_data)

            # MAU (Monthly Active Users) - скользящее окно 30 дней
            mau_data = []
            for current_date in dates:
                current_dt = pd.to_datetime(current_date)
                month_start = current_dt - timedelta(days=29)

                mask = (pd.to_datetime(df['date']) >= month_start) & (pd.to_datetime(df['date']) <= current_dt)
                month_users = df[mask]['user_id'].nunique()
                mau_data.append({'date': current_date, 'mau': month_users})

            mau_df = pd.DataFrame(mau_data)

            activity_df = dau_df.merge(wau_df, on='date').merge(mau_df, on='date')
            activity_df['date'] = pd.to_datetime(activity_df['date'])

            activity_df['wau_to_dau_ratio'] = activity_df['wau'] / activity_df['dau']
            activity_df['mau_to_dau_ratio'] = activity_df['mau'] / activity_df['dau']

            context['ti'].xcom_push(key='user_activity', value=activity_df.to_json(date_format='iso'))

            logger.info(f"Calculated user activity for {len(activity_df)} days")
            logger.info(
                f"DAU stats - Min: {activity_df['dau'].min()}, Max: {activity_df['dau'].max()}, Avg: {activity_df['dau'].mean():.2f}")

            return activity_df.shape[0]

        except Exception as e:
            logger.error(f"Failed to calculate user activity: {e}")
            raise

    @task
    def calculate_conversion_metrics(**context):
        try:
            raw_data_json = context['ti'].xcom_pull(key='raw_data', task_ids='extract_data_from_cassandra')
            df = pd.read_json(raw_data_json, convert_dates=['created_at'])

            if df.empty:
                logger.warning("No data for conversion calculation")
                return 0

            def get_event_type(row):
                if row['event_type'] in ['view', 'VIEW']:
                    return 'view'
                elif row['event_type'] in ['click', 'CLICK']:
                    return 'click'
                elif 'view' in str(row['event_title']).lower():
                    return 'view'
                elif 'click' in str(row['event_title']).lower():
                    return 'click'
                else:
                    return 'other'

            df['event_type_category'] = df.apply(get_event_type, axis=1)

            element_metrics = []

            element_df = df[df['element_id'].notna() & (df['element_id'] != '')]

            if not element_df.empty:
                for element_id in element_df['element_id'].unique():
                    element_data = element_df[element_df['element_id'] == element_id]
                    views = len(element_data[element_data['event_type_category'] == 'view'])
                    clicks = len(element_data[element_data['event_type_category'] == 'click'])

                    conversion_rate = (clicks / views * 100) if views > 0 else 0

                    click_data = element_data[element_data['event_type_category'] == 'click']
                    avg_x = click_data['x'].mean() if not click_data.empty and click_data['x'].notna().any() else None
                    avg_y = click_data['y'].mean() if not click_data.empty and click_data['y'].notna().any() else None

                    last_date = element_data['created_at'].max().date()

                    element_metrics.append({
                        'element_id': element_id,
                        'date': last_date,
                        'views': int(views),
                        'clicks': int(clicks),
                        'conversion_rate': float(conversion_rate),
                        'avg_x': float(avg_x) if avg_x else 0,
                        'avg_y': float(avg_y) if avg_y else 0,
                        'total_interactions': int(views + clicks)
                    })

            page_df = df.copy()
            page_metrics = []

            if not page_df.empty:
                for url in page_df['url'].unique():
                    url_data = page_df[page_df['url'] == url]
                    views = len(url_data[url_data['event_type_category'] == 'view'])
                    clicks = len(url_data[url_data['event_type_category'] == 'click'])

                    conversion_rate = (clicks / views * 100) if views > 0 else 0

                    # Статистика по дате
                    last_date = url_data['created_at'].max().date()

                    page_metrics.append({
                        'url': url,
                        'date': last_date,
                        'views': int(views),
                        'clicks': int(clicks),
                        'conversion_rate': float(conversion_rate),
                        'unique_users': int(url_data['user_id'].nunique()),
                        'unique_sessions': int(url_data['session_id'].nunique())
                    })

            element_metrics_df = pd.DataFrame(element_metrics) if element_metrics else pd.DataFrame()
            page_metrics_df = pd.DataFrame(page_metrics) if page_metrics else pd.DataFrame()

            context['ti'].xcom_push(key='element_metrics', value=element_metrics_df.to_json(
                date_format='iso') if not element_metrics_df.empty else '')
            context['ti'].xcom_push(key='page_metrics',
                                    value=page_metrics_df.to_json(
                                        date_format='iso') if not page_metrics_df.empty else '')

            logger.info(f"Calculated conversion for {len(element_metrics)} elements and {len(page_metrics)} pages")

            return len(element_metrics) + len(page_metrics)

        except Exception as e:
            logger.error(f"Failed to calculate conversion metrics: {e}")
            raise

    @task
    def calculate_session_duration(**context):
        try:
            raw_data_json = context['ti'].xcom_pull(key='raw_data', task_ids='extract_data_from_cassandra')
            df = pd.read_json(raw_data_json, convert_dates=['created_at'])

            if df.empty:
                logger.warning("No data for session duration calculation")
                context['ti'].xcom_push(key='session_metrics', value=pd.DataFrame().to_json(date_format='iso'))
                return 0

            df = df.sort_values(['session_id', 'created_at'])

            session_durations = []

            for session_id in df['session_id'].unique():
                session_data = df[df['session_id'] == session_id]

                if len(session_data) == 1:
                    session_start = session_data['created_at'].iloc[0]
                    session_end = session_start
                    duration = 0
                else:
                    session_start = session_data['created_at'].min()
                    session_end = session_data['created_at'].max()
                    duration = (session_end - session_start).total_seconds()

                if duration < 0:
                    logger.warning(f"Negative duration for session {session_id}: {duration}")
                    continue

                if duration > 86400:
                    logger.warning(f"Session {session_id} too long: {duration} seconds")
                    duration = 86400

                session_durations.append({
                    'session_id': session_id,
                    'user_id': session_data['user_id'].iloc[0] if not session_data.empty else None,
                    'duration_seconds': duration,
                    'event_count': len(session_data),
                    'start_time': session_start,
                    'end_time': session_end,
                    'date': session_start.date()
                })

            if session_durations:
                sessions_df = pd.DataFrame(session_durations)

                sessions_df['date'] = pd.to_datetime(sessions_df['date'])

                daily_metrics = sessions_df.groupby('date').agg({
                    'duration_seconds': ['mean', 'median', lambda x: x.std() if len(x) > 1 else 0],
                    'session_id': 'count',
                    'user_id': 'nunique'
                }).reset_index()

                daily_metrics.columns = ['date', 'avg_duration', 'median_duration',
                                         'std_duration', 'session_count', 'unique_users']

                daily_metrics['total_events'] = sessions_df.groupby('date')['event_count'].sum().values
                daily_metrics['avg_duration_minutes'] = daily_metrics['avg_duration'] / 60
                daily_metrics['median_duration_minutes'] = daily_metrics['median_duration'] / 60

                daily_metrics = daily_metrics.fillna(0)

                daily_metrics['avg_duration'] = daily_metrics['avg_duration'].astype(float)
                daily_metrics['median_duration'] = daily_metrics['median_duration'].astype(float)
                daily_metrics['std_duration'] = daily_metrics['std_duration'].astype(float)
                daily_metrics['session_count'] = daily_metrics['session_count'].astype(int)
                daily_metrics['unique_users'] = daily_metrics['unique_users'].astype(int)
                daily_metrics['total_events'] = daily_metrics['total_events'].astype(int)

                logger.info(f"Calculated session metrics for {len(daily_metrics)} days")
                logger.info(f"Session count per day: {daily_metrics['session_count'].sum()}")
                logger.info(f"Average duration range: {daily_metrics['avg_duration'].min():.2f} - "
                            f"{daily_metrics['avg_duration'].max():.2f} seconds")

            else:
                daily_metrics = pd.DataFrame(columns=['date', 'avg_duration', 'median_duration',
                                                      'std_duration', 'session_count', 'unique_users',
                                                      'total_events', 'avg_duration_minutes',
                                                      'median_duration_minutes'])
                logger.warning("No session durations calculated")

            context['ti'].xcom_push(key='session_metrics', value=daily_metrics.to_json(date_format='iso'))

            return len(daily_metrics)

        except Exception as e:
            logger.error(f"Failed to calculate session duration: {e}")
            context['ti'].xcom_push(key='session_metrics', value=pd.DataFrame().to_json(date_format='iso'))
            raise

    @task
    def calculate_device_distribution(**context):
        try:
            raw_data_json = context['ti'].xcom_pull(key='raw_data', task_ids='extract_data_from_cassandra')
            df = pd.read_json(raw_data_json, convert_dates=['created_at'])

            if df.empty:
                logger.warning("No data for device distribution calculation")
                return 0

            df['date'] = df['created_at'].dt.date

            device_distribution = df.groupby(['date', 'device_type']).agg({
                'user_id': 'nunique',
                'session_id': 'nunique',
                'event_id': 'count'
            }).reset_index()

            device_distribution.columns = ['date', 'device_type', 'unique_users', 'unique_sessions', 'device_events']

            daily_totals = df.groupby('date').agg({
                'user_id': 'nunique',
                'session_id': 'nunique',
                'event_id': 'count'
            }).reset_index()

            daily_totals.columns = ['date', 'total_users', 'total_sessions', 'total_events']

            device_distribution = device_distribution.merge(daily_totals, on='date', how='left')
            device_distribution['user_percentage'] = (device_distribution['unique_users'] /
                                                      device_distribution['total_users'] * 100)
            device_distribution['session_percentage'] = (device_distribution['unique_sessions'] /
                                                         device_distribution['total_sessions'] * 100)
            device_distribution['event_percentage'] = (device_distribution['device_events'] /
                                                       device_distribution['total_events'] * 100)

            device_distribution['date'] = pd.to_datetime(device_distribution['date'])

            context['ti'].xcom_push(key='device_distribution', value=device_distribution.to_json(date_format='iso'))

            logger.info(f"Calculated device distribution for {len(device_distribution)} records")

            return len(device_distribution)

        except Exception as e:
            logger.error(f"Failed to calculate device distribution: {e}")
            raise

    @task
    def calculate_top_pages(**context):
        try:
            raw_data_json = context['ti'].xcom_pull(key='raw_data', task_ids='extract_data_from_cassandra')
            df = pd.read_json(raw_data_json, convert_dates=['created_at'])

            if df.empty:
                logger.warning("No data for top pages calculation")
                return 0

            def get_event_type(row):
                if row['event_type'] in ['view', 'VIEW']:
                    return 'view'
                elif row['event_type'] in ['click', 'CLICK']:
                    return 'click'
                elif 'view' in str(row['event_title']).lower():
                    return 'view'
                elif 'click' in str(row['event_title']).lower():
                    return 'click'
                else:
                    return 'other'

            df['event_type_category'] = df.apply(get_event_type, axis=1)
            df['date'] = df['created_at'].dt.date

            view_data = df[df['event_type_category'] == 'view']

            if not view_data.empty:
                top_pages_views = view_data.groupby(['date', 'url']).agg({
                    'user_id': 'nunique',
                    'session_id': 'nunique',
                    'event_id': 'count'
                }).reset_index()

                top_pages_views.columns = ['date', 'url', 'unique_users', 'unique_sessions', 'view_count']
            else:
                top_pages_views = pd.DataFrame(columns=['date', 'url', 'unique_users', 'unique_sessions', 'view_count'])

            click_data = df[df['event_type_category'] == 'click']

            if not click_data.empty:
                top_pages_clicks = click_data.groupby(['date', 'url']).agg({
                    'event_id': 'count'
                }).reset_index()

                top_pages_clicks.columns = ['date', 'url', 'click_count']
            else:
                top_pages_clicks = pd.DataFrame(columns=['date', 'url', 'click_count'])

            if not top_pages_views.empty:
                top_pages = top_pages_views.merge(top_pages_clicks, on=['date', 'url'], how='left')
                top_pages['click_count'] = top_pages['click_count'].fillna(0)

                top_pages['conversion_rate'] = np.where(
                    top_pages['view_count'] > 0,
                    (top_pages['click_count'] / top_pages['view_count']) * 100,
                    0
                )

                top_pages['rank'] = top_pages.groupby('date')['view_count'].rank(
                    method='dense', ascending=False
                )

                top_pages['date'] = pd.to_datetime(top_pages['date'])
            else:
                top_pages = pd.DataFrame(columns=['date', 'url', 'unique_users', 'unique_sessions',
                                                  'view_count', 'click_count', 'conversion_rate', 'rank'])

            context['ti'].xcom_push(key='top_pages', value=top_pages.to_json(date_format='iso'))

            logger.info(f"Calculated top pages for {len(top_pages)} page-date combinations")

            return len(top_pages)

        except Exception as e:
            logger.error(f"Failed to calculate top pages: {e}")
            raise

    @task
    def save_to_clickhouse(**context):
        try:
            client = get_clickhouse_client()

            # 1. Сохраняем активность пользователей (DAU/WAU/MAU)
            try:
                activity_json = context['ti'].xcom_pull(key='user_activity', task_ids='calculate_dau_wau_mau')
                if activity_json and activity_json.strip():
                    activity_df = pd.read_json(activity_json, convert_dates=['date'])
                    if not activity_df.empty:
                        for _, row in activity_df.iterrows():
                            insert_query = """
                                           INSERT INTO user_activity_daily (date, dau, wau, mau, wau_to_dau_ratio, mau_to_dau_ratio)
                                           VALUES (%(date)s, %(dau)s, %(wau)s, %(mau)s, %(wau_to_dau_ratio)s, \
                                                   %(mau_to_dau_ratio)s) \
                                           """
                            client.execute(insert_query, {
                                'date': row['date'].date(),
                                'dau': int(row['dau']),
                                'wau': int(row['wau']),
                                'mau': int(row['mau']),
                                'wau_to_dau_ratio': float(row['wau_to_dau_ratio']) if 'wau_to_dau_ratio' in row else 0,
                                'mau_to_dau_ratio': float(row['mau_to_dau_ratio']) if 'mau_to_dau_ratio' in row else 0
                            })

                        logger.info(f"Saved {len(activity_df)} records to user_activity_daily")
            except Exception as e:
                logger.warning(f"Failed to save user activity to ClickHouse: {e}")

            # 2. Сохраняем конверсию по элементам
            try:
                element_json = context['ti'].xcom_pull(key='element_metrics', task_ids='calculate_conversion_metrics')
                if element_json and element_json.strip():
                    element_df = pd.read_json(element_json, convert_dates=['date'])

                    if not element_df.empty:
                        for _, row in element_df.iterrows():
                            insert_query = """
                                           INSERT INTO element_conversion (date, element_id, views, clicks,
                                                                           conversion_rate, avg_x, avg_y, \
                                                                           total_interactions)
                                           VALUES (%(date)s, %(element_id)s, %(views)s, %(clicks)s,
                                                   %(conversion_rate)s, %(avg_x)s, %(avg_y)s, %(total_interactions)s) \
                                           """
                            client.execute(insert_query, {
                                'date': row['date'],
                                'element_id': str(row['element_id']),
                                'views': int(row['views']),
                                'clicks': int(row['clicks']),
                                'conversion_rate': float(row['conversion_rate']),
                                'avg_x': float(row['avg_x']),
                                'avg_y': float(row['avg_y']),
                                'total_interactions': int(row['total_interactions'])
                            })

                        logger.info(f"Saved {len(element_df)} records to element_conversion")
            except Exception as e:
                logger.warning(f"Failed to save element conversion to ClickHouse: {e}")

            # 3. Сохраняем метрики сессий
            try:
                session_json = context['ti'].xcom_pull(key='session_metrics', task_ids='calculate_session_duration')
                if session_json and session_json.strip():
                    session_df = pd.read_json(session_json, convert_dates=['date'])

                    if not session_df.empty:
                        for _, row in session_df.iterrows():
                            insert_query = """
                                           INSERT INTO session_metrics_daily (date, avg_duration_seconds, \
                                                                              median_duration_seconds,
                                                                              std_duration_seconds, total_events, \
                                                                              session_count,
                                                                              unique_users, avg_duration_minutes, \
                                                                              median_duration_minutes)
                                           VALUES (%(date)s, %(avg_duration_seconds)s, %(median_duration_seconds)s,
                                                   %(std_duration_seconds)s, %(total_events)s, %(session_count)s,
                                                   %(unique_users)s, %(avg_duration_minutes)s, \
                                                   %(median_duration_minutes)s) \
                                           """
                            client.execute(insert_query, {
                                'date': row['date'].date(),
                                'avg_duration_seconds': float(row['avg_duration']),
                                'median_duration_seconds': float(row['median_duration']),
                                'std_duration_seconds': float(row['std_duration']) if pd.notna(
                                    row['std_duration']) else 0,
                                'total_events': int(row['total_events']),
                                'session_count': int(row['session_count']),
                                'unique_users': int(row['unique_users']),
                                'avg_duration_minutes': float(row['avg_duration_minutes']),
                                'median_duration_minutes': float(row['median_duration_minutes'])
                            })

                        logger.info(f"Saved {len(session_df)} records to session_metrics_daily")
            except Exception as e:
                logger.warning(f"Failed to save session metrics to ClickHouse: {e}")

            # 4. Сохраняем распределение устройств
            try:
                device_json = context['ti'].xcom_pull(key='device_distribution',
                                                      task_ids='calculate_device_distribution')
                if device_json and device_json.strip():
                    device_df = pd.read_json(device_json, convert_dates=['date'])

                    if not device_df.empty:
                        for _, row in device_df.iterrows():
                            insert_query = """
                                           INSERT INTO device_distribution_daily (date, device_type, unique_users, \
                                                                                  unique_sessions,
                                                                                  device_events, user_percentage, \
                                                                                  session_percentage, event_percentage)
                                           VALUES (%(date)s, %(device_type)s, %(unique_users)s, %(unique_sessions)s,
                                                   %(device_events)s, %(user_percentage)s, %(session_percentage)s, \
                                                   %(event_percentage)s) \
                                           """
                            client.execute(insert_query, {
                                'date': row['date'].date(),
                                'device_type': str(row['device_type']),
                                'unique_users': int(row['unique_users']),
                                'unique_sessions': int(row['unique_sessions']),
                                'device_events': int(row['device_events']),
                                'user_percentage': float(row['user_percentage']),
                                'session_percentage': float(row['session_percentage']),
                                'event_percentage': float(row['event_percentage'])
                            })

                        logger.info(f"Saved {len(device_df)} records to device_distribution_daily")
            except Exception as e:
                logger.warning(f"Failed to save device distribution to ClickHouse: {e}")

            # 5. Сохраняем топ страниц
            try:
                pages_json = context['ti'].xcom_pull(key='top_pages', task_ids='calculate_top_pages')
                if pages_json and pages_json.strip():
                    pages_df = pd.read_json(pages_json, convert_dates=['date'])

                    if not pages_df.empty:
                        top_10_df = pages_df[pages_df['rank'] <= 10]

                        for _, row in top_10_df.iterrows():
                            insert_query = """
                                           INSERT INTO top_pages_daily (date, url, unique_users, unique_sessions,
                                                                        view_count, click_count, conversion_rate, rank)
                                           VALUES (%(date)s, %(url)s, %(unique_users)s, %(unique_sessions)s,
                                                   %(view_count)s, %(click_count)s, %(conversion_rate)s, %(rank)s) \
                                           """
                            client.execute(insert_query, {
                                'date': row['date'].date(),
                                'url': str(row['url']),
                                'unique_users': int(row['unique_users']),
                                'unique_sessions': int(row['unique_sessions']),
                                'view_count': int(row['view_count']),
                                'click_count': int(row['click_count']),
                                'conversion_rate': float(row['conversion_rate']),
                                'rank': int(row['rank'])
                            })

                        logger.info(f"Saved {len(top_10_df)} records to top_pages_daily")
            except Exception as e:
                logger.warning(f"Failed to save top pages to ClickHouse: {e}")

            logger.info("Successfully saved all metrics to ClickHouse")

            return "Success"

        except Exception as e:
            logger.error(f"Failed to save results to ClickHouse: {e}")
            raise

    extract_data_task = extract_data_from_cassandra()
    calculate_dau_task = calculate_dau_wau_mau()
    calculate_conversion_task = calculate_conversion_metrics()
    calculate_session_task = calculate_session_duration()
    calculate_device_task = calculate_device_distribution()
    calculate_top_pages_task = calculate_top_pages()
    save_results_task = save_to_clickhouse()

    extract_data_task >> [
        calculate_dau_task,
        calculate_conversion_task,
        calculate_session_task,
        calculate_device_task,
        calculate_top_pages_task
    ] >> save_results_task

dag = cassandra_to_clickhouse_load()