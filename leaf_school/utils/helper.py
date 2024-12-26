from pytz import timezone as pytz_timezone
from django.utils import timezone
from datetime import datetime


def parse_clickhouse_timestamp(timestamp):
    """Ensure ClickHouse timestamp is timezone-aware in UTC."""
    if isinstance(timestamp, str):
        # Parse string timestamp into a datetime object
        timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
    # If it's naive, assume it's in UTC and make it timezone-aware
    if timestamp.tzinfo is None:
        timestamp = timezone.make_aware(timestamp, pytz_timezone('UTC'))
    return timestamp