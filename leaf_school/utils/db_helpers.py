from contextlib import contextmanager
from django.db import connections
import logging

logger = logging.getLogger(__name__)

@contextmanager
def clickhouse_connection():
    """
    Context manager for handling ClickHouse database connections safely
    """
    connection = connections['clickhouse_db']
    print("Initializing clickhouse connection.....")
    try:
        print("Connecting to ClickHouse.....")
        yield connection
        print("Connection to ClickHouse successful")
    except Exception as e:
        logger.error(f"ClickHouse connection error: {str(e)}")
        raise
    finally:
        connection.close()