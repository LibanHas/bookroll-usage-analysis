from contextlib import contextmanager
from django.db import connections
from datetime import datetime, date
from typing import Union, Optional
import logging

logger = logging.getLogger(__name__)

@contextmanager
def clickhouse_connection():
    """
    Context manager for handling ClickHouse database connections safely.
    Uses the main 'clickhouse_db' alias for 2025+ data.
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

@contextmanager
def clickhouse_connection_for_year(year: int):
    """
    Context manager for handling year-specific ClickHouse database connections.

    Args:
        year (int): The year to determine which database to connect to

    Yields:
        Django database connection for the appropriate ClickHouse database
    """
    from leaf_school.db_router import DatabaseRouter

    db_alias = DatabaseRouter.get_database_for_year(year)
    connection = connections[db_alias]

    logger.info(f"Initializing ClickHouse connection for year {year} using database: {db_alias}")
    try:
        yield connection
        logger.info(f"Connection to {db_alias} successful")
    except Exception as e:
        logger.error(f"ClickHouse connection error for {db_alias}: {str(e)}")
        raise
    finally:
        connection.close()

@contextmanager
def clickhouse_connection_for_date_range(date_from: Optional[Union[datetime, date, str]] = None,
                                       date_to: Optional[Union[datetime, date, str]] = None):
    """
    Context manager for handling date-range-specific ClickHouse database connections.

    Args:
        date_from: Start date (datetime, date, or ISO string)
        date_to: End date (datetime, date, or ISO string)

    Yields:
        Django database connection for the appropriate ClickHouse database
    """
    from leaf_school.db_router import DatabaseRouter

    db_alias = DatabaseRouter.get_database_for_date_range(date_from, date_to)
    connection = connections[db_alias]

    logger.info(f"Initializing ClickHouse connection for date range {date_from} to {date_to} using database: {db_alias}")
    try:
        yield connection
        logger.info(f"Connection to {db_alias} successful")
    except Exception as e:
        logger.error(f"ClickHouse connection error for {db_alias}: {str(e)}")
        raise
    finally:
        connection.close()

@contextmanager
def clickhouse_db_pre_2025_connection():
    """
    Context manager for handling pre-2025 ClickHouse database connections.
    """
    connection = connections['clickhouse_db_pre_2025']
    logger.info("Initializing ClickHouse database (pre-2025) connection.....")
    try:
        yield connection
        logger.info("Connection to ClickHouse database (pre-2025) successful")
    except Exception as e:
        logger.error(f"ClickHouse database (pre-2025) connection error: {str(e)}")
        raise
    finally:
        connection.close()

@contextmanager
def analysis_db_pre_2025_connection():
    """
    Context manager for handling pre-2025 analysis database connections.
    """
    connection = connections['analysis_db_pre_2025']
    logger.info("Initializing analysis database (pre-2025) connection.....")
    try:
        yield connection
        logger.info("Connection to analysis database (pre-2025) successful")
    except Exception as e:
        logger.error(f"Analysis database (pre-2025) connection error: {str(e)}")
        raise
    finally:
        connection.close()

@contextmanager
def clickhouse_db_2025_connection():
    """
    Context manager for handling 2025+ ClickHouse database connections.
    This is an alias for the main clickhouse_db.
    """
    connection = connections['clickhouse_db']
    logger.info("Initializing ClickHouse database (2025+) connection.....")
    try:
        yield connection
        logger.info("Connection to ClickHouse database (2025+) successful")
    except Exception as e:
        logger.error(f"ClickHouse database (2025+) connection error: {str(e)}")
        raise
    finally:
        connection.close()

def get_clickhouse_db_for_year(year: int) -> str:
    """
    Get the appropriate ClickHouse database alias for a given year.

    Args:
        year (int): The year to determine database for

    Returns:
        str: Database alias ('clickhouse_db' or 'clickhouse_db_pre_2025')
    """
    from leaf_school.db_router import DatabaseRouter
    return DatabaseRouter.get_database_for_year(year)

def get_clickhouse_db_for_date_range(date_from: Optional[Union[datetime, date, str]] = None,
                                    date_to: Optional[Union[datetime, date, str]] = None) -> str:
    """
    Get the appropriate ClickHouse database alias for a date range.

    Args:
        date_from: Start date (datetime, date, or ISO string)
        date_to: End date (datetime, date, or ISO string)

    Returns:
        str: Database alias ('clickhouse_db' or 'clickhouse_db_pre_2025')
    """
    from leaf_school.db_router import DatabaseRouter
    return DatabaseRouter.get_database_for_date_range(date_from, date_to)

def execute_clickhouse_query_with_year_routing(query: str, params: list = None, year: int = None,
                                             date_from: Optional[Union[datetime, date, str]] = None,
                                             date_to: Optional[Union[datetime, date, str]] = None):
    """
    Execute a ClickHouse query with automatic year-based database routing.

    Args:
        query (str): SQL query to execute
        params (list, optional): Query parameters
        year (int, optional): Specific year to route to
        date_from: Start date for date range routing
        date_to: End date for date range routing

    Returns:
        Query results

    Raises:
        ValueError: If neither year nor date range is provided
    """
    if year is not None:
        with clickhouse_connection_for_year(year) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params or [])
                return cursor.fetchall()
    elif date_from is not None or date_to is not None:
        with clickhouse_connection_for_date_range(date_from, date_to) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params or [])
                return cursor.fetchall()
    else:
        raise ValueError("Either 'year' or date range ('date_from'/'date_to') must be provided")

# Backward compatibility aliases
def get_current_year_clickhouse_db() -> str:
    """Get the ClickHouse database for the current year."""
    current_year = datetime.now().year
    return get_clickhouse_db_for_year(current_year)