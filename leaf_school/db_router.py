# db_router.py
# This is very important file for database routing. This file is used to route the database to the respective app. We should avoid accidental writes to the wrong database.

from datetime import datetime
from typing import Optional, Any
import logging

logger = logging.getLogger(__name__)

class DatabaseRouter:
    """
    Database router for multi-database architecture with year-based ClickHouse routing.

    Handles routing for:
    - PostgreSQL (default): Primary application data
    - MySQL (moodle_db, bookroll_db): External LMS data (read-only)
    - ClickHouse (year-based): Analytics data
      - clickhouse_db: Data from 2025 onwards (current/future data pipeline)
      - clickhouse_db_pre_2025: Historical data before 2025 (legacy data pipeline)
    """

    def db_for_read(self, model, **hints):
        """Route read operations based on the app_label or table."""
        app_label = model._meta.app_label

        # Route external database apps
        if app_label == 'moodle_app':
            return 'moodle_db'
        if app_label == 'bookroll_app':
            return 'bookroll_db'

        # Route ClickHouse apps based on year-based logic
        if app_label in ['clickhouse_app', 'analysis_app']:
            return self._get_clickhouse_db_for_model(model, **hints)

        # Default to PostgreSQL for all other apps
        return 'default'

    def db_for_write(self, model, **hints):
        """Route write operations - disallow writes to external databases."""
        app_label = model._meta.app_label

        # Disallow writes to external read-only databases
        if app_label in ['moodle_app', 'bookroll_app', 'clickhouse_app', 'analysis_app']:
            logger.warning(f"Write operation attempted on read-only database for app: {app_label}")
            return None

        # Allow writes only to default PostgreSQL database
        return 'default'

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """Prevent migrations for non-default databases."""
        # Only allow migrations on the default PostgreSQL database
        if db in ['moodle_db', 'bookroll_db', 'clickhouse_db', 'clickhouse_db_pre_2025', 'analysis_db_pre_2025']:
            return False
        return db == 'default'

    def allow_relation(self, obj1, obj2, **hints):
        """Allow relations between objects from the same database."""
        db_set = {'default', 'moodle_db', 'bookroll_db', 'clickhouse_db', 'clickhouse_db_pre_2025', 'analysis_db_pre_2025'}
        if obj1._state.db in db_set and obj2._state.db in db_set:
            return True
        return None

    def _get_clickhouse_db_for_model(self, model, **hints) -> str:
        """
        Determine which ClickHouse database to use based on year context.

        Args:
            model: The Django model being queried
            **hints: Additional routing hints (may contain year information)

        Returns:
            str: Database alias to use ('clickhouse_db' or 'clickhouse_db_pre_2025')
        """
        # Check if hints contain year information
        year_hint = hints.get('year')
        if year_hint:
            try:
                year = int(year_hint)
                return 'clickhouse_db' if year >= 2025 else 'clickhouse_db_pre_2025'
            except (ValueError, TypeError):
                logger.warning(f"Invalid year hint provided: {year_hint}")

        # Check if hints contain a date range
        date_from = hints.get('date_from')
        date_to = hints.get('date_to')

        if date_from or date_to:
            # If we have date information, determine the appropriate database
            target_year = None

            if date_from:
                try:
                    if isinstance(date_from, str):
                        target_year = datetime.fromisoformat(date_from.replace('Z', '+00:00')).year
                    elif hasattr(date_from, 'year'):
                        target_year = date_from.year
                except (ValueError, AttributeError):
                    logger.warning(f"Invalid date_from hint: {date_from}")

            if target_year:
                return 'clickhouse_db' if target_year >= 2025 else 'clickhouse_db_pre_2025'

        # Default behavior: use current year to determine database
        current_year = datetime.now().year
        default_db = 'clickhouse_db' if current_year >= 2025 else 'clickhouse_db_pre_2025'

        logger.debug(f"Using default ClickHouse database: {default_db} (current year: {current_year})")
        return default_db

    @staticmethod
    def get_database_for_year(year: int) -> str:
        """
        Public method to get the appropriate ClickHouse database for a given year.

        Args:
            year (int): The year to determine database for

        Returns:
            str: Database alias ('clickhouse_db' or 'clickhouse_db_pre_2025')
        """
        return 'clickhouse_db' if year >= 2025 else 'clickhouse_db_pre_2025'

    @staticmethod
    def get_database_for_date_range(date_from=None, date_to=None) -> str:
        """
        Public method to get the appropriate ClickHouse database for a date range.

        Args:
            date_from: Start date (datetime, date, or ISO string)
            date_to: End date (datetime, date, or ISO string)

        Returns:
            str: Database alias ('clickhouse_db' or 'clickhouse_db_pre_2025')

        Note:
            If the date range spans across 2025, returns 'clickhouse_db'
            to prioritize current data.
        """
        years = []

        for date_val in [date_from, date_to]:
            if date_val:
                try:
                    if isinstance(date_val, str):
                        year = datetime.fromisoformat(date_val.replace('Z', '+00:00')).year
                    elif hasattr(date_val, 'year'):
                        year = date_val.year
                    else:
                        continue
                    years.append(year)
                except (ValueError, AttributeError):
                    logger.warning(f"Invalid date value: {date_val}")
                    continue

        if not years:
            # No valid dates provided, use current year
            current_year = datetime.now().year
            return 'clickhouse_db' if current_year >= 2025 else 'clickhouse_db_pre_2025'

        # If any year is 2025 or later, use the 2025+ database
        if any(year >= 2025 for year in years):
            return 'clickhouse_db'
        else:
            return 'clickhouse_db_pre_2025'


