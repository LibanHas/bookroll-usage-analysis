#!/usr/bin/env python
"""
Test script for database routing functionality.
Run this script to verify that the year-based database routing works correctly.

Usage:
    python test_database_routing.py
"""

import os
import sys
import django
from datetime import datetime, date

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leaf_school.settings')
django.setup()

from leaf_school.db_router import DatabaseRouter
from leaf_school.utils.db_helpers import (
    get_clickhouse_db_for_year,
    get_clickhouse_db_for_date_range,
)

def test_year_routing():
    """Test year-based database routing."""
    print("=== Testing Year-Based Routing ===")

    test_cases = [
        (2020, 'analysis_db_pre_2025'),
        (2021, 'analysis_db_pre_2025'),
        (2022, 'analysis_db_pre_2025'),
        (2023, 'analysis_db_pre_2025'),
        (2024, 'analysis_db_pre_2025'),
        (2025, 'clickhouse_db_2025'),
        (2026, 'clickhouse_db_2025'),
        (2030, 'clickhouse_db_2025'),
    ]

    for year, expected_db in test_cases:
        # Test DatabaseRouter static method
        result_router = DatabaseRouter.get_database_for_year(year)

        # Test helper function
        result_helper = get_clickhouse_db_for_year(year)

        # Verify both methods return the same result
        assert result_router == result_helper == expected_db, \
            f"Year {year}: Expected {expected_db}, got router={result_router}, helper={result_helper}"

        print(f"✓ Year {year} → {result_router}")

    print("Year-based routing tests passed!\n")

def test_date_range_routing():
    """Test date range-based database routing."""
    print("=== Testing Date Range-Based Routing ===")

    test_cases = [
        # Single year ranges
        (date(2024, 1, 1), date(2024, 12, 31), 'analysis_db_pre_2025'),
        (date(2025, 1, 1), date(2025, 12, 31), 'clickhouse_db_2025'),

        # Cross-year ranges (should prefer 2025+ database)
        (date(2024, 6, 1), date(2025, 6, 1), 'clickhouse_db_2025'),
        (date(2023, 1, 1), date(2025, 1, 1), 'clickhouse_db_2025'),

        # Pre-2025 only
        (date(2022, 1, 1), date(2024, 12, 31), 'analysis_db_pre_2025'),

        # Post-2025 only
        (date(2025, 6, 1), date(2026, 6, 1), 'clickhouse_db_2025'),

        # Single date (from only)
        (date(2024, 6, 1), None, 'analysis_db_pre_2025'),
        (date(2025, 6, 1), None, 'clickhouse_db_2025'),

        # Single date (to only)
        (None, date(2024, 6, 1), 'analysis_db_pre_2025'),
        (None, date(2025, 6, 1), 'clickhouse_db_2025'),
    ]

    for date_from, date_to, expected_db in test_cases:
        # Test DatabaseRouter static method
        result_router = DatabaseRouter.get_database_for_date_range(date_from, date_to)

        # Test helper function
        result_helper = get_clickhouse_db_for_date_range(date_from, date_to)

        # Verify both methods return the same result
        assert result_router == result_helper == expected_db, \
            f"Date range {date_from} to {date_to}: Expected {expected_db}, got router={result_router}, helper={result_helper}"

        print(f"✓ {date_from} to {date_to} → {result_router}")

    print("Date range-based routing tests passed!\n")

def test_datetime_routing():
    """Test datetime-based database routing."""
    print("=== Testing DateTime-Based Routing ===")

    test_cases = [
        (datetime(2024, 6, 15, 10, 30), datetime(2024, 8, 15, 14, 45), 'analysis_db_pre_2025'),
        (datetime(2025, 3, 10, 9, 0), datetime(2025, 9, 20, 17, 30), 'clickhouse_db_2025'),
        (datetime(2024, 11, 1, 0, 0), datetime(2025, 2, 28, 23, 59), 'clickhouse_db_2025'),
    ]

    for date_from, date_to, expected_db in test_cases:
        result = DatabaseRouter.get_database_for_date_range(date_from, date_to)

        assert result == expected_db, \
            f"DateTime range {date_from} to {date_to}: Expected {expected_db}, got {result}"

        print(f"✓ {date_from} to {date_to} → {result}")

    print("DateTime-based routing tests passed!\n")

def test_string_date_routing():
    """Test ISO string date-based database routing."""
    print("=== Testing ISO String Date-Based Routing ===")

    test_cases = [
        ('2024-06-15', '2024-08-15', 'analysis_db_pre_2025'),
        ('2025-03-10', '2025-09-20', 'clickhouse_db_2025'),
        ('2024-11-01T00:00:00Z', '2025-02-28T23:59:59Z', 'clickhouse_db_2025'),
        ('2024-01-01T00:00:00+00:00', '2024-12-31T23:59:59+00:00', 'analysis_db_pre_2025'),
    ]

    for date_from, date_to, expected_db in test_cases:
        result = DatabaseRouter.get_database_for_date_range(date_from, date_to)

        assert result == expected_db, \
            f"String date range {date_from} to {date_to}: Expected {expected_db}, got {result}"

        print(f"✓ {date_from} to {date_to} → {result}")

    print("ISO string date-based routing tests passed!\n")

def test_edge_cases():
    """Test edge cases and error handling."""
    print("=== Testing Edge Cases ===")

    # Test with no dates (should use current year)
    current_year = datetime.now().year
    expected_current = 'clickhouse_db_2025' if current_year >= 2025 else 'analysis_db_pre_2025'

    result = DatabaseRouter.get_database_for_date_range(None, None)
    assert result == expected_current, \
        f"No dates provided: Expected {expected_current}, got {result}"
    print(f"✓ No dates (current year {current_year}) → {result}")

    # Test with invalid date strings (should handle gracefully)
    try:
        result = DatabaseRouter.get_database_for_date_range('invalid-date', None)
        print(f"✓ Invalid date string handled gracefully → {result}")
    except Exception as e:
        print(f"✗ Invalid date string caused error: {e}")

    print("Edge case tests completed!\n")

def main():
    """Run all database routing tests."""
    print("Database Routing Test Suite")
    print("=" * 50)

    try:
        test_year_routing()
        test_date_range_routing()
        test_datetime_routing()
        test_string_date_routing()
        test_edge_cases()

        print("🎉 All tests passed! Database routing is working correctly.")

    except AssertionError as e:
        print(f"❌ Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()