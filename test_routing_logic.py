#!/usr/bin/env python
"""
Simplified test script for database routing logic.
Tests the routing logic without requiring Django setup.

Usage:
    python test_routing_logic.py
"""

from datetime import datetime, date

def get_database_for_year(year: int) -> str:
    """
    Get the appropriate ClickHouse database for a given year.

    Args:
        year (int): The year to determine database for

    Returns:
        str: Database alias ('clickhouse_db' or 'clickhouse_db_pre_2025')
    """
    return 'clickhouse_db' if year >= 2025 else 'clickhouse_db_pre_2025'

def get_database_for_date_range(date_from=None, date_to=None) -> str:
    """
    Get the appropriate ClickHouse database for a date range.

    Args:
        date_from: Start date (datetime, date, or ISO string)
        date_to: End date (datetime, date, or ISO string)

    Returns:
        str: Database alias ('clickhouse_db' or 'clickhouse_db_pre_2025')
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
                print(f"Warning: Invalid date value: {date_val}")
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

def test_year_routing():
    """Test year-based database routing."""
    print("=== Testing Year-Based Routing ===")

    test_cases = [
        (2020, 'clickhouse_db_pre_2025'),
        (2021, 'clickhouse_db_pre_2025'),
        (2022, 'clickhouse_db_pre_2025'),
        (2023, 'clickhouse_db_pre_2025'),
        (2024, 'clickhouse_db_pre_2025'),
        (2025, 'clickhouse_db'),
        (2026, 'clickhouse_db'),
        (2030, 'clickhouse_db'),
    ]

    for year, expected_db in test_cases:
        result = get_database_for_year(year)

        assert result == expected_db, \
            f"Year {year}: Expected {expected_db}, got {result}"

        print(f"✓ Year {year} → {result}")

    print("Year-based routing tests passed!\n")

def test_date_range_routing():
    """Test date range-based database routing."""
    print("=== Testing Date Range-Based Routing ===")

    test_cases = [
        # Single year ranges
        (date(2024, 1, 1), date(2024, 12, 31), 'clickhouse_db_pre_2025'),
        (date(2025, 1, 1), date(2025, 12, 31), 'clickhouse_db'),

        # Cross-year ranges (should prefer 2025+ database)
        (date(2024, 6, 1), date(2025, 6, 1), 'clickhouse_db'),
        (date(2023, 1, 1), date(2025, 1, 1), 'clickhouse_db'),

        # Pre-2025 only
        (date(2022, 1, 1), date(2024, 12, 31), 'clickhouse_db_pre_2025'),

        # Post-2025 only
        (date(2025, 6, 1), date(2026, 6, 1), 'clickhouse_db'),

        # Single date (from only)
        (date(2024, 6, 1), None, 'clickhouse_db_pre_2025'),
        (date(2025, 6, 1), None, 'clickhouse_db'),

        # Single date (to only)
        (None, date(2024, 6, 1), 'clickhouse_db_pre_2025'),
        (None, date(2025, 6, 1), 'clickhouse_db'),
    ]

    for date_from, date_to, expected_db in test_cases:
        result = get_database_for_date_range(date_from, date_to)

        assert result == expected_db, \
            f"Date range {date_from} to {date_to}: Expected {expected_db}, got {result}"

        print(f"✓ {date_from} to {date_to} → {result}")

    print("Date range-based routing tests passed!\n")

def test_datetime_routing():
    """Test datetime-based database routing."""
    print("=== Testing DateTime-Based Routing ===")

    test_cases = [
        (datetime(2024, 6, 15, 10, 30), datetime(2024, 8, 15, 14, 45), 'clickhouse_db_pre_2025'),
        (datetime(2025, 3, 10, 9, 0), datetime(2025, 9, 20, 17, 30), 'clickhouse_db'),
        (datetime(2024, 11, 1, 0, 0), datetime(2025, 2, 28, 23, 59), 'clickhouse_db'),
    ]

    for date_from, date_to, expected_db in test_cases:
        result = get_database_for_date_range(date_from, date_to)

        assert result == expected_db, \
            f"DateTime range {date_from} to {date_to}: Expected {expected_db}, got {result}"

        print(f"✓ {date_from} to {date_to} → {result}")

    print("DateTime-based routing tests passed!\n")

def test_string_date_routing():
    """Test ISO string date-based database routing."""
    print("=== Testing ISO String Date-Based Routing ===")

    test_cases = [
        ('2024-06-15', '2024-08-15', 'clickhouse_db_pre_2025'),
        ('2025-03-10', '2025-09-20', 'clickhouse_db'),
        ('2024-11-01T00:00:00Z', '2025-02-28T23:59:59Z', 'clickhouse_db'),
        ('2024-01-01T00:00:00+00:00', '2024-12-31T23:59:59+00:00', 'clickhouse_db_pre_2025'),
    ]

    for date_from, date_to, expected_db in test_cases:
        result = get_database_for_date_range(date_from, date_to)

        assert result == expected_db, \
            f"String date range {date_from} to {date_to}: Expected {expected_db}, got {result}"

        print(f"✓ {date_from} to {date_to} → {result}")

    print("ISO string date-based routing tests passed!\n")

def test_edge_cases():
    """Test edge cases and error handling."""
    print("=== Testing Edge Cases ===")

    # Test with no dates (should use current year)
    current_year = datetime.now().year
    expected_current = 'clickhouse_db' if current_year >= 2025 else 'clickhouse_db_pre_2025'

    result = get_database_for_date_range(None, None)
    assert result == expected_current, \
        f"No dates provided: Expected {expected_current}, got {result}"
    print(f"✓ No dates (current year {current_year}) → {result}")

    # Test with invalid date strings (should handle gracefully)
    try:
        result = get_database_for_date_range('invalid-date', None)
        print(f"✓ Invalid date string handled gracefully → {result}")
    except Exception as e:
        print(f"✗ Invalid date string caused error: {e}")

    print("Edge case tests completed!\n")

def main():
    """Run all database routing tests."""
    print("Database Routing Logic Test Suite")
    print("=" * 50)

    try:
        test_year_routing()
        test_date_range_routing()
        test_datetime_routing()
        test_string_date_routing()
        test_edge_cases()

        print("🎉 All tests passed! Database routing logic is working correctly.")
        print("\nDatabase Configuration Summary:")
        print("- Years < 2025: clickhouse_db_pre_2025")
        print("- Years >= 2025: clickhouse_db")
        print("- Cross-year ranges: Prefer clickhouse_db")

    except AssertionError as e:
        print(f"❌ Test failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

    return True

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)