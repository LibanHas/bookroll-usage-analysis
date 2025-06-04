import logging
import datetime
from typing import Dict, List, Any, Optional
from django.db import connections
from django.conf import settings
from django.core.cache import cache
import json

# Import the student ID extraction function
from past_years.models import extract_student_id_from_actor_account_name

logger = logging.getLogger(__name__)


def get_clickhouse_db_for_academic_year(academic_year: int) -> str:
    """
    Get the appropriate ClickHouse database alias for a given academic year.

    Note: After data migration, all student activity data (including historical data from 2019+)
    is now stored in the 2025+ database. The pre-2025 database contains other types of data
    but not the student activity data we need for time spent analysis.

    Args:
        academic_year (int): The academic year (e.g., 2024, 2025)

    Returns:
        str: Database alias ('clickhouse_db' or 'clickhouse_db_pre_2025')
    """
    return 'clickhouse_db' if academic_year >= 2025 else 'clickhouse_db_pre_2025'


def get_time_spent_by_school_vs_home(start_year: int = 2018, end_year: int = None,
                                   cache_timeout: int = 3600) -> Dict[str, Any]:
    """
    Calculate actual time spent (in hours) segmented by school vs home time for each academic year.

    This function:
    1. Uses sophisticated session boundary detection with leadInFrame()
    2. Applies school time categorization (weekdays during school hours, excluding holidays)
    3. Groups by academic year with monthly breakdown support
    4. Handles both pre-2025 and post-2025 ClickHouse databases
    5. Implements aggressive caching due to 50M+ records

    Args:
        start_year (int): Starting academic year (default: 2019)
        end_year (int): Ending academic year (default: current year - 1)
        cache_timeout (int): Cache timeout in seconds (default: 1 hour)

    Returns:
        Dict containing:
        - yearly_data: List of yearly aggregations
        - monthly_data: List of monthly aggregations
        - summary_stats: Overall statistics
        - metadata: Configuration and cache info
    """
    if end_year is None:
        end_year = datetime.datetime.now().year - 1

    # Create cache key based on parameters
    cache_key = f'time_spent_school_home_{start_year}_{end_year}'
    cached_data = cache.get(cache_key)

    if cached_data:
        logger.info(f"Returning cached time spent data for years {start_year}-{end_year}")
        return cached_data

    logger.info(f"Calculating time spent by school vs home for years {start_year}-{end_year}")

    # Get configuration
    max_session_duration = getattr(settings, 'MAX_SESSION_DURATION', 5400)  # 1.5 hours
    max_activity_duration = 1800  # 30 minutes
    school_start_time = getattr(settings, 'SCHOOL_START_TIME', '09:00')
    school_end_time = getattr(settings, 'SCHOOL_END_TIME', '16:00')

    # Parse school hours
    school_start_hour, school_start_minute = map(int, school_start_time.split(':'))
    school_end_hour, school_end_minute = map(int, school_end_time.split(':'))
    school_start_minutes = school_start_hour * 60 + school_start_minute
    school_end_minutes = school_end_hour * 60 + school_end_minute

    # Get holidays for the entire date range
    from holiday.models import JapaneseHoliday

    # Calculate academic year date ranges
    start_date = f"{start_year}-04-01"
    end_date = f"{end_year + 1}-03-31"

    holidays = set()
    holiday_records = JapaneseHoliday.objects.filter(
        date__gte=start_date,
        date__lte=end_date
    ).values_list('date', flat=True)

    for holiday_date in holiday_records:
        holidays.add(holiday_date.strftime('%Y-%m-%d'))

    logger.info(f"Found {len(holidays)} holidays in date range {start_date} to {end_date}")

    # Prepare result structure
    result = {
        'yearly_data': [],
        'monthly_data': [],
        'summary_stats': {
            'total_school_hours': 0,
            'total_home_hours': 0,
            'total_students_analyzed': 0,
            'years_analyzed': list(range(start_year, end_year + 1))
        },
        'metadata': {
            'date_range': {'start': start_date, 'end': end_date},
            'school_hours': f"{school_start_time}-{school_end_time}",
            'max_session_duration_hours': round(max_session_duration / 3600, 1),
            'max_activity_duration_minutes': round(max_activity_duration / 60, 0),
            'holidays_count': len(holidays),
            'cache_timeout': cache_timeout
        }
    }

    # Track unique students across all years to avoid double counting
    all_unique_students = set()

    # Process each academic year
    for academic_year in range(start_year, end_year + 1):
        year_data = _calculate_time_spent_for_year(
            academic_year=academic_year,
            holidays=holidays,
            school_start_minutes=school_start_minutes,
            school_end_minutes=school_end_minutes,
            max_session_duration=max_session_duration,
            max_activity_duration=max_activity_duration
        )

        if year_data:
            result['yearly_data'].append(year_data['yearly'])
            result['monthly_data'].extend(year_data['monthly'])

            # Update summary stats
            result['summary_stats']['total_school_hours'] += year_data['yearly']['school_hours']
            result['summary_stats']['total_home_hours'] += year_data['yearly']['home_hours']

            # Add unique students from this year to the overall set
            if 'unique_student_ids' in year_data:
                all_unique_students.update(year_data['unique_student_ids'])

    # Set the total unique students count
    result['summary_stats']['total_students_analyzed'] = len(all_unique_students)

    # Sort monthly data by year and month
    result['monthly_data'].sort(key=lambda x: (x['academic_year'], x['month']))

    # Cache the result
    cache.set(cache_key, result, cache_timeout)
    logger.info(f"Cached time spent data for {len(result['yearly_data'])} years")

    return result


def _calculate_time_spent_for_year(academic_year: int, holidays: set,
                                 school_start_minutes: int, school_end_minutes: int,
                                 max_session_duration: int, max_activity_duration: int) -> Optional[Dict[str, Any]]:
    """
    Calculate time spent for a single academic year.

    Args:
        academic_year (int): Academic year to process
        holidays (set): Set of holiday date strings
        school_start_minutes (int): School start time in minutes from midnight
        school_end_minutes (int): School end time in minutes from midnight
        max_session_duration (int): Maximum session duration in seconds
        max_activity_duration (int): Maximum activity duration in seconds

    Returns:
        Dict containing yearly and monthly data, or None if no data
    """
    # Get appropriate database
    db_alias = get_clickhouse_db_for_academic_year(academic_year)

    # Academic year date range (April 1 - March 31)
    start_date = f"{academic_year}-04-01"
    end_date = f"{academic_year + 1}-03-31"

    logger.info(f"Processing academic year {academic_year} using database {db_alias}")

    try:
        with connections[db_alias].cursor() as cursor:
            # Get raw data with actor_account_name for proper student ID extraction
            query = f"""
            SELECT
                academic_year,
                month,
                school_time_flag,
                actor_account_name,
                time_spent_hours
            FROM (
                SELECT
                    actor_account_name,
                    {academic_year} as academic_year,
                    toMonth(activity_date) as month,
                    activity_date,
                    jst_hour,
                    jst_minute,
                    jst_day_of_week,
                    multiIf(
                        jst_day_of_week >= 1 AND jst_day_of_week <= 5 AND
                        activity_date_str NOT IN ({_format_holiday_list(holidays)}) AND
                        (jst_hour * 60 + jst_minute) >= {school_start_minutes} AND
                        (jst_hour * 60 + jst_minute) < {school_end_minutes},
                        1,  -- School time
                        0   -- Home time
                    ) as school_time_flag,
                    round(sum(read_seconds) / 3600, 3) as time_spent_hours
                FROM (
                    SELECT
                        actor_account_name,
                        toDate(addHours(timestamp, 9)) as activity_date,
                        toString(toDate(addHours(timestamp, 9))) as activity_date_str,
                        toHour(addHours(timestamp, 9)) as jst_hour,
                        toMinute(addHours(timestamp, 9)) as jst_minute,
                        toDayOfWeek(addHours(timestamp, 9)) as jst_day_of_week,
                        CASE
                            WHEN time_diff <= {max_session_duration} THEN greatest(0, least({max_activity_duration}, time_diff))
                            ELSE 0
                        END as read_seconds
                    FROM (
                        SELECT
                            actor_account_name,
                            timestamp,
                            dateDiff(
                                'second',
                                timestamp,
                                leadInFrame(timestamp) OVER (
                                    PARTITION BY actor_account_name
                                    ORDER BY timestamp
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                )
                            ) as time_diff
                        FROM statements_mv
                        WHERE actor_account_name != ''
                            AND timestamp >= toDate('{start_date}')
                            AND timestamp <= toDate('{end_date}')
                    )
                )
                GROUP BY
                    actor_account_name,
                    academic_year,
                    month,
                    activity_date,
                    jst_hour,
                    jst_minute,
                    jst_day_of_week,
                    school_time_flag
                HAVING time_spent_hours > 0
            )
            ORDER BY academic_year, month, school_time_flag, actor_account_name
            """
            logger.debug(f"TIME QUERY for {db_alias}: {query}")
            cursor.execute(query)
            raw_data = cursor.fetchall()

            if not raw_data:
                logger.warning(f"No time spent data found for academic year {academic_year}")
                return None

            # Process results and extract student IDs
            yearly_school_hours = 0
            yearly_home_hours = 0
            yearly_students = set()
            monthly_data = {}
            monthly_students = {}  # Track students per month

            for row in raw_data:
                year, month, is_school_time, actor_account_name, hours = row

                # Extract student ID
                student_id = extract_student_id_from_actor_account_name(actor_account_name)
                if not student_id:
                    continue  # Skip if we can't extract a valid student ID

                # Track unique students across the year
                yearly_students.add(student_id)

                # Initialize monthly record if needed
                month_key = f"{year}-{month:02d}"
                if month_key not in monthly_data:
                    monthly_data[month_key] = {
                        'academic_year': year,
                        'month': month,
                        'month_display': f"{year}-{month:02d}",
                        'school_hours': 0,
                        'home_hours': 0,
                        'total_hours': 0,
                        'student_count': 0,
                        'sessions': 0
                    }
                    monthly_students[month_key] = set()

                # Track students for this month
                monthly_students[month_key].add(student_id)

                # Add to monthly data
                if is_school_time == 1:
                    monthly_data[month_key]['school_hours'] += hours
                    yearly_school_hours += hours
                else:
                    monthly_data[month_key]['home_hours'] += hours
                    yearly_home_hours += hours

                monthly_data[month_key]['total_hours'] += hours
                monthly_data[month_key]['sessions'] += 1  # Each row represents a session

            # Update monthly student counts
            for month_key in monthly_data:
                monthly_data[month_key]['student_count'] = len(monthly_students[month_key])

            total_students = len(yearly_students)
            unique_student_ids = yearly_students

            logger.info(f"Academic year {academic_year}: Processed {len(raw_data)} records, extracted {total_students} unique student IDs")

            # Prepare yearly summary
            yearly_data = {
                'academic_year': academic_year,
                'year_display': f"{academic_year}年度",
                'school_hours': round(yearly_school_hours, 2),
                'home_hours': round(yearly_home_hours, 2),
                'total_hours': round(yearly_school_hours + yearly_home_hours, 2),
                'student_count': total_students,
                'school_percentage': round((yearly_school_hours / (yearly_school_hours + yearly_home_hours)) * 100, 1) if (yearly_school_hours + yearly_home_hours) > 0 else 0,
                'home_percentage': round((yearly_home_hours / (yearly_school_hours + yearly_home_hours)) * 100, 1) if (yearly_school_hours + yearly_home_hours) > 0 else 0
            }

            # Convert monthly data to list and add percentages
            monthly_list = []
            for month_data in monthly_data.values():
                total = month_data['school_hours'] + month_data['home_hours']
                month_data['school_percentage'] = round((month_data['school_hours'] / total) * 100, 1) if total > 0 else 0
                month_data['home_percentage'] = round((month_data['home_hours'] / total) * 100, 1) if total > 0 else 0
                monthly_list.append(month_data)

            logger.info(f"Academic year {academic_year}: {yearly_data['total_hours']} total hours, {total_students} students")

            return {
                'yearly': yearly_data,
                'monthly': monthly_list,
                'unique_student_ids': unique_student_ids
            }

    except Exception as e:
        logger.error(f"Error calculating time spent for academic year {academic_year}: {str(e)}")
        return None


def _format_holiday_list(holidays: set) -> str:
    """
    Format holiday set for ClickHouse IN clause.

    Args:
        holidays (set): Set of holiday date strings

    Returns:
        str: Formatted string for SQL IN clause
    """
    if not holidays:
        return "''"

    formatted_holidays = [f"'{holiday}'" for holiday in holidays]
    return ', '.join(formatted_holidays)


def clear_time_spent_cache(start_year: int = None, end_year: int = None) -> bool:
    """
    Clear time spent analysis cache.

    Args:
        start_year (int): If provided, clear cache for specific year range
        end_year (int): If provided, clear cache for specific year range

    Returns:
        bool: True if cache was cleared successfully
    """
    try:
        if start_year and end_year:
            cache_key = f'time_spent_school_home_{start_year}_{end_year}'
            cache.delete(cache_key)
            logger.info(f"Cleared time spent cache for years {start_year}-{end_year}")
        else:
            # Clear all time spent caches
            cache.delete_pattern('time_spent_school_home_*')
            logger.info("Cleared all time spent caches")
        return True
    except Exception as e:
        logger.error(f"Error clearing time spent cache: {str(e)}")
        return False
