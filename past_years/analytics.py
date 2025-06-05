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
                                   cache_timeout: int = 86400) -> Dict[str, Any]:
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
        cache_timeout (int): Cache timeout in seconds (default: 24 hours)

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


def get_engagement_vs_grade_performance(start_year: int = 2018, end_year: int = None,
                                       engagement_metric: str = 'activities_hours',
                                       cache_timeout: int = 7200) -> Dict[str, Any]:
    """
    Calculate engagement vs grade performance by comparing top 25% and bottom 25%
    of students based on their platform engagement (log activity) and their academic performance.

    This function:
    1. Gets students for each academic year using course-based categorization
    2. Calculates platform engagement using the specified metric from ClickHouse
    3. Gets academic performance (average grades) from analysis_db
    4. Segments students into top 25% vs bottom 25% based on engagement
    5. Compares average grades between high and low engagement groups

    Args:
        start_year (int): Starting academic year (default: 2019)
        end_year (int): Ending academic year (default: current year - 1)
        engagement_metric (str): How to calculate engagement ('activities_hours', 'activities', 'hours')
        cache_timeout (int): Cache timeout in seconds (default: 2 hours)

    Returns:
        Dict containing:
        - yearly_data: List of yearly engagement vs grade comparisons
        - summary_stats: Overall statistics
        - metadata: Configuration and analysis info
    """
    if end_year is None:
        end_year = datetime.datetime.now().year - 1

    # Validate engagement metric
    valid_metrics = ['activities_hours', 'activities', 'hours']
    if engagement_metric not in valid_metrics:
        engagement_metric = 'activities_hours'

    # Create cache key based on parameters including engagement metric
    cache_key = f'engagement_vs_grade_{start_year}_{end_year}_{engagement_metric}'
    cached_data = cache.get(cache_key)

    if cached_data:
        logger.info(f"Returning cached engagement vs grade data for years {start_year}-{end_year}, metric: {engagement_metric}")
        return cached_data

    logger.info(f"Calculating engagement vs grade performance for years {start_year}-{end_year}, metric: {engagement_metric}")

    # Import here to avoid circular imports
    from past_years.models import PastYearCourseCategory

    # Prepare result structure
    result = {
        'yearly_data': [],
        'summary_stats': {
            'total_students_analyzed': 0,
            'total_high_engagement': 0,
            'total_low_engagement': 0,
            'years_analyzed': list(range(start_year, end_year + 1)),
            'average_engagement_difference': 0,
            'average_grade_difference': 0
        },
        'metadata': {
            'date_range': {'start': f"{start_year}-04-01", 'end': f"{end_year + 1}-03-31"},
            'engagement_percentiles': {'top': 25, 'bottom': 25},
            'engagement_metric': engagement_metric,
            'grade_metric': 'average_course_grade',
            'cache_timeout': cache_timeout
        }
    }

    total_engagement_differences = []
    total_grade_differences = []
    all_students_count = 0

    # Process each academic year
    for academic_year in range(start_year, end_year + 1):
        year_data = _calculate_engagement_vs_grade_for_year(academic_year, engagement_metric)

        if year_data:
            result['yearly_data'].append(year_data)

            # Update summary stats
            all_students_count += year_data.get('total_students', 0)
            result['summary_stats']['total_high_engagement'] += year_data.get('high_engagement_count', 0)
            result['summary_stats']['total_low_engagement'] += year_data.get('low_engagement_count', 0)

            # Track differences for overall averages
            if year_data.get('engagement_difference') is not None:
                total_engagement_differences.append(year_data['engagement_difference'])
            if year_data.get('grade_difference') is not None:
                total_grade_differences.append(year_data['grade_difference'])

    # Calculate summary averages
    result['summary_stats']['total_students_analyzed'] = all_students_count
    result['summary_stats']['average_engagement_difference'] = round(
        sum(total_engagement_differences) / len(total_engagement_differences), 2
    ) if total_engagement_differences else 0
    result['summary_stats']['average_grade_difference'] = round(
        sum(total_grade_differences) / len(total_grade_differences), 2
    ) if total_grade_differences else 0

    # Cache the result
    cache.set(cache_key, result, cache_timeout)
    logger.info(f"Cached engagement vs grade data for {len(result['yearly_data'])} years with metric {engagement_metric}")

    return result


def _get_students_with_benesse_grades(academic_year: int) -> List[str]:
    """
    Get student IDs who have grades in Benesse courses for the academic year.
    This ensures we only analyze students who actually have meaningful grade data.

    Args:
        academic_year (int): Academic year to process

    Returns:
        List of student IDs who have Benesse grade data for this academic year
    """
    student_ids = []

    try:
        from django.db import connections

        with connections['analysis_db'].cursor() as cursor:
            query = f"""
            SELECT DISTINCT student_id
            FROM course_student_scores
            WHERE course_name LIKE '%{academic_year}年度%'
                AND (name LIKE '%Benesse%' OR name LIKE '%ベネッセ%')
                AND quiz IS NOT NULL
                AND quiz >= 0 AND quiz <= 100
            ORDER BY student_id
            """

            logger.debug(f"BENESSE STUDENT IDS QUERY for analysis_db: {query}")
            cursor.execute(query)
            results = cursor.fetchall()

            student_ids = [str(row[0]) for row in results]

        logger.info(f"Found {len(student_ids)} students with Benesse grades for academic year {academic_year}")
        return student_ids

    except Exception as e:
        logger.error(f"Error getting students with Benesse grades for academic year {academic_year}: {str(e)}")
        return []


def _calculate_engagement_vs_grade_for_year(academic_year: int, engagement_metric: str = 'activities_hours') -> Optional[Dict[str, Any]]:
    """
    Calculate engagement vs grade performance for a single academic year.

    This function ensures data accuracy by:
    1. Getting students who have grades in Benesse courses for this academic year
    2. Getting ALL engagement data for those students (not filtered by course)
    3. Getting grade data from Benesse courses only
    4. Ranking students by engagement and comparing grade performance

    Args:
        academic_year (int): Academic year to process
        engagement_metric (str): How to calculate engagement ('activities_hours', 'activities', 'hours')

    Returns:
        Dict containing engagement vs grade analysis for the year, or None if no data
    """
    logger.info(f"Processing engagement vs grade for academic year {academic_year} (ensuring course_student_scores accuracy)")

    try:
        # Step 1: Get students who have grades in Benesse courses for this academic year
        # We don't need the course IDs for ClickHouse filtering - just the student IDs
        student_user_ids = _get_students_with_benesse_grades(academic_year)

        if not student_user_ids:
            logger.warning(f"No students with Benesse grades found for academic year {academic_year}")
            return None

        logger.info(f"Found {len(student_user_ids)} students with Benesse grades for academic year {academic_year}")

        # Step 2: Get ALL engagement data for these students (no course filtering)
        # This gets their complete platform activity for ranking purposes
        engagement_data = _get_engagement_data_for_students(
            academic_year=academic_year,
            student_user_ids=student_user_ids,
            engagement_metric=engagement_metric
            # Note: Not passing course_ids - we want all activities for these students
        )

        if not engagement_data:
            logger.warning(f"No engagement data found for academic year {academic_year}")
            return None

        # Step 3: Get grade data from Benesse courses only (this is already filtered correctly)
        grade_data = _get_grade_data_for_students(academic_year, student_user_ids)

        if not grade_data:
            logger.warning(f"No grade data found for academic year {academic_year}")
            return None

        # Step 4: Combine engagement and grade data
        combined_data = _combine_engagement_and_grade_data(engagement_data, grade_data)

        if len(combined_data) < 10:  # Need minimum students for meaningful analysis
            logger.warning(f"Not enough combined data for academic year {academic_year}: {len(combined_data)} students")
            return None

        logger.info(f"Successfully combined data for {len(combined_data)} students with both complete engagement and Benesse grade data")

        # Step 5: Rank students by engagement and compare grade performance
        return _analyze_engagement_vs_performance(academic_year, combined_data, engagement_metric)

    except Exception as e:
        logger.error(f"Error calculating engagement vs grade for academic year {academic_year}: {str(e)}")
        return None


def _get_engagement_data_for_students(academic_year: int, student_user_ids: List[str], engagement_metric: str = 'activities_hours') -> Dict[str, Dict[str, Any]]:
    """
    Get engagement data (log activity) for students from ClickHouse.

    Args:
        academic_year (int): Academic year
        student_user_ids (List[str]): List of student user IDs
        engagement_metric (str): How to calculate engagement ('activities_hours', 'activities', 'hours')

    Returns:
        Dict mapping student_id to engagement stats
    """
    # Get appropriate database
    db_alias = get_clickhouse_db_for_academic_year(academic_year)

    # Academic year date range (April 1 - March 31)
    start_date = f"{academic_year}-04-01"
    end_date = f"{academic_year + 1}-03-31"

    engagement_data = {}

    try:
        with connections[db_alias].cursor() as cursor:
            # Convert student user IDs to format for ClickHouse query
            student_placeholders = ', '.join([f"'{uid}'" for uid in student_user_ids[:3000]])  # Limit to avoid query size issues

            query = f"""
            SELECT
                student_id,
                count(*) as total_activities,
                count(DISTINCT toDate(addHours(timestamp, 9))) as active_days,
                round(sum(activity_duration) / 3600, 2) as total_hours,
                round(avg(activity_duration), 2) as avg_activity_duration,
                min(toDate(addHours(timestamp, 9))) as first_activity_date,
                max(toDate(addHours(timestamp, 9))) as last_activity_date
            FROM (
                SELECT
                    extractAll(actor_account_name, '[0-9]+')[1] as student_id,
                    timestamp,
                    CASE
                        WHEN time_diff <= 1800 THEN greatest(0, least(1800, time_diff))
                        ELSE 0
                    END as activity_duration
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
                        AND extractAll(actor_account_name, '[0-9]+')[1] IN ({student_placeholders})
                )
                WHERE activity_duration > 0
            )
            WHERE student_id != ''
            GROUP BY student_id
            HAVING total_activities >= 5  -- Minimum activity threshold
            ORDER BY total_activities DESC
            """

            logger.debug(f"ENGAGEMENT QUERY for {db_alias}: {query}")
            cursor.execute(query)
            results = cursor.fetchall()

            for row in results:
                student_id, total_activities, active_days, total_hours, avg_duration, first_date, last_date = row

                if student_id and student_id in student_user_ids:
                    # Calculate engagement score based on the selected metric
                    if engagement_metric == 'activities':
                        engagement_score = total_activities
                    elif engagement_metric == 'hours':
                        engagement_score = total_hours
                    else:  # 'activities_hours' (default)
                        engagement_score = total_activities * total_hours

                    engagement_data[student_id] = {
                        'student_id': student_id,
                        'total_activities': total_activities,
                        'active_days': active_days,
                        'total_hours': total_hours,
                        'avg_activity_duration': avg_duration,
                        'first_activity_date': str(first_date),
                        'last_activity_date': str(last_date),
                        'engagement_score': engagement_score,
                        'engagement_metric_used': engagement_metric
                    }

        logger.info(f"Found engagement data for {len(engagement_data)} students in academic year {academic_year} (no course filter)")
        return engagement_data

    except Exception as e:
        logger.error(f"Error getting engagement data for academic year {academic_year}: {str(e)}")
        return {}


def _get_grade_data_for_students(academic_year: int, student_user_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Get grade data for students from analysis_db using course-based academic year filtering.

    Args:
        academic_year (int): Academic year
        student_user_ids (List[str]): List of student user IDs

    Returns:
        Dict mapping student_id to grade stats
    """
    grade_data = {}

    try:
        from django.db import connections

        with connections['analysis_db'].cursor() as cursor:
            # Use course name pattern to filter by academic year
            student_placeholders = ', '.join([f"'{uid}'" for uid in student_user_ids])

            query = f"""
            SELECT
                student_id,
                count(*) as total_grades,
                round(avg(quiz), 2) as average_grade,
                round(min(quiz), 2) as min_grade,
                round(max(quiz), 2) as max_grade,
                round(stddev(quiz), 2) as grade_stddev,
                count(DISTINCT course_id) as course_count
            FROM course_student_scores
            WHERE student_id IN ({student_placeholders})
                AND course_name LIKE '%{academic_year}年度%'
                AND (name LIKE '%Benesse%' OR name LIKE '%ベネッセ%')
                AND quiz IS NOT NULL
                AND quiz >= 0 AND quiz <= 100
            GROUP BY student_id
            HAVING total_grades >= 3  -- Minimum grade records for reliability
            ORDER BY average_grade DESC
            """

            logger.debug(f"GRADE QUERY for analysis_db: {query}")
            cursor.execute(query)
            results = cursor.fetchall()

            for row in results:
                student_id, total_grades, avg_grade, min_grade, max_grade, grade_stddev, course_count = row

                grade_data[student_id] = {
                    'student_id': student_id,
                    'total_grades': total_grades,
                    'average_grade': avg_grade,
                    'min_grade': min_grade,
                    'max_grade': max_grade,
                    'grade_stddev': grade_stddev or 0,
                    'course_count': course_count
                }

        logger.info(f"Found grade data for {len(grade_data)} students in academic year {academic_year}")
        return grade_data

    except Exception as e:
        logger.error(f"Error getting grade data for academic year {academic_year}: {str(e)}")
        return {}


def _combine_engagement_and_grade_data(engagement_data: Dict[str, Dict], grade_data: Dict[str, Dict]) -> List[Dict[str, Any]]:
    """
    Combine engagement and grade data for students who have both.

    Args:
        engagement_data (Dict): Student engagement data
        grade_data (Dict): Student grade data

    Returns:
        List of combined student data
    """
    combined_data = []

    # Find students who have both engagement and grade data
    common_students = set(engagement_data.keys()) & set(grade_data.keys())

    for student_id in common_students:
        engagement = engagement_data[student_id]
        grades = grade_data[student_id]

        combined_student = {
            'student_id': student_id,
            # Engagement metrics
            'total_activities': engagement['total_activities'],
            'active_days': engagement['active_days'],
            'total_hours': engagement['total_hours'],
            'avg_activity_duration': engagement['avg_activity_duration'],
            'engagement_score': engagement['engagement_score'],
            # Grade metrics
            'total_grades': grades['total_grades'],
            'average_grade': grades['average_grade'],
            'min_grade': grades['min_grade'],
            'max_grade': grades['max_grade'],
            'grade_stddev': grades['grade_stddev'],
            'course_count': grades['course_count']
        }

        combined_data.append(combined_student)

    logger.info(f"Combined data for {len(combined_data)} students with both engagement and grade data")
    return combined_data


def _analyze_engagement_vs_performance(academic_year: int, combined_data: List[Dict[str, Any]], engagement_metric: str = 'activities_hours') -> Dict[str, Any]:
    """
    Analyze engagement vs performance by comparing top 25% and bottom 25% engagement groups.

    Args:
        academic_year (int): Academic year
        combined_data (List): Combined student data with engagement and grades
        engagement_metric (str): How engagement was calculated ('activities_hours', 'activities', 'hours')

    Returns:
        Dict containing analysis results
    """
    # Sort by engagement score (descending)
    sorted_by_engagement = sorted(combined_data, key=lambda x: x['engagement_score'], reverse=True)

    total_students = len(sorted_by_engagement)
    top_25_count = max(1, total_students // 4)
    bottom_25_count = max(1, total_students // 4)

    # Get top 25% and bottom 25% by engagement
    high_engagement_students = sorted_by_engagement[:top_25_count]
    low_engagement_students = sorted_by_engagement[-bottom_25_count:]

    # Calculate averages for high engagement group
    high_engagement_avg_grade = sum(s['average_grade'] for s in high_engagement_students) / len(high_engagement_students)
    high_engagement_avg_activities = sum(s['total_activities'] for s in high_engagement_students) / len(high_engagement_students)
    high_engagement_avg_hours = sum(s['total_hours'] for s in high_engagement_students) / len(high_engagement_students)
    high_engagement_avg_score = sum(s['engagement_score'] for s in high_engagement_students) / len(high_engagement_students)

    # Calculate averages for low engagement group
    low_engagement_avg_grade = sum(s['average_grade'] for s in low_engagement_students) / len(low_engagement_students)
    low_engagement_avg_activities = sum(s['total_activities'] for s in low_engagement_students) / len(low_engagement_students)
    low_engagement_avg_hours = sum(s['total_hours'] for s in low_engagement_students) / len(low_engagement_students)
    low_engagement_avg_score = sum(s['engagement_score'] for s in low_engagement_students) / len(low_engagement_students)

    # Calculate differences
    grade_difference = high_engagement_avg_grade - low_engagement_avg_grade
    engagement_difference = high_engagement_avg_score - low_engagement_avg_score

    year_data = {
        'academic_year': academic_year,
        'year_display': f"{academic_year}年度",
        'total_students': total_students,
        'engagement_metric_used': engagement_metric,

        # High engagement group
        'high_engagement_count': len(high_engagement_students),
        'high_engagement_avg_grade': round(high_engagement_avg_grade, 2),
        'high_engagement_avg_activities': round(high_engagement_avg_activities, 1),
        'high_engagement_avg_hours': round(high_engagement_avg_hours, 2),
        'high_engagement_avg_score': round(high_engagement_avg_score, 2),
        'high_engagement_percentage': 25,  # Always 25% for consistency

        # Low engagement group
        'low_engagement_count': len(low_engagement_students),
        'low_engagement_avg_grade': round(low_engagement_avg_grade, 2),
        'low_engagement_avg_activities': round(low_engagement_avg_activities, 1),
        'low_engagement_avg_hours': round(low_engagement_avg_hours, 2),
        'low_engagement_avg_score': round(low_engagement_avg_score, 2),
        'low_engagement_percentage': 25,  # Always 25% for consistency

        # Differences and insights
        'grade_difference': round(grade_difference, 2),
        'engagement_difference': round(engagement_difference, 2),
        'correlation_strength': 'positive' if grade_difference > 0 else 'negative' if grade_difference < 0 else 'neutral',

        # Additional stats
        'middle_students_count': total_students - len(high_engagement_students) - len(low_engagement_students),
        'overall_avg_grade': round(sum(s['average_grade'] for s in combined_data) / len(combined_data), 2),
        'overall_avg_activities': round(sum(s['total_activities'] for s in combined_data) / len(combined_data), 1),
        'overall_avg_hours': round(sum(s['total_hours'] for s in combined_data) / len(combined_data), 2),
        'overall_avg_engagement_score': round(sum(s['engagement_score'] for s in combined_data) / len(combined_data), 2)
    }

    logger.info(f"Academic year {academic_year} ({engagement_metric}): Grade difference {grade_difference:.2f} points, "
                f"Engagement difference {engagement_difference:.2f}")

    return year_data


def clear_engagement_vs_grade_cache(start_year: int = None, end_year: int = None, engagement_metric: str = None) -> bool:
    """
    Clear engagement vs grade analysis cache.

    Args:
        start_year (int): If provided, clear cache for specific year range
        end_year (int): If provided, clear cache for specific year range
        engagement_metric (str): If provided, clear cache for specific engagement metric

    Returns:
        bool: True if cache was cleared successfully
    """
    try:
        if start_year and end_year and engagement_metric:
            cache_key = f'engagement_vs_grade_{start_year}_{end_year}_{engagement_metric}'
            cache.delete(cache_key)
            logger.info(f"Cleared engagement vs grade cache for years {start_year}-{end_year}, metric: {engagement_metric}")
        elif start_year and end_year:
            # Clear all metrics for the year range
            for metric in ['activities_hours', 'activities', 'hours']:
                cache_key = f'engagement_vs_grade_{start_year}_{end_year}_{metric}'
                cache.delete(cache_key)
            logger.info(f"Cleared all engagement vs grade caches for years {start_year}-{end_year}")
        else:
            # Clear all engagement vs grade caches
            cache.delete_pattern('engagement_vs_grade_*')
            logger.info("Cleared all engagement vs grade caches")
        return True
    except Exception as e:
        logger.error(f"Error clearing engagement vs grade cache: {str(e)}")
        return False
