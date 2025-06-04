from django.db import models, connections
from django.core.cache import cache
from django.conf import settings
import datetime
import logging
import re
from typing import Dict, List, Any, Optional
import statistics
import hashlib
import json
import time
import numpy as np
import random

logger = logging.getLogger(__name__)


def get_clickhouse_db_for_academic_year(academic_year: int) -> str:
    """
    Get the appropriate ClickHouse database alias for a given academic year.

    This function implements the database routing logic:
    - Years before 2025: Use 'clickhouse_db_pre_2025' database
    - Years 2025 and after: Use 'clickhouse_db' database

    Args:
        academic_year (int): The academic year (e.g., 2024, 2025)

    Returns:
        str: Database alias ('clickhouse_db' or 'clickhouse_db_pre_2025')
    """
    return 'clickhouse_db' if academic_year >= 2025 else 'clickhouse_db_pre_2025'


def get_clickhouse_db_for_date_range(start_date: str = None, end_date: str = None) -> str:
    """
    Get the appropriate ClickHouse database for a date range.

    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format
        end_date (str): End date in 'YYYY-MM-DD' format

    Returns:
        str: Database alias ('clickhouse_db' or 'clickhouse_db_pre_2025')
    """
    years = []

    for date_str in [start_date, end_date]:
        if date_str:
            try:
                year = int(date_str[:4])  # Extract year from YYYY-MM-DD
                years.append(year)
            except (ValueError, TypeError):
                continue

    if not years:
        # No valid dates, use current year logic
        current_year = datetime.datetime.now().year
        return get_clickhouse_db_for_academic_year(current_year)

    # If any year is 2025 or later, use the new database
    if any(year >= 2025 for year in years):
        return 'clickhouse_db'
    else:
        return 'clickhouse_db_pre_2025'


# Cache configuration for historical data
CACHE_CONFIG = {
    'DEFAULT_TTL': 3600 * 6,  # 6 hours for most data
    'LONG_TTL': 3600 * 24,    # 24 hours for stable historical data
    'SHORT_TTL': 3600,        # 1 hour for frequently changing data
    'LOG_ANALYTICS_TTL': 3600 * 12,  # 12 hours for log analytics
    'COURSE_DATA_TTL': 3600 * 8,     # 8 hours for course data
}

def generate_cache_key(*args, **kwargs) -> str:
    """
    Generate a consistent cache key from arguments.

    Args:
        *args: Positional arguments to include in key
        **kwargs: Keyword arguments to include in key

    Returns:
        str: A consistent cache key
    """
    # Create a string representation of all arguments
    key_parts = []

    # Add positional arguments
    for arg in args:
        if isinstance(arg, (list, dict)):
            # For complex types, create a hash
            key_parts.append(hashlib.md5(json.dumps(arg, sort_keys=True, default=str).encode()).hexdigest()[:8])
        else:
            key_parts.append(str(arg))

    # Add keyword arguments
    for key, value in sorted(kwargs.items()):
        if isinstance(value, (list, dict)):
            value_hash = hashlib.md5(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()[:8]
            key_parts.append(f"{key}_{value_hash}")
        else:
            key_parts.append(f"{key}_{value}")

    # Join with underscores and ensure it's not too long
    cache_key = "_".join(key_parts)

    # Redis keys should be under 250 characters
    if len(cache_key) > 200:
        # Create a hash of the long key
        cache_key = hashlib.md5(cache_key.encode()).hexdigest()

    return f"past_years_{cache_key}"

def clear_all_past_years_cache() -> Dict[str, Any]:
    """
    Clear all past years related cache entries.

    Returns:
        Dict with clearing results
    """
    try:
        # Get Redis connection
        from django.core.cache.backends.redis import RedisCache

        if not isinstance(cache, RedisCache):
            logger.warning("Cache backend is not Redis, using Django cache.clear()")
            cache.clear()
            return {
                'success': True,
                'method': 'django_clear_all',
                'keys_cleared': 'all',
                'message': 'All cache cleared (non-Redis backend)'
            }

        # Get Redis client
        redis_client = cache._cache.get_client(write=True)

        # Find all past years related keys
        patterns = [
            'past_years_*',
            'available_academic_years*',
            'student_user_ids_*',
            'non_student_user_ids_*',
            'student_analytics_*',
            'cache_registry_*',
            'course_grade_distribution_*',
            'log_analytics_*',
            'grade_performance_*',
            'grade_analytics_*',
            'course_activity_*',
            'engagement_patterns_*'
        ]

        total_cleared = 0
        cleared_patterns = []

        for pattern in patterns:
            keys = redis_client.keys(pattern)
            if keys:
                redis_client.delete(*keys)
                total_cleared += len(keys)
                cleared_patterns.append(f"{pattern}: {len(keys)} keys")
                logger.info(f"Cleared {len(keys)} keys matching pattern: {pattern}")

        logger.info(f"Cache clear completed: {total_cleared} total keys cleared")

        return {
            'success': True,
            'method': 'redis_pattern_clear',
            'keys_cleared': total_cleared,
            'patterns_cleared': cleared_patterns,
            'message': f'Successfully cleared {total_cleared} cache entries'
        }

    except Exception as e:
        logger.error(f"Error clearing past years cache: {str(e)}")

        # Fallback to Django's cache.clear()
        try:
            cache.clear()
            return {
                'success': True,
                'method': 'django_fallback_clear',
                'keys_cleared': 'all',
                'message': 'Cache cleared using Django fallback method',
                'error': str(e)
            }
        except Exception as fallback_error:
            return {
                'success': False,
                'method': 'failed',
                'keys_cleared': 0,
                'message': f'Failed to clear cache: {str(fallback_error)}',
                'original_error': str(e)
            }

class CachedModelMixin:
    """Mixin to provide caching functionality to models"""

    @classmethod
    def get_cached_data(cls, cache_key: str, fetch_function, ttl: int = None, *args, **kwargs):
        """
        Generic method to get cached data or fetch and cache it.

        Args:
            cache_key (str): The cache key to use
            fetch_function: Function to call if cache miss
            ttl (int): Time to live in seconds
            *args, **kwargs: Arguments to pass to fetch_function

        Returns:
            The cached or freshly fetched data
        """
        if ttl is None:
            ttl = CACHE_CONFIG['DEFAULT_TTL']

        # Try to get from cache first
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.info(f"Cache HIT for key: {cache_key}")
            return cached_data

        logger.info(f"Cache MISS for key: {cache_key}, fetching fresh data")

        # Fetch fresh data
        try:
            fresh_data = fetch_function(*args, **kwargs)

            # Cache the result
            cache.set(cache_key, fresh_data, ttl)
            logger.info(f"Cached data with key: {cache_key}, TTL: {ttl}s")

            return fresh_data

        except Exception as e:
            logger.error(f"Error fetching data for cache key {cache_key}: {str(e)}")
            # Return empty result structure to prevent crashes
            return {}

    @classmethod
    def invalidate_cache_pattern(cls, pattern: str) -> int:
        """
        Invalidate all cache keys matching a pattern.

        Args:
            pattern (str): Pattern to match (e.g., 'past_years_2024_*')

        Returns:
            int: Number of keys invalidated
        """
        try:
            from django.core.cache.backends.redis import RedisCache

            if not isinstance(cache, RedisCache):
                logger.warning("Cannot use pattern invalidation with non-Redis cache")
                return 0

            redis_client = cache._cache.get_client(write=True)
            keys = redis_client.keys(pattern)

            if keys:
                redis_client.delete(*keys)
                logger.info(f"Invalidated {len(keys)} cache keys matching pattern: {pattern}")
                return len(keys)

            return 0

        except Exception as e:
            logger.error(f"Error invalidating cache pattern {pattern}: {str(e)}")
            return 0


class PastYearCourseCategory(CachedModelMixin, models.Model):
    """Model to access course categories for past years analysis"""
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=255)

    class Meta:
        managed = False
        app_label = 'moodle_app'

    @classmethod
    def get_academic_year_from_category_name(cls, category_name: str) -> Optional[int]:
        """
        Extract academic year from category name.
        Looks for patterns like '2024年度', '2023年度', etc.
        Returns the academic year as integer (e.g., 2024 for '2024年度')
        """
        # Pattern to match Japanese academic year format: YYYY年度
        pattern = r'(\d{4})年度'
        match = re.search(pattern, category_name)
        if match:
            logger.info(f"Academic year found---------------->: {match.group(1)}")
            return int(match.group(1))
        return None

    @classmethod
    def get_courses_by_academic_year(cls, academic_year: int) -> Dict[str, Any]:
        """
        Get all courses for a specific academic year with Redis caching.
        """
        cache_key = generate_cache_key('courses_by_year', academic_year)

        def fetch_courses():
            return cls._fetch_courses_by_academic_year(academic_year)

        return cls.get_cached_data(
            cache_key,
            fetch_courses,
            ttl=CACHE_CONFIG['COURSE_DATA_TTL']
        )

    @classmethod
    def _fetch_courses_by_academic_year(cls, academic_year: int) -> Dict[str, Any]:
        """Original implementation moved to separate method for caching"""
        logger.info(f"Fetching course data for academic year {academic_year} - NO CACHE FOUND")

        try:
            with connections['moodle_db'].cursor() as cursor:
                # [Previous implementation remains the same]
                # Use the working query to get all courses with their category hierarchy
                courses_query = """
                    SELECT
                        parent_cat.id AS parent_category_id,
                        parent_cat.name AS parent_category_name,
                        child_cat.id AS child_category_id,
                        child_cat.name AS child_category_name,
                        course.id AS course_id,
                        course.fullname AS course_name,
                        course.shortname AS course_shortname,
                        course.summary AS course_summary,
                        course.sortorder AS course_sortorder,
                        course.visible AS course_visible,
                        course.startdate AS course_startdate,
                        course.enddate AS course_enddate,
                        course.timecreated AS course_created,
                        course.timemodified AS course_modified
                    FROM mdl_course_categories parent_cat
                    JOIN mdl_course_categories child_cat ON child_cat.parent = parent_cat.id
                    LEFT JOIN mdl_course course ON course.category = child_cat.id
                    WHERE parent_cat.parent = 0
                    AND course.id IS NOT NULL
                    ORDER BY parent_cat.sortorder, child_cat.sortorder, course.sortorder
                """

                cursor.execute(courses_query)
                rows = cursor.fetchall()

                # Initialize result structure
                year_courses = {
                    'academic_year': academic_year,
                    'categories': {},
                    'total_courses': 0,
                    'course_summary': {
                        'by_category': {},
                        'by_month_created': {},
                        'total_visible': 0
                    }
                }

                matched_courses_count = 0

                for row in rows:
                    parent_id = row[0]
                    parent_name = row[1]
                    child_id = row[2]
                    child_name = row[3]
                    course_id = row[4]
                    course_name = row[5]
                    course_shortname = row[6]
                    course_summary = row[7]
                    course_sortorder = row[8]
                    course_visible = row[9]
                    course_startdate = row[10]
                    course_enddate = row[11]
                    course_created = row[12]
                    course_modified = row[13]

                    # Check if parent category contains the academic year
                    parent_year = cls.get_academic_year_from_category_name(parent_name)

                    # Only include courses if parent category matches the academic year
                    if parent_year == academic_year:
                        matched_courses_count += 1

                        # Convert Unix timestamps to datetime objects
                        if course_startdate:
                            course_startdate = datetime.datetime.fromtimestamp(course_startdate)
                        if course_enddate:
                            course_enddate = datetime.datetime.fromtimestamp(course_enddate)
                        if course_created:
                            course_created = datetime.datetime.fromtimestamp(course_created)
                        if course_modified:
                            course_modified = datetime.datetime.fromtimestamp(course_modified)

                        # Add parent category if not exists
                        if parent_id not in year_courses['categories']:
                            year_courses['categories'][parent_id] = {
                                'id': parent_id,
                                'name': parent_name,
                                'academic_year': parent_year,
                                'children': {},
                                'course_count': 0
                            }

                        # Add child category if not exists
                        if child_id not in year_courses['categories'][parent_id]['children']:
                            year_courses['categories'][parent_id]['children'][child_id] = {
                                'id': child_id,
                                'name': child_name,
                                'academic_year': parent_year,
                                'courses': [],
                                'course_count': 0
                            }

                        # Add course
                        course_data = {
                            'id': course_id,
                            'name': course_name,
                            'shortname': course_shortname,
                            'summary': course_summary,
                            'sortorder': course_sortorder,
                            'visible': course_visible,
                            'startdate': course_startdate,
                            'enddate': course_enddate,
                            'created': course_created,
                            'modified': course_modified
                        }

                        year_courses['categories'][parent_id]['children'][child_id]['courses'].append(course_data)
                        year_courses['categories'][parent_id]['children'][child_id]['course_count'] += 1
                        year_courses['categories'][parent_id]['course_count'] += 1
                        year_courses['total_courses'] += 1

                        # Update summary statistics
                        category_key = f"{parent_name} > {child_name}"
                        if category_key not in year_courses['course_summary']['by_category']:
                            year_courses['course_summary']['by_category'][category_key] = 0
                        year_courses['course_summary']['by_category'][category_key] += 1

                        # Use course creation date for monthly summary if available
                        if course_created:
                            month_key = course_created.strftime('%Y-%m')
                            if month_key not in year_courses['course_summary']['by_month_created']:
                                year_courses['course_summary']['by_month_created'][month_key] = 0
                            year_courses['course_summary']['by_month_created'][month_key] += 1

                        # Count visible courses for summary
                        if course_visible:
                            year_courses['course_summary']['total_visible'] += 1

                logger.info(f"Fetched {matched_courses_count} courses for academic year {academic_year}")
                return year_courses

        except Exception as e:
            logger.error(f"Error fetching courses for academic year {academic_year}: {str(e)}")
            return {
                'academic_year': academic_year,
                'categories': {},
                'total_courses': 0,
                'course_summary': {
                    'by_category': {},
                    'by_month_created': {},
                    'total_visible': 0
                },
                'error': str(e)
            }

    @classmethod
    def get_available_academic_years(cls) -> List[int]:
        """
        Get all available academic years from course categories with Redis caching.
        """
        cache_key = generate_cache_key('available_academic_years')

        def fetch_years():
            return cls._fetch_available_academic_years()

        return cls.get_cached_data(
            cache_key,
            fetch_years,
            ttl=CACHE_CONFIG['LONG_TTL']
        )

    @classmethod
    def _fetch_available_academic_years(cls) -> List[int]:
        """Original implementation for fetching available years"""
        logger.info("Fetching available academic years from categories")

        try:
            with connections['moodle_db'].cursor() as cursor:
                query = """
                    SELECT DISTINCT name
                    FROM mdl_course_categories
                    WHERE parent = 0
                    ORDER BY name DESC
                """

                cursor.execute(query)
                category_names = [row[0] for row in cursor.fetchall()]

                # Extract academic years from category names
                academic_years = []
                for name in category_names:
                    year = cls.get_academic_year_from_category_name(name)
                    if year and year not in academic_years:
                        academic_years.append(year)

                # Sort in descending order (most recent first)
                academic_years.sort(reverse=True)
                return academic_years

        except Exception as e:
            logger.error(f"Error fetching available academic years: {str(e)}")
            return []

    @classmethod
    def get_student_user_ids_for_academic_year(cls, academic_year: int) -> List[str]:
        """
        Get all student user IDs enrolled in courses for a specific academic year with Redis caching.
        """
        cache_key = generate_cache_key('student_user_ids', academic_year)

        def fetch_student_ids():
            return cls._fetch_student_user_ids_for_academic_year(academic_year)

        return cls.get_cached_data(
            cache_key,
            fetch_student_ids,
            ttl=CACHE_CONFIG['DEFAULT_TTL']
        )

    @classmethod
    def _fetch_student_user_ids_for_academic_year(cls, academic_year: int) -> List[str]:
        """Original implementation for fetching student user IDs"""
        logger.info(f"Fetching student user IDs for academic year {academic_year}")

        try:
            # First get course IDs for the academic year
            year_courses = cls.get_courses_by_academic_year(academic_year)
            if not year_courses or not year_courses.get('categories'):
                logger.warning(f"No courses found for academic year {academic_year}")
                return []

            # Collect all course IDs for the academic year
            course_ids = []
            for parent_category in year_courses['categories'].values():
                for child_category in parent_category['children'].values():
                    for course in child_category['courses']:
                        course_ids.append(course['id'])

            if not course_ids:
                logger.warning(f"No course IDs found for academic year {academic_year}")
                return []

            # Get students enrolled in these courses
            with connections['moodle_db'].cursor() as cursor:
                course_placeholders = ','.join(['%s'] * len(course_ids))
                query = f"""
                    SELECT DISTINCT u.id
                    FROM mdl_user u
                    JOIN mdl_role_assignments ra ON u.id = ra.userid
                    JOIN mdl_role r ON ra.roleid = r.id
                    JOIN mdl_context ctx ON ra.contextid = ctx.id
                    JOIN mdl_course c ON ctx.instanceid = c.id
                    WHERE r.shortname = 'student'
                    AND ctx.contextlevel = 50
                    AND u.deleted = 0
                    AND u.suspended = 0
                    AND c.id IN ({course_placeholders})
                    ORDER BY u.id
                """

                cursor.execute(query, course_ids)
                student_records = cursor.fetchall()

            # Convert to list of strings
            student_user_ids = [str(record[0]) for record in student_records]
            logger.info(f"Found {len(student_user_ids)} students for academic year {academic_year}")
            return student_user_ids

        except Exception as e:
            logger.error(f"Error fetching student user IDs for academic year {academic_year}: {str(e)}")
            return []

    @classmethod
    def clear_cache_for_year(cls, academic_year: int) -> bool:
        """Clear all cache keys for a specific academic year"""
        try:
            patterns_to_clear = [
                f"past_years_*{academic_year}*",
                f"past_years_courses_by_year_{academic_year}*",
                f"past_years_student_user_ids_{academic_year}*",
                f"past_years_student_analytics_{academic_year}*",
            ]

            total_cleared = 0
            for pattern in patterns_to_clear:
                cleared = cls.invalidate_cache_pattern(pattern)
                total_cleared += cleared

            logger.info(f"Cleared {total_cleared} cache keys for academic year {academic_year}")
            return True

        except Exception as e:
            logger.error(f"Error clearing cache for year {academic_year}: {str(e)}")
            return False

    @classmethod
    def get_non_student_user_ids_for_academic_year(cls, academic_year: int) -> List[str]:
        """
        Get all non-student user IDs (teachers, managers, etc.) enrolled in courses for a specific academic year.
        This is more efficient when there are fewer non-students than students.

        Args:
            academic_year (int): The academic year (e.g., 2024 for 2024年度)

        Returns:
            List[str]: List of non-student user IDs for the academic year
        """
        cache_key = f'non_student_user_ids_{academic_year}'
        cached_data = cache.get(cache_key)

        if cached_data:
            logger.info(f"Using cached non-student user IDs for academic year {academic_year}: {len(cached_data)} non-students")
            return cached_data

        logger.info(f"Fetching non-student user IDs for academic year {academic_year}")

        try:
            # First get course IDs for the academic year
            year_courses = cls.get_courses_by_academic_year(academic_year)
            if not year_courses or not year_courses.get('categories'):
                logger.warning(f"No courses found for academic year {academic_year}")
                return []

            # Collect all course IDs for the academic year
            course_ids = []
            for parent_category in year_courses['categories'].values():
                for child_category in parent_category['children'].values():
                    for course in child_category['courses']:
                        course_ids.append(course['id'])

            if not course_ids:
                logger.warning(f"No course IDs found for academic year {academic_year}")
                return []

            logger.info(f"Found {len(course_ids)} courses for academic year {academic_year}")

            # Get non-students (teachers, managers, etc.) enrolled in these courses
            with connections['moodle_db'].cursor() as cursor:
                # Build query with placeholders for course IDs
                course_placeholders = ','.join(['%s'] * len(course_ids))
                query = f"""
                    SELECT DISTINCT u.id
                    FROM mdl_user u
                    JOIN mdl_role_assignments ra ON u.id = ra.userid
                    JOIN mdl_role r ON ra.roleid = r.id
                    JOIN mdl_context ctx ON ra.contextid = ctx.id
                    JOIN mdl_course c ON ctx.instanceid = c.id
                    WHERE r.shortname IN ('teacher', 'editingteacher', 'manager', 'coursecreator')
                    AND ctx.contextlevel = 50
                    AND u.deleted = 0
                    AND u.suspended = 0
                    AND c.id IN ({course_placeholders})
                    ORDER BY u.id
                """

                logger.debug(f"Executing non-student enrollment query for {len(course_ids)} courses")
                cursor.execute(query, course_ids)
                non_student_records = cursor.fetchall()

            # Convert to list of strings for consistency with ClickHouse data
            non_student_user_ids = [str(record[0]) for record in non_student_records]

            logger.info(f"Found {len(non_student_user_ids)} non-students enrolled in academic year {academic_year} courses")

            # Cache for 1 hour
            cache.set(cache_key, non_student_user_ids, 3600)

            return non_student_user_ids

        except Exception as e:
            logger.error(f"Error fetching non-student user IDs for academic year {academic_year}: {str(e)}")
            return []

    @classmethod
    def get_optimal_student_filter_for_academic_year(cls, academic_year: int) -> Dict[str, Any]:
        """
        Determine the most efficient filtering approach (IN students vs NOT IN non-students).
        Returns the filter configuration that should be most performant.

        Args:
            academic_year (int): The academic year

        Returns:
            Dict with filter type and parameters
        """
        try:
            student_ids = cls.get_student_user_ids_for_academic_year(academic_year)
            non_student_ids = cls.get_non_student_user_ids_for_academic_year(academic_year)

            student_count = len(student_ids)
            non_student_count = len(non_student_ids)

            logger.info(f"FILTER OPTIMIZATION: Academic year {academic_year} - Students: {student_count}, Non-students: {non_student_count}")

            # Use NOT IN if non-students are significantly fewer (less than 30% of students)
            # and there are actual non-students to filter out
            if non_student_count > 0 and non_student_count < (student_count * 0.3):
                logger.info(f"FILTER OPTIMIZATION: Using NOT IN approach (more efficient with {non_student_count} non-students vs {student_count} students)")
                return {
                    'filter_type': 'NOT_IN',
                    'filter_ids': non_student_ids,
                    'filter_count': non_student_count,
                    'efficiency_reason': f'NOT IN with {non_student_count} non-students is more efficient than IN with {student_count} students'
                }
            else:
                logger.info(f"FILTER OPTIMIZATION: Using IN approach (standard with {student_count} students)")
                return {
                    'filter_type': 'IN',
                    'filter_ids': student_ids,
                    'filter_count': student_count,
                    'efficiency_reason': f'IN with {student_count} students is standard approach' + (f' (only {non_student_count} non-students, not worth NOT IN)' if non_student_count > 0 else ' (no non-students found)')
                }

        except Exception as e:
            logger.error(f"Error determining optimal filter for academic year {academic_year}: {str(e)}")
            # Fallback to student IN approach
            student_ids = cls.get_student_user_ids_for_academic_year(academic_year)
            return {
                'filter_type': 'IN',
                'filter_ids': student_ids,
                'filter_count': len(student_ids),
                'efficiency_reason': 'Fallback to standard IN approach due to error'
            }


    @classmethod
    def get_course_grade_distribution(cls, course_id: str, academic_year: int) -> Dict[str, Any]:
        """
        Get individual student grades for a specific course to create distribution charts.
        Uses course-based filtering only (consistent with main analytics).

        Args:
            course_id (str): The course ID to get grades for
            academic_year (int): The academic year to filter by

        Returns:
            Dict containing individual grades and distribution data
        """
        logger.info(f"Fetching grade distribution for course {course_id} in academic year {academic_year}")

        try:
            # Get optimal student filtering approach
            filter_config = PastYearCourseCategory.get_optimal_student_filter_for_academic_year(academic_year)
            filter_type = filter_config['filter_type']
            filter_ids = filter_config['filter_ids']
            filter_count = filter_config['filter_count']

            if not filter_ids:
                logger.warning(f"No filter IDs found for academic year {academic_year}")
                return {
                    'course_id': course_id,
                    'individual_grades': [],
                    'distribution_data': [],
                    'stats': {},
                    'error': 'No student filter data available'
                }

            # Verify that this course belongs to the specified academic year
            courses_data = PastYearCourseCategory.get_courses_by_academic_year(academic_year)
            valid_course_ids = []
            if courses_data and courses_data.get('categories'):
                for category in courses_data.get('categories', {}).values():
                    for child_category in category.get('children', {}).values():
                        valid_course_ids.extend([str(course['id']) for course in child_category.get('courses', [])])

            if course_id not in valid_course_ids:
                logger.warning(f"Course {course_id} does not belong to academic year {academic_year}")
                return {
                    'course_id': course_id,
                    'individual_grades': [],
                    'distribution_data': [],
                    'stats': {},
                    'error': f'Course {course_id} does not belong to academic year {academic_year}'
                }

            with connections['analysis_db'].cursor() as cursor:
                # Build student filter clause
                filter_placeholders = ",".join(["%s"] * len(filter_ids))
                if filter_type == 'NOT_IN':
                    student_filter = f" AND student_id NOT IN ({filter_placeholders}) AND student_id IS NOT NULL"
                else:
                    student_filter = f" AND student_id IN ({filter_placeholders})"

                # Get individual grades for the course - ONLY course and student filtering (NO DATE FILTERING)
                individual_grades_query = f"""
                    SELECT
                        student_id,
                        quiz as grade,
                        created_at,
                        course_name,
                        name as grade_file_name
                    FROM course_student_scores
                    WHERE course_id = %s
                    AND quiz IS NOT NULL
                    AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%') {student_filter}
                    ORDER BY quiz DESC
                """

                cursor.execute(individual_grades_query, [course_id] + filter_ids)
                individual_grades = cursor.fetchall()

                if not individual_grades:
                    return {
                        'course_id': course_id,
                        'individual_grades': [],
                        'distribution_data': [],
                        'stats': {},
                        'error': f'No Benesse grades found for course {course_id} in academic year {academic_year}'
                    }

                # Extract just the grade values for statistical analysis
                grade_values = [float(grade[1]) for grade in individual_grades]

                # Get unique grade file names for this course
                grade_file_names = list(set([grade[4] for grade in individual_grades if grade[4]]))
                grade_file_names_str = ', '.join(sorted(grade_file_names)) if grade_file_names else ""

                # Calculate basic statistics
                grade_count = len(grade_values)
                mean_grade = statistics.mean(grade_values)
                median_grade = statistics.median(grade_values)
                std_dev = statistics.stdev(grade_values) if grade_count > 1 else 0
                min_grade = min(grade_values)
                max_grade = max(grade_values)

                # Calculate quartiles
                sorted_grades = sorted(grade_values)
                q1 = statistics.median(sorted_grades[:len(sorted_grades)//2]) if grade_count > 2 else min_grade
                q3 = statistics.median(sorted_grades[(len(sorted_grades)+1)//2:]) if grade_count > 2 else max_grade

                # Create distribution bins for histogram
                # Use 10-point intervals: 0-10, 11-20, ..., 91-100
                distribution_bins = []
                for i in range(0, 100, 10):
                    bin_start = i
                    bin_end = i + 9 if i < 90 else 100
                    bin_count = len([g for g in grade_values if bin_start <= g <= bin_end])
                    distribution_bins.append({
                        'bin_start': bin_start,
                        'bin_end': bin_end,
                        'bin_label': f"{bin_start}-{bin_end}",
                        'count': bin_count,
                        'percentage': round((bin_count / grade_count) * 100, 1) if grade_count > 0 else 0
                    })

                # Format individual grades for return
                formatted_grades = []
                for grade_record in individual_grades:
                    formatted_grades.append({
                        'student_id': grade_record[0],
                        'grade': float(grade_record[1]),
                        'created_at': grade_record[2].isoformat() if grade_record[2] else None,
                        'course_name': grade_record[3],
                        'grade_file_name': grade_record[4]
                    })

                result = {
                    'course_id': course_id,
                    'course_name': individual_grades[0][3] if individual_grades else f"Course {course_id}",
                    'individual_grades': formatted_grades,
                    'distribution_data': distribution_bins,
                    'stats': {
                        'count': grade_count,
                        'mean': round(mean_grade, 2),
                        'median': round(median_grade, 2),
                        'std_dev': round(std_dev, 2),
                        'min': round(min_grade, 2),
                        'max': round(max_grade, 2),
                        'q1': round(q1, 2),
                        'q3': round(q3, 2),
                        'range': round(max_grade - min_grade, 2)
                    },
                    'filter_info': filter_config,
                    'academic_year': academic_year,
                    'filtering_method': 'course_based_only',
                    'grade_file_names': grade_file_names_str
                }

                logger.info(f"Successfully retrieved {grade_count} Benesse grades for course {course_id} in academic year {academic_year}")
                return result

        except Exception as e:
            logger.error(f"Error fetching grade distribution for course {course_id}: {str(e)}")
            return {
                'course_id': course_id,
                'individual_grades': [],
                'distribution_data': [],
                'stats': {},
                'error': str(e)
            }


class PastYearCourseActivity(models.Model):
    """Model to get course activity data from pre-2025 ClickHouse database"""

    class Meta:
        managed = False
        app_label = 'clickhouse_app'

    @classmethod
    def get_course_activity_summary(cls, academic_year: int, course_ids: List[int] = None) -> Dict[str, Any]:
        """
        Get activity summary for courses in a specific academic year.
        Uses the pre-2025 ClickHouse database for historical data.

        Args:
            academic_year (int): The academic year
            course_ids (List[int], optional): Specific course IDs to analyze

        Returns:
            Dict containing activity summary data
        """
        # Early exit if no course IDs provided
        if course_ids is not None and len(course_ids) == 0:
            logger.info(f"No course IDs provided for academic year {academic_year}, skipping ClickHouse queries")
            return {
                'academic_year': academic_year,
                'date_range': {
                    'start': f"{academic_year}-04-01",
                    'end': f"{academic_year + 1}-03-31"
                },
                'course_activities': [],
                'overall_stats': {
                    'total_courses_with_activity': 0,
                    'total_unique_students': 0,
                    'total_activities': 0,
                    'avg_activity_hour': 0
                },
                'daily_trends': [],
                'top_operations': []
            }

        logger.info(f"Fetching ClickHouse activity summary for academic year {academic_year} with {len(course_ids) if course_ids else 'all'} courses")

        try:
            # Calculate date range for academic year (April 1st to March 31st)
            start_date = f"{academic_year}-04-01"
            end_date = f"{academic_year + 1}-03-31"

            with connections['clickhouse_db_pre_2025'].cursor() as cursor:
                # Base query for activity data using updated old table schema
                base_query = """
                    SELECT
                        context_id,
                        COUNT(DISTINCT actor_account_name) as unique_students,
                        COUNT(DISTINCT _id) as total_activities,
                        COUNT(DISTINCT toDate(timestamp)) as active_days,
                        countIf(operation_name = 'OPEN') as content_opens,
                        countIf(operation_name = 'ADD_MARKER') as markers_added,
                        countIf(operation_name = 'ADD_MEMO') as memos_added,
                        countIf(operation_name = 'ADD_HW_MEMO') as handwriting_memos,
                        countIf(operation_name = 'ADD_BOOKMARK') as bookmarks_added
                    FROM statements_mv
                    WHERE timestamp >= toDate(%s)
                    AND timestamp <= toDate(%s)
                    AND context_id != ''
                    AND context_id IS NOT NULL
                """

                params = [start_date, end_date]

                # Add course filter if specified
                if course_ids:
                    course_ids_str = ','.join([f"'{cid}'" for cid in course_ids])
                    base_query += f" AND context_id IN ({course_ids_str})"

                base_query += " GROUP BY context_id ORDER BY total_activities DESC"

                logger.debug(f"Executing ClickHouse base query for date range {start_date} to {end_date}")
                cursor.execute(base_query, params)
                course_activities = cursor.fetchall()
                logger.info(f"Retrieved {len(course_activities)} course activity records")

                # Get overall statistics
                overall_query = """
                    SELECT
                        COUNT(DISTINCT context_id) as total_courses_with_activity,
                        COUNT(DISTINCT actor_account_name) as total_unique_students,
                        COUNT(DISTINCT _id) as total_activities,
                        AVG(toHour(timestamp)) as avg_activity_hour
                    FROM statements_mv
                    WHERE timestamp >= toDate(%s)
                    AND timestamp <= toDate(%s)
                    AND context_id != ''
                    AND context_id IS NOT NULL
                """

                if course_ids:
                    course_ids_str = ','.join([f"'{cid}'" for cid in course_ids])
                    overall_query += f" AND context_id IN ({course_ids_str})"

                cursor.execute(overall_query, params)
                overall_stats = cursor.fetchone()

                # Get daily activity trends
                daily_query = """
                    SELECT
                        toDate(timestamp) as date,
                        COUNT(DISTINCT context_id) as active_courses,
                        COUNT(DISTINCT actor_account_name) as active_students,
                        COUNT(DISTINCT _id) as total_activities
                    FROM statements_mv
                    WHERE timestamp >= toDate(%s)
                    AND timestamp <= toDate(%s)
                    AND context_id != ''
                    AND context_id IS NOT NULL
                """

                if course_ids:
                    course_ids_str = ','.join([f"'{cid}'" for cid in course_ids])
                    daily_query += f" AND context_id IN ({course_ids_str})"

                daily_query += " GROUP BY date ORDER BY date"

                cursor.execute(daily_query, params)
                daily_trends = cursor.fetchall()

                # Get top operation types
                operations_query = """
                    SELECT
                        operation_name,
                        COUNT(DISTINCT _id) as activity_count,
                        COUNT(DISTINCT context_id) as course_count,
                        COUNT(DISTINCT actor_account_name) as student_count
                    FROM statements_mv
                    WHERE timestamp >= toDate(%s)
                    AND timestamp <= toDate(%s)
                    AND context_id != ''
                    AND context_id IS NOT NULL
                    AND operation_name != ''
                    AND operation_name IS NOT NULL
                """

                if course_ids:
                    course_ids_str = ','.join([f"'{cid}'" for cid in course_ids])
                    operations_query += f" AND context_id IN ({course_ids_str})"

                operations_query += " GROUP BY operation_name ORDER BY activity_count DESC LIMIT 10"

                cursor.execute(operations_query, params)
                top_operations = cursor.fetchall()

                result = {
                    'academic_year': academic_year,
                    'date_range': {
                        'start': start_date,
                        'end': end_date
                    },
                    'course_activities': [
                        {
                            'course_id': row[0],
                            'unique_students': row[1],
                            'total_activities': row[2],
                            'active_days': row[3],
                            'content_opens': row[4],
                            'markers_added': row[5],
                            'memos_added': row[6],
                            'handwriting_memos': row[7],
                            'bookmarks_added': row[8]
                        }
                        for row in course_activities
                    ],
                    'overall_stats': {
                        'total_courses_with_activity': overall_stats[0] if overall_stats else 0,
                        'total_unique_students': overall_stats[1] if overall_stats else 0,
                        'total_activities': overall_stats[2] if overall_stats else 0,
                        'avg_activity_hour': round(overall_stats[3], 2) if overall_stats and overall_stats[3] else 0
                    },
                    'daily_trends': [
                        {
                            'date': row[0].isoformat(),
                            'active_courses': row[1],
                            'active_students': row[2],
                            'total_activities': row[3]
                        }
                        for row in daily_trends
                    ],
                    'top_operations': [
                        {
                            'operation': row[0],
                            'activity_count': row[1],
                            'course_count': row[2],
                            'student_count': row[3]
                        }
                        for row in top_operations
                    ]
                }

                logger.info(f"ClickHouse activity summary completed: {len(result['course_activities'])} courses, {result['overall_stats']['total_activities']} total activities")

                return result

        except Exception as e:
            logger.error(f"Error fetching course activity for academic year {academic_year}: {str(e)}")
            return {
                'academic_year': academic_year,
                'course_activities': [],
                'overall_stats': {},
                'daily_trends': [],
                'top_operations': [],
                'error': str(e)
            }

    @classmethod
    def get_course_engagement_patterns(cls, academic_year: int) -> Dict[str, Any]:
        """
        Get engagement patterns for courses in a specific academic year.
        Analyzes when students are most active (by hour, day of week).
        """
        logger.info(f"Fetching ClickHouse engagement patterns for academic year {academic_year}")

        try:
            # Calculate date range for academic year
            start_date = f"{academic_year}-04-01"
            end_date = f"{academic_year + 1}-03-31"

            with connections['clickhouse_db_pre_2025'].cursor() as cursor:
                # Get activity by hour of day (JST)
                hourly_query = """
                    SELECT
                        toHour(timestamp + INTERVAL 9 HOUR) as jst_hour,
                        COUNT(DISTINCT _id) as activity_count,
                        COUNT(DISTINCT actor_account_name) as student_count
                    FROM statements_mv
                    WHERE timestamp >= toDate(%s)
                    AND timestamp <= toDate(%s)
                    AND context_id != ''
                    AND context_id IS NOT NULL
                    GROUP BY jst_hour
                    ORDER BY jst_hour
                """

                cursor.execute(hourly_query, [start_date, end_date])
                hourly_patterns = cursor.fetchall()

                # Get activity by day of week (JST)
                daily_query = """
                    SELECT
                        toDayOfWeek(timestamp + INTERVAL 9 HOUR) as jst_day_of_week,
                        COUNT(DISTINCT _id) as activity_count,
                        COUNT(DISTINCT actor_account_name) as student_count
                    FROM statements_mv
                    WHERE timestamp >= toDate(%s)
                    AND timestamp <= toDate(%s)
                    AND context_id != ''
                    AND context_id IS NOT NULL
                    GROUP BY jst_day_of_week
                    ORDER BY jst_day_of_week
                """

                cursor.execute(daily_query, [start_date, end_date])
                daily_patterns = cursor.fetchall()

                # Get monthly trends
                monthly_query = """
                    SELECT
                        toYYYYMM(timestamp) as month,
                        COUNT(DISTINCT context_id) as active_courses,
                        COUNT(DISTINCT actor_account_name) as active_students,
                        COUNT(DISTINCT _id) as total_activities
                    FROM statements_mv
                    WHERE timestamp >= toDate(%s)
                    AND timestamp <= toDate(%s)
                    AND context_id != ''
                    AND context_id IS NOT NULL
                    GROUP BY month
                    ORDER BY month
                """

                cursor.execute(monthly_query, [start_date, end_date])
                monthly_trends = cursor.fetchall()

                result = {
                    'academic_year': academic_year,
                    'hourly_patterns': [
                        {
                            'hour': row[0],
                            'activity_count': row[1],
                            'student_count': row[2]
                        }
                        for row in hourly_patterns
                    ],
                    'daily_patterns': [
                        {
                            'day_of_week': row[0],  # 1=Monday, 7=Sunday
                            'activity_count': row[1],
                            'student_count': row[2]
                        }
                        for row in daily_patterns
                    ],
                    'monthly_trends': [
                        {
                            'month': str(row[0]),
                            'active_courses': row[1],
                            'active_students': row[2],
                            'total_activities': row[3]
                        }
                        for row in monthly_trends
                    ]
                }

                logger.info(f"ClickHouse engagement patterns completed: {len(result['hourly_patterns'])} hourly, {len(result['monthly_trends'])} monthly records")
                return result

        except Exception as e:
            logger.error(f"Error fetching engagement patterns for academic year {academic_year}: {str(e)}")
            return {
                'academic_year': academic_year,
                'hourly_patterns': [],
                'daily_patterns': [],
                'monthly_trends': [],
                'error': str(e)
            }


class PastYearStudentGrades(models.Model):
    """
    Model to access student grades from analysis_db

    IMPORTANT: GRADE CATEGORIZATION METHOD
    =====================================

    ✅ NEW APPROACH (Course name-based year matching):
    - Courses are categorized by academic year based on course name patterns (e.g., "2022年度1年B組英語")
    - Extracts academic year from course names using "{year}年度" pattern matching
    - Students are filtered by academic year using PastYearCourseCategory.get_student_user_ids_for_academic_year()
    - Ensures proper course-year alignment for transparency and accuracy
    - NO date filtering on created_at for academic year categorization

    ❌ OLD APPROACH (All courses approach):
    - Previously used ALL courses that had grade data regardless of academic year
    - Led to wrong course transparency (2022 courses showing up in 2025 data)
    - Students from one year could see courses from any other year if they had grades

    ❌ ORIGINAL APPROACH (Date-based categorization):
    - Initially filtered grades by created_at date range (April 1 - March 31)
    - Incorrect because grades are manually uploaded, so created_at represents upload date, not academic year

    This change affects:
    - _get_yearly_performance_data()
    - _fetch_grade_performance_summary_stats()
    - Course transparency data in templates
    """
    id = models.IntegerField(primary_key=True)
    course_student_id = models.CharField(max_length=255)
    quiz = models.FloatField()  # Grade out of 100
    response = models.TextField(blank=True, null=True)
    user_id = models.CharField(max_length=255)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()
    sort = models.IntegerField(blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    student_id = models.CharField(max_length=255)
    moodle_url = models.URLField(blank=True, null=True)
    course_id = models.CharField(max_length=255)
    min = models.FloatField(blank=True, null=True)
    max = models.FloatField(blank=True, null=True)
    scaled = models.FloatField(blank=True, null=True)
    course_name = models.CharField(max_length=255, blank=True, null=True)
    date_at = models.DateTimeField(blank=True, null=True)
    consumer_key = models.CharField(max_length=255, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'course_student_scores'
        app_label = 'analysis_app'

    @classmethod
    def get_student_analytics_for_year(cls, academic_year: int, course_ids: List[str] = None) -> Dict[str, Any]:
        """
        Get comprehensive student analytics for a specific academic year.
        Combines grade data with course access patterns.

        Grade data: Uses course-based categorization (grades are categorized by course_id, not upload date)
        Activity data: Uses date-based filtering (activities are time-based)

        Args:
            academic_year (int): The academic year to analyze
            course_ids (List[str], optional): List of course IDs to filter analysis to specific courses
        """
        logger.info(f"Fetching student analytics for academic year {academic_year} with {len(course_ids) if course_ids else 'all'} courses")
        logger.debug(f"STUDENT ANALYTICS: Input course_ids: {course_ids[:10] if course_ids else 'None'}...")

        try:
            # Calculate date range for academic year (April to March)
            # NOTE: This is ONLY used for ClickHouse activity data, NOT for grade categorization
            start_date = f"{academic_year}-04-01"  # April 1 of academic year
            end_date = f"{academic_year + 1}-03-31"  # March 31 of following year
            logger.debug(f"STUDENT ANALYTICS: Date range for activity data: {start_date} to {end_date}")

            # Get grade analytics from analysis_db using COURSE-BASED categorization
            # (date parameters are ignored in grade analytics now)
            logger.debug(f"STUDENT ANALYTICS: Starting grade analytics (course-based categorization)...")
            grade_analytics = cls._get_grade_analytics(academic_year, start_date, end_date, course_ids)
            logger.debug(f"STUDENT ANALYTICS: Grade analytics completed (categorized by course_id)")

            # Get course access analytics from clickhouse_db_pre_2025 using DATE-BASED filtering
            logger.debug(f"STUDENT ANALYTICS: Starting access analytics (date-based filtering)...")
            access_analytics = cls._get_course_access_analytics(academic_year, start_date, end_date, course_ids)
            logger.debug(f"STUDENT ANALYTICS: Access analytics completed (filtered by date range)")

            # Combine and analyze the data
            logger.debug(f"STUDENT ANALYTICS: Starting combined analytics...")
            combined_analytics = cls._combine_grade_and_access_data(grade_analytics, access_analytics, academic_year)
            logger.debug(f"STUDENT ANALYTICS: Combined analytics completed")

            # Calculate summary stats
            logger.debug(f"STUDENT ANALYTICS: Starting summary stats calculation...")
            summary_stats = cls._calculate_summary_stats(grade_analytics, access_analytics)
            logger.debug(f"STUDENT ANALYTICS: Summary stats completed")

            result = {
                'academic_year': academic_year,
                'date_range': {
                    'start': start_date,
                    'end': end_date,
                    'note': 'Date range used for activity data only. Grades categorized by course_id.'
                },
                'grade_analytics': grade_analytics,
                'access_analytics': access_analytics,
                'combined_analytics': combined_analytics,
                'summary_stats': summary_stats,
                'categorization_methods': {
                    'grades': 'course_based',
                    'activities': 'date_based'
                }
            }

            logger.info(f"Student analytics completed for year {academic_year}")
            logger.debug(f"STUDENT ANALYTICS: Final summary - Grade courses: {summary_stats.get('total_courses_with_grades', 0)}, Activity courses: {access_analytics.get('overall_stats', {}).get('total_courses_with_activity', 0)}, Total activities: {summary_stats.get('total_activities', 0)}")
            return result

        except Exception as e:
            logger.error(f"Error fetching student analytics for academic year {academic_year}: {str(e)}")
            return {
                'academic_year': academic_year,
                'grade_analytics': {},
                'access_analytics': {},
                'combined_analytics': {},
                'summary_stats': {},
                'categorization_methods': {
                    'grades': 'course_based',
                    'activities': 'date_based'
                },
                'error': str(e)
            }

    @classmethod
    def _get_grade_analytics(cls, academic_year: int, start_date: str, end_date: str, course_ids: List[str] = None) -> Dict[str, Any]:
        """Get grade analytics from analysis_db (MySQL) using course-based categorization only"""
        try:
            # Get optimal student filtering approach (IN vs NOT IN)
            filter_config = PastYearCourseCategory.get_optimal_student_filter_for_academic_year(academic_year)
            filter_type = filter_config['filter_type']
            filter_ids = filter_config['filter_ids']
            filter_count = filter_config['filter_count']

            if not filter_ids:
                logger.warning(f"No filter IDs found for academic year {academic_year}")
                return {
                    'overall_stats': {},
                    'grade_distribution': [],
                    'course_stats': [],
                    'monthly_trends': [],
                    'categorization_method': 'course_based'
                }

            # Get course IDs for this academic year if not provided
            if course_ids is None:
                courses_data = PastYearCourseCategory.get_courses_by_academic_year(academic_year)
                course_ids = []

                if courses_data and courses_data.get('categories'):
                    for category in courses_data.get('categories', {}).values():
                        for child_category in category.get('children', {}).values():
                            course_ids.extend([str(course['id']) for course in child_category.get('courses', [])])

                logger.debug(f"GRADE ANALYTICS: Auto-detected {len(course_ids)} courses for academic year {academic_year}")
            else:
                logger.debug(f"GRADE ANALYTICS: Using provided {len(course_ids)} course IDs")

            if not course_ids:
                logger.warning(f"No courses found for academic year {academic_year}")
                return {
                    'overall_stats': {},
                    'grade_distribution': [],
                    'course_stats': [],
                    'monthly_trends': [],
                    'categorization_method': 'course_based'
                }

            logger.debug(f"GRADE ANALYTICS: Using {filter_type} filtering with {filter_count} student IDs and {len(course_ids)} course IDs")
            logger.debug(f"GRADE ANALYTICS: {filter_config['efficiency_reason']}")
            logger.debug(f"GRADE ANALYTICS: ✅ USING COURSE-BASED CATEGORIZATION - NO DATE FILTERING")

            with connections['analysis_db'].cursor() as cursor:
                # Build course filter clause - ALWAYS APPLIED
                course_filter_placeholders = ",".join(["%s"] * len(course_ids))
                course_filter = f" AND course_id IN ({course_filter_placeholders})"
                course_params = course_ids
                logger.debug(f"GRADE ANALYTICS: Filtering by {len(course_ids)} course IDs from academic year {academic_year}")

                # Build student filter clause based on optimal approach
                filter_placeholders = ",".join(["%s"] * len(filter_ids))
                if filter_type == 'NOT_IN':
                    student_filter = f" AND student_id NOT IN ({filter_placeholders}) AND student_id IS NOT NULL"
                    logger.debug(f"GRADE ANALYTICS: Using NOT IN filter to exclude {filter_count} non-students")
                else:
                    student_filter = f" AND student_id IN ({filter_placeholders})"
                    logger.debug(f"GRADE ANALYTICS: Using IN filter to include {filter_count} students")

                filter_params = filter_ids

                # Overall grade statistics - ONLY course and student filtering (NO DATE FILTERING)
                overall_stats_query = f"""
                    SELECT
                        COUNT(DISTINCT student_id) as total_students,
                        COUNT(DISTINCT course_id) as total_courses,
                        COUNT(*) as total_grades,
                        AVG(quiz) as avg_grade,
                        MIN(quiz) as min_grade,
                        MAX(quiz) as max_grade
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%') {student_filter}{course_filter}
                    {PastYearGradeAnalytics._get_valid_grade_filter_clause()}
                """
                logger.debug(f"GRADE ANALYTICS: Overall stats query with ONLY course and student filtering (no date filtering)")
                cursor.execute(overall_stats_query, filter_params + course_params)
                overall_stats = cursor.fetchone()
                logger.debug(f"GRADE ANALYTICS: Overall stats result: {overall_stats}")

                # Check what courses actually have grades (with student and course filtering only)
                courses_with_grades_query = f"""
                    SELECT DISTINCT course_id, course_name, COUNT(*) as grade_count
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%') {student_filter}{course_filter}
                    {PastYearGradeAnalytics._get_valid_grade_filter_clause()}
                    GROUP BY course_id, course_name
                    ORDER BY grade_count DESC
                """
                logger.debug(f"GRADE ANALYTICS: Courses with grades query (course-based categorization)")
                cursor.execute(courses_with_grades_query, filter_params + course_params)
                courses_with_grades = cursor.fetchall()
                logger.debug(f"GRADE ANALYTICS: Found {len(courses_with_grades)} courses with grades (course-based categorization)")
                for i, course in enumerate(courses_with_grades[:5]):  # Log first 5
                    logger.debug(f"GRADE ANALYTICS: Course {i+1}: ID={course[0]}, Name={course[1]}, Grades={course[2]}")

                # Simplified median calculation - just use average as approximation for now
                # MySQL median calculation is complex and not critical for analytics
                median_grade = overall_stats[3] if overall_stats and overall_stats[3] else 0

                # Grade distribution by ranges - ONLY course and student filtering
                grade_distribution_query = f"""
                    SELECT
                        CASE
                            WHEN quiz >= 90 THEN 'A (90-100)'
                            WHEN quiz >= 80 THEN 'B (80-89)'
                            WHEN quiz >= 70 THEN 'C (70-79)'
                            WHEN quiz >= 60 THEN 'D (60-69)'
                            ELSE 'F (0-59)'
                        END as grade_range,
                        COUNT(*) as count,
                        COUNT(DISTINCT student_id) as unique_students
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%') {student_filter}{course_filter}
                    GROUP BY grade_range
                    ORDER BY grade_range
                """
                logger.debug(f"GRADE ANALYTICS: Grade distribution query (course-based categorization)")
                cursor.execute(grade_distribution_query, filter_params + course_params)
                grade_distribution = cursor.fetchall()
                logger.debug(f"GRADE ANALYTICS: Grade distribution result: {grade_distribution}")

                # Course-level grade statistics - ONLY course and student filtering
                course_stats_query = f"""
                    SELECT
                        course_id,
                        course_name,
                        COUNT(DISTINCT student_id) as student_count,
                        COUNT(*) as grade_count,
                        AVG(quiz) as avg_grade,
                        MIN(quiz) as min_grade,
                        MAX(quiz) as max_grade,
                        GROUP_CONCAT(DISTINCT name ORDER BY name SEPARATOR ', ') as grade_file_names
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%') {student_filter}{course_filter}
                    GROUP BY course_id, course_name
                    ORDER BY student_count DESC
                """
                logger.debug(f"GRADE ANALYTICS: Course stats query (course-based categorization)")
                cursor.execute(course_stats_query, filter_params + course_params)
                course_stats = cursor.fetchall()
                logger.debug(f"GRADE ANALYTICS: Found {len(course_stats)} courses with detailed stats (course-based categorization)")

                # Monthly grade trends based on created_at (for reference only, not for academic year categorization)
                # This shows when grades were uploaded, not when they belong to academic years
                monthly_trends_query = f"""
                    SELECT
                        DATE_FORMAT(created_at, '%%Y%%m') as month,
                        COUNT(DISTINCT student_id) as active_students,
                        COUNT(*) as total_grades,
                        AVG(quiz) as avg_grade
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%') {student_filter}{course_filter}
                    GROUP BY month
                    ORDER BY month
                """
                logger.debug(f"GRADE ANALYTICS: Monthly trends query (upload dates for reference only)")
                cursor.execute(monthly_trends_query, filter_params + course_params)
                monthly_trends = cursor.fetchall()
                logger.debug(f"GRADE ANALYTICS: Monthly trends result: {len(monthly_trends)} months (showing upload dates, not academic year categorization)")

                # Simplified course stats without complex median calculation
                course_stats_with_median = []
                for course_stat in course_stats:
                    course_id = course_stat[0]
                    course_name = course_stat[1]
                    student_count = course_stat[2]
                    grade_count = course_stat[3]
                    avg_grade = course_stat[4]
                    min_grade = course_stat[5]
                    max_grade = course_stat[6]
                    grade_file_names = course_stat[7] if len(course_stat) > 7 else ""

                    # Skip courses with no grades (safety check)
                    if not avg_grade or grade_count == 0:
                        logger.debug(f"GRADE ANALYTICS: Skipping course {course_id} - no valid grades")
                        continue

                    # Calculate proper median for this course
                    median_query = f"""
                        SELECT quiz FROM course_student_scores
                        WHERE course_id = %s
                        AND quiz IS NOT NULL
                        AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%') {student_filter}
                        ORDER BY quiz
                    """
                    cursor.execute(median_query, [course_id] + filter_params)
                    course_grades = [float(row[0]) for row in cursor.fetchall()]

                    # Calculate median using statistics module for accuracy
                    if course_grades:
                        import statistics
                        median_grade = statistics.median(course_grades)
                    else:
                        median_grade = avg_grade  # Fallback to average if no grades found

                    course_stats_with_median.append({
                        'course_id': course_id,
                        'course_name': course_name,
                        'student_count': student_count,
                        'grade_count': grade_count,
                        'avg_grade': round(float(avg_grade), 2),
                        'min_grade': float(min_grade),
                        'max_grade': float(max_grade),
                        'median_grade': round(float(median_grade), 2),
                        'grade_file_names': grade_file_names or "",  # Add grade file names
                    })

                result = {
                    'overall_stats': {
                        'total_students': overall_stats[0] if overall_stats else 0,
                        'total_courses': overall_stats[1] if overall_stats else 0,
                        'total_grades': overall_stats[2] if overall_stats else 0,
                        'avg_grade': round(float(overall_stats[3]), 2) if overall_stats and overall_stats[3] else 0,
                        'min_grade': float(overall_stats[4]) if overall_stats else 0,
                        'max_grade': float(overall_stats[5]) if overall_stats else 0,
                        'median_grade': round(float(median_grade), 2) if median_grade else 0,
                        'q1_grade': 0,  # Quartiles are complex in MySQL, setting to 0 for now
                        'q3_grade': 0,  # Quartiles are complex in MySQL, setting to 0 for now
                    },
                    'grade_distribution': [
                        {
                            'grade_range': row[0],
                            'count': row[1],
                            'unique_students': row[2]
                        }
                        for row in grade_distribution
                    ],
                    'course_stats': course_stats_with_median,
                    'monthly_trends': [
                        {
                            'month': str(row[0]),
                            'active_students': row[1],
                            'total_grades': row[2],
                            'avg_grade': round(float(row[3]), 2) if row[3] else 0
                        }
                        for row in monthly_trends
                    ],
                    'filter_info': filter_config,  # Include filter info for debugging
                    'academic_year_courses': len(course_ids),  # Add course count for debugging
                    'categorization_method': 'course_based'  # Indicate that we use course-based categorization
                }

                logger.debug(f"GRADE ANALYTICS: Final result summary (COURSE-BASED categorization) - Students: {result['overall_stats']['total_students']}, Courses: {result['overall_stats']['total_courses']}, Grades: {result['overall_stats']['total_grades']}")
                return result

        except Exception as e:
            logger.error(f"Error fetching grade analytics: {str(e)}")
            return {
                'categorization_method': 'course_based',
                'error': str(e)
            }

    @classmethod
    def _get_course_access_analytics(cls, academic_year: int, start_date: str, end_date: str, course_ids: List[str] = None) -> Dict[str, Any]:
        """Get course access analytics from clickhouse_db_pre_2025 with dynamic top activity types"""
        try:
            # Get list of actual student user IDs for this academic year
            student_user_ids = PastYearCourseCategory.get_student_user_ids_for_academic_year(academic_year)
            if not student_user_ids:
                logger.warning(f"No student user IDs found for academic year {academic_year}")
                return {
                    'overall_stats': {},
                    'student_access': [],
                    'course_access': [],
                    'activity_types': [],
                    'top_activity_types': [],
                    'student_id_mapping_debug': {}
                }

            # Convert to set for faster lookup when filtering
            student_user_ids_set = set(student_user_ids)
            logger.debug(f"ACCESS ANALYTICS: Filtering by {len(student_user_ids)} student user IDs")

            with connections['clickhouse_db_pre_2025'].cursor() as cursor:
                # Build course filter for ClickHouse queries
                course_filter = ""
                if course_ids:
                    course_ids_str = ','.join([f"'{cid}'" for cid in course_ids])
                    course_filter = f" AND context_id IN ({course_ids_str})"
                    logger.debug(f"ACCESS ANALYTICS: Filtering ClickHouse queries by {len(course_ids)} course IDs")
                    logger.debug(f"ACCESS ANALYTICS: Course IDs filter: {course_ids[:10]}...")
                else:
                    logger.debug("ACCESS ANALYTICS: No course filtering applied to ClickHouse queries")

                # STEP 1: Get ALL activity types from the database (not just top 10)
                # This ensures the chart considers all activities, while still showing top 10 as UI controls
                top_activity_types_query = f"""
                    SELECT
                        operation_name,
                        COUNT(DISTINCT _id) as activity_count,
                        COUNT(DISTINCT actor_account_name) as account_count,
                        COUNT(DISTINCT context_id) as course_count
                    FROM statements_mv
                    WHERE timestamp >= toDate(%s)
                    AND timestamp <= toDate(%s)
                    AND context_id != ''
                    AND context_id IS NOT NULL
                    AND operation_name != ''
                    AND operation_name IS NOT NULL
                    AND actor_account_name != ''
                    AND actor_account_name IS NOT NULL{course_filter}
                    GROUP BY operation_name
                    ORDER BY activity_count DESC
                """
                logger.debug(f"ACCESS ANALYTICS: Getting ALL activity types: {top_activity_types_query}")
                cursor.execute(top_activity_types_query, [start_date, end_date])
                all_activity_types_raw = cursor.fetchall()

                # Build dynamic ALL activity types list for correlation data
                all_activity_types = []
                dynamic_activity_fields = []

                for i, row in enumerate(all_activity_types_raw):
                    operation_name = row[0]
                    activity_count = row[1]

                    # Create dynamic field name (use operation_name as-is per user request)
                    field_name = operation_name.lower().replace(' ', '_').replace('-', '_')

                    all_activity_types.append({
                        'key': field_name,
                        'name': operation_name,  # Use operation_name as-is
                        'description': f'Activity type: {operation_name}',
                        'total_count': activity_count,
                        'operation_name': operation_name  # Keep original for SQL queries
                    })

                    dynamic_activity_fields.append({
                        'field_name': field_name,
                        'operation_name': operation_name
                    })

                logger.info(f"ACCESS ANALYTICS: Found {len(all_activity_types)} total activity types: {[at['name'] for at in all_activity_types[:10]]}...")

                # Calculate top 10 activity types for UI controls (from all activity types)
                top_activity_types = all_activity_types[:10]  # Take top 10 for UI controls
                logger.info(f"ACCESS ANALYTICS: Top 10 activity types for UI controls: {[at['name'] for at in top_activity_types]}")

                # If no activity types found, return empty result
                if not all_activity_types:
                    logger.warning(f"ACCESS ANALYTICS: No activity types found for academic year {academic_year}")
                    return {
                        'overall_stats': {},
                        'student_access': [],
                        'course_access': [],
                        'activity_types': [],
                        'top_activity_types': [],
                        'student_id_mapping_debug': {}
                    }

                # STEP 2: Build dynamic SQL query with ALL activity types (not just top 10)
                dynamic_activity_selects = []
                for field in dynamic_activity_fields:
                    field_name = field['field_name']
                    operation_name = field['operation_name']
                    dynamic_activity_selects.append(
                        f"COUNT(DISTINCT CASE WHEN operation_name = '{operation_name}' THEN _id END) as {field_name}"
                    )

                dynamic_activity_sql = ',\n                        '.join(dynamic_activity_selects)

                # STEP 3: Get all raw activity data with dynamic activity types
                raw_activity_query = f"""
                    SELECT
                        actor_account_name,
                        context_id as course_id,
                        COUNT(DISTINCT _id) as total_activities,
                        COUNT(DISTINCT toDate(timestamp)) as active_days,
                        MIN(timestamp) as first_access,
                        MAX(timestamp) as last_access,
                        {dynamic_activity_sql}
                    FROM statements_mv
                    WHERE timestamp >= toDate(%s)
                    AND timestamp <= toDate(%s)
                    AND context_id != ''
                    AND context_id IS NOT NULL
                    AND actor_account_name != ''
                    AND actor_account_name IS NOT NULL{course_filter}
                    GROUP BY actor_account_name, context_id
                    ORDER BY total_activities DESC
                """
                logger.debug(f"ACCESS ANALYTICS: Dynamic raw activity query: {raw_activity_query}")
                cursor.execute(raw_activity_query, [start_date, end_date])
                raw_activity_data = cursor.fetchall()
                logger.debug(f"ACCESS ANALYTICS: Retrieved {len(raw_activity_data)} raw student-course activity records with dynamic types")

                # STEP 4: Filter activity data by actual student IDs and build with dynamic fields
                filtered_student_access = []
                student_id_mapping = {}  # For debugging and validation
                filtered_actor_accounts = set()  # Track which actor accounts correspond to students

                for row in raw_activity_data:
                    actor_account_name = row[0]
                    student_id = extract_student_id_from_actor_account_name(actor_account_name)

                    # Only include if this is an actual student for this academic year
                    if student_id and student_id in student_user_ids_set:
                        # Build dynamic student record
                        student_record = {
                            'student_id': student_id,
                            'actor_account_name': actor_account_name,  # Keep original for debugging
                            'course_id': row[1],
                            'total_activities': row[2],
                            'active_days': row[3],
                            'first_access': row[4].isoformat() if row[4] else None,
                            'last_access': row[5].isoformat() if row[5] else None,
                        }

                        # Add dynamic activity type fields
                        for i, field in enumerate(dynamic_activity_fields):
                            field_name = field['field_name']
                            # Activity counts start at index 6 (after the fixed fields)
                            student_record[field_name] = row[6 + i]

                        filtered_student_access.append(student_record)

                        # Track mapping for debugging
                        if student_id not in student_id_mapping:
                            student_id_mapping[student_id] = set()
                        student_id_mapping[student_id].add(actor_account_name)
                        filtered_actor_accounts.add(actor_account_name)

                logger.debug(f"ACCESS ANALYTICS: Filtered to {len(filtered_student_access)} student-course activity records with dynamic types")
                logger.debug(f"ACCESS ANALYTICS: Found {len(student_id_mapping)} unique student IDs with activity")

                # STEP 5: Get overall statistics using only filtered actor accounts
                if filtered_actor_accounts:
                    # Convert filtered actor accounts to quoted string for SQL
                    actor_accounts_str = ','.join([f"'{acc}'" for acc in filtered_actor_accounts])

                    overall_stats_query = f"""
                        SELECT
                            COUNT(DISTINCT context_id) as total_courses_with_activity,
                            COUNT(DISTINCT actor_account_name) as total_unique_accounts,
                            COUNT(DISTINCT _id) as total_activities,
                            AVG(toHour(timestamp)) as avg_activity_hour
                        FROM statements_mv
                        WHERE timestamp >= toDate(%s)
                        AND timestamp <= toDate(%s)
                        AND context_id != ''
                        AND context_id IS NOT NULL
                        AND actor_account_name IN ({actor_accounts_str}){course_filter}
                    """
                    logger.debug(f"ACCESS ANALYTICS: Overall stats query (student-filtered): {overall_stats_query}")
                    cursor.execute(overall_stats_query, [start_date, end_date])
                    overall_stats_raw = cursor.fetchone()
                    logger.debug(f"ACCESS ANALYTICS: Overall stats result (student-filtered): {overall_stats_raw}")
                else:
                    # No students found with activity
                    overall_stats_raw = (0, 0, 0, 0)
                    logger.warning(f"ACCESS ANALYTICS: No student activity found for academic year {academic_year}")

                # STEP 6: Course access summary with filtered student data
                course_access = []
                course_activity_summary = {}

                for student_record in filtered_student_access:
                    course_id = student_record['course_id']
                    if course_id not in course_activity_summary:
                        course_activity_summary[course_id] = {
                            'unique_students': set(),
                            'unique_actor_accounts': set(),
                            'total_activities': 0
                        }

                    course_activity_summary[course_id]['unique_students'].add(student_record['student_id'])
                    course_activity_summary[course_id]['unique_actor_accounts'].add(student_record['actor_account_name'])
                    course_activity_summary[course_id]['total_activities'] += student_record['total_activities']

                for course_id, summary in course_activity_summary.items():
                    unique_students_count = len(summary['unique_students'])
                    course_access.append({
                        'course_id': course_id,
                        'unique_students': unique_students_count,
                        'unique_actor_accounts': len(summary['unique_actor_accounts']),  # Original count for comparison
                        'total_activities': summary['total_activities'],
                        'avg_activities_per_student': round(summary['total_activities'] / unique_students_count, 2) if unique_students_count > 0 else 0,
                        'avg_activities_per_account': round(summary['total_activities'] / len(summary['unique_actor_accounts']), 2) if len(summary['unique_actor_accounts']) > 0 else 0,
                    })

                logger.debug(f"ACCESS ANALYTICS: Generated {len(course_access)} course access summaries (student-filtered)")

                # STEP 7: Activity type distribution (with student filtering) - using extended list
                if filtered_actor_accounts:
                    activity_types_query = f"""
                        SELECT
                            operation_name,
                            COUNT(DISTINCT _id) as activity_count,
                            COUNT(DISTINCT actor_account_name) as account_count,
                            COUNT(DISTINCT context_id) as course_count
                        FROM statements_mv
                        WHERE timestamp >= toDate(%s)
                        AND timestamp <= toDate(%s)
                        AND context_id != ''
                        AND context_id IS NOT NULL
                        AND operation_name != ''
                        AND operation_name IS NOT NULL
                        AND actor_account_name IN ({actor_accounts_str}){course_filter}
                        GROUP BY operation_name
                        ORDER BY activity_count DESC
                        LIMIT 10
                    """
                    logger.debug(f"ACCESS ANALYTICS: Activity types query (student-filtered): {activity_types_query}")
                    cursor.execute(activity_types_query, [start_date, end_date])
                    activity_types = cursor.fetchall()
                    logger.debug(f"ACCESS ANALYTICS: Found {len(activity_types)} activity types (student-filtered)")
                else:
                    activity_types = []

                # Count unique students from the filtered data
                unique_students_total = len(student_id_mapping)

                result = {
                    'overall_stats': {
                        'total_courses_with_activity': overall_stats_raw[0] if overall_stats_raw else 0,
                        'total_unique_accounts': overall_stats_raw[1] if overall_stats_raw else 0,
                        'total_unique_students': unique_students_total,
                        'total_activities': overall_stats_raw[2] if overall_stats_raw else 0,
                        'avg_activity_hour': round(overall_stats_raw[3], 2) if overall_stats_raw and overall_stats_raw[3] else 0
                    },
                    'student_access': filtered_student_access,
                    'course_access': course_access,
                    'activity_types': [
                        {
                            'operation_name': row[0],
                            'activity_count': row[1],
                            'account_count': row[2],
                            'course_count': row[3]
                        }
                        for row in activity_types
                    ],
                    'top_activity_types': top_activity_types,  # Add the dynamic top activity types
                    'student_id_mapping_debug': {
                        student_id: list(accounts) for student_id, accounts in student_id_mapping.items()
                    }
                }

                logger.debug(f"ACCESS ANALYTICS: Final result summary (student-filtered, dynamic) - Courses with activity: {result['overall_stats']['total_courses_with_activity']}, Students: {result['overall_stats']['total_unique_students']}, Activities: {result['overall_stats']['total_activities']}")
                logger.info(f"ACCESS ANALYTICS: Dynamic top activity types: {[at['name'] for at in top_activity_types]}")
                return result

        except Exception as e:
            logger.error(f"Error fetching course access analytics: {str(e)}")
            logger.error(f"Exception details:", exc_info=True)
            return {}

    @classmethod
    def _combine_grade_and_access_data(cls, grade_analytics: Dict, access_analytics: Dict, academic_year: int = None) -> Dict[str, Any]:
        """Combine grade and access data to find meaningful patterns"""
        try:
            # Create student-course activity lookup
            student_course_activities = {}
            for access in access_analytics.get('student_access', []):
                key = f"{access['student_id']}_{access['course_id']}"
                student_course_activities[key] = access

            logger.info(f"COMBINE DATA: Built {len(student_course_activities)} student-course activity records")
            if student_course_activities:
                # Log some sample keys
                sample_keys = list(student_course_activities.keys())[:5]
                logger.info(f"COMBINE DATA: Sample activity keys: {sample_keys}")

                # Log student ID and course ID details for debugging
                first_access = list(access_analytics.get('student_access', []))[0] if access_analytics.get('student_access', []) else None
                if first_access:
                    logger.info(f"COMBINE DATA: First activity record - Student ID: '{first_access['student_id']}' (type: {type(first_access['student_id'])}), Course ID: '{first_access['course_id']}' (type: {type(first_access['course_id'])})")

            # Create student-course grade lookup from analysis_db with proper student filtering
            student_course_grades = {}

            try:
                # Get the optimal student filtering approach - same as used in grade analytics
                if academic_year:
                    filter_config = PastYearCourseCategory.get_optimal_student_filter_for_academic_year(academic_year)
                    filter_type = filter_config['filter_type']
                    filter_ids = filter_config['filter_ids']
                    filter_count = filter_config['filter_count']

                    logger.info(f"COMBINE DATA: Using {filter_type} filtering with {filter_count} IDs for individual grades")

                    with connections['analysis_db'].cursor() as cursor:
                        # Build student filter clause based on optimal approach - SAME AS GRADE ANALYTICS
                        if filter_ids:
                            filter_placeholders = ",".join(["%s"] * len(filter_ids))
                            if filter_type == 'NOT_IN':
                                student_filter = f" AND student_id NOT IN ({filter_placeholders}) AND student_id IS NOT NULL"
                            else:
                                student_filter = f" AND student_id IN ({filter_placeholders})"

                            student_grades_query = f"""
                                SELECT
                                    student_id,
                                    course_id,
                                    course_name,
                                    AVG(quiz) as avg_grade,
                                    COUNT(*) as grade_count,
                                    MIN(quiz) as min_grade,
                                    MAX(quiz) as max_grade,
                                    MIN(created_at) as first_grade_date,
                                    MAX(created_at) as last_grade_date
                                FROM course_student_scores
                                WHERE quiz IS NOT NULL
                                AND student_id IS NOT NULL
                                AND course_id IS NOT NULL
                                AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')
                                {student_filter}
                                GROUP BY student_id, course_id, course_name
                                ORDER BY student_id, course_id
                            """

                            cursor.execute(student_grades_query, filter_ids)
                            all_student_grade_records = cursor.fetchall()

                            logger.info(f"COMBINE DATA: Retrieved {len(all_student_grade_records)} FILTERED student-course grade records from database")

                            # Now we can trust all these records are actual students
                            for record in all_student_grade_records:
                                key = f"{record[0]}_{record[1]}"  # student_id_course_id
                                student_course_grades[key] = {
                                    'student_id': record[0],
                                    'course_id': record[1],
                                    'course_name': record[2],
                                    'avg_grade': float(record[3]) if record[3] else 0,
                                    'grade_count': record[4],
                                    'min_grade': float(record[5]) if record[5] else 0,
                                    'max_grade': float(record[6]) if record[6] else 0,
                                    'first_grade_date': record[7].isoformat() if record[7] else None,
                                    'last_grade_date': record[8].isoformat() if record[8] else None,
                                }

                            logger.info(f"COMBINE DATA: Built {len(student_course_grades)} student-course grade records with proper filtering")

                            if student_course_grades:
                                # Log some sample keys
                                sample_grade_keys = list(student_course_grades.keys())[:5]
                                logger.info(f"COMBINE DATA: Sample grade keys: {sample_grade_keys}")
                            else:
                                logger.warning(f"COMBINE DATA: ❌ NO FILTERED GRADE RECORDS found!")
                        else:
                            logger.warning(f"COMBINE DATA: No filter IDs available for student filtering")
                            student_course_grades = {}
                else:
                    logger.warning(f"COMBINE DATA: No academic year provided - falling back to filtering by activity students")
                    # Fallback to old approach if no academic year provided
                    with connections['analysis_db'].cursor() as cursor:
                        # Get all grades and filter by students with activity data
                        student_grades_query = """
                            SELECT
                                student_id,
                                course_id,
                                course_name,
                                AVG(quiz) as avg_grade,
                                COUNT(*) as grade_count,
                                MIN(quiz) as min_grade,
                                MAX(quiz) as max_grade,
                                MIN(created_at) as first_grade_date,
                                MAX(created_at) as last_grade_date
                            FROM course_student_scores
                            WHERE quiz IS NOT NULL
                            AND student_id IS NOT NULL
                            AND course_id IS NOT NULL
                            AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')
                            GROUP BY student_id, course_id, course_name
                            ORDER BY student_id, course_id
                        """

                        cursor.execute(student_grades_query)
                        all_student_grade_records = cursor.fetchall()

                        logger.info(f"COMBINE DATA: Retrieved {len(all_student_grade_records)} total student-course grade records from database")

                        # Get the student IDs that have activity data to filter grades
                        students_with_activity = set()
                        for access in access_analytics.get('student_access', []):
                            students_with_activity.add(access['student_id'])

                        logger.info(f"COMBINE DATA: Found {len(students_with_activity)} students with activity data")

                        # Filter grade records to only include students who have activity data
                        filtered_count = 0
                        for record in all_student_grade_records:
                            student_id = record[0]

                            # Only include if this student also has activity data
                            if student_id in students_with_activity:
                                key = f"{record[0]}_{record[1]}"  # student_id_course_id
                                student_course_grades[key] = {
                                    'student_id': record[0],
                                    'course_id': record[1],
                                    'course_name': record[2],
                                    'avg_grade': float(record[3]) if record[3] else 0,
                                    'grade_count': record[4],
                                    'min_grade': float(record[5]) if record[5] else 0,
                                    'max_grade': float(record[6]) if record[6] else 0,
                                    'first_grade_date': record[7].isoformat() if record[7] else None,
                                    'last_grade_date': record[8].isoformat() if record[8] else None,
                                }
                                filtered_count += 1

                        logger.info(f"COMBINE DATA: Built {len(student_course_grades)} student-course grade records (filtered to students with activity)")

            except Exception as e:
                logger.error(f"Error fetching individual student grades: {str(e)}")
                student_course_grades = {}

            # Create correlation analysis between activities and grades
            activity_grade_correlations = []
            students_with_both_data = []

            # Debug the matching process
            matched_keys = []
            unmatched_grade_keys = []
            unmatched_activity_keys = []

            logger.info(f"COMBINE DATA: Starting correlation matching...")
            logger.info(f"COMBINE DATA: - {len(student_course_grades)} grade keys available")
            logger.info(f"COMBINE DATA: - {len(student_course_activities)} activity keys available")

            for key, grade_data in student_course_grades.items():
                if key in student_course_activities:
                    activity_data = student_course_activities[key]
                    matched_keys.append(key)

                    # Build correlation with basic fields
                    correlation = {
                        'student_id': grade_data['student_id'],
                        'course_id': grade_data['course_id'],
                        'course_name': grade_data['course_name'],
                        'avg_grade': grade_data['avg_grade'],
                        'grade_count': grade_data['grade_count'],
                        'total_activities': activity_data['total_activities'],
                        'active_days': activity_data['active_days'],
                        'activities_per_grade_point': (
                            activity_data['total_activities'] / grade_data['avg_grade']
                            if grade_data['avg_grade'] > 0 else 0
                        ),
                        'grade_per_activity': (
                            grade_data['avg_grade'] / activity_data['total_activities']
                            if activity_data['total_activities'] > 0 else 0
                        )
                    }

                    # Add all dynamic activity type fields from activity_data
                    # Skip the standard fields and only add activity type counts
                    standard_activity_fields = {
                        'student_id', 'actor_account_name', 'course_id', 'total_activities',
                        'active_days', 'first_access', 'last_access'
                    }

                    for field_name, field_value in activity_data.items():
                        if (field_name not in standard_activity_fields and
                            isinstance(field_value, (int, float))):
                            correlation[field_name] = field_value

                    activity_grade_correlations.append(correlation)
                    students_with_both_data.append({
                        'student_id': grade_data['student_id'],
                        'course_id': grade_data['course_id'],
                        'has_grades': True,
                        'has_activities': True
                    })

                    # Log first few matches
                    if len(matched_keys) <= 3:
                        logger.info(f"COMBINE DATA: ✓ CORRELATION {len(matched_keys)}: Key '{key}', Activities: {activity_data['total_activities']}, Grade: {grade_data['avg_grade']}")
                else:
                    unmatched_grade_keys.append(key)

            # Check for activity keys that don't have matching grades
            for key in student_course_activities.keys():
                if key not in student_course_grades:
                    unmatched_activity_keys.append(key)

            logger.info(f"COMBINE DATA: Matching results (student-filtered):")
            logger.info(f"  - Matched keys: {len(matched_keys)}")
            logger.info(f"  - Unmatched grade keys: {len(unmatched_grade_keys)}")
            logger.info(f"  - Unmatched activity keys: {len(unmatched_activity_keys)}")

            # Additional analysis of matching patterns
            if matched_keys:
                matched_student_ids = set()
                matched_course_ids = set()
                for key in matched_keys:
                    student_id, course_id = key.split('_')
                    matched_student_ids.add(student_id)
                    matched_course_ids.add(course_id)

                logger.info(f"COMBINE DATA: Matching covers {len(matched_student_ids)} unique students and {len(matched_course_ids)} unique courses")
                logger.info(f"COMBINE DATA: Student ID range in matches: {min(matched_student_ids)} to {max(matched_student_ids)}")
                logger.info(f"COMBINE DATA: Course ID range in matches: {min(matched_course_ids)} to {max(matched_course_ids)}")

            # Analyze why we have unmatched records
            if unmatched_grade_keys:
                grade_student_ids = set()
                grade_course_ids = set()
                for key in unmatched_grade_keys[:100]:  # Sample first 100
                    student_id, course_id = key.split('_')
                    grade_student_ids.add(student_id)
                    grade_course_ids.add(course_id)

                logger.info(f"COMBINE DATA: Unmatched grades - {len(grade_student_ids)} students, {len(grade_course_ids)} courses (sample of 100)")

            if unmatched_activity_keys:
                activity_student_ids = set()
                activity_course_ids = set()
                for key in unmatched_activity_keys[:100]:  # Sample first 100
                    student_id, course_id = key.split('_')
                    activity_student_ids.add(student_id)
                    activity_course_ids.add(course_id)

                logger.info(f"COMBINE DATA: Unmatched activities - {len(activity_student_ids)} students, {len(activity_course_ids)} courses (sample of 100)")

            if len(activity_grade_correlations) == 0:
                logger.error(f"COMBINE DATA: ❌ ZERO CORRELATIONS CREATED! This is why the scatter plot doesn't show.")
                logger.error(f"COMBINE DATA: Possible issues:")
                logger.error(f"  1. No students have both grade and activity data")
                logger.error(f"  2. Student ID or Course ID format mismatch between databases")
                logger.error(f"  3. Student filtering is too restrictive")
                logger.error(f"  4. Date range filtering excludes all data")

            # Calculate top activity types from correlations or use from access_analytics if available
            access_top_activities = access_analytics.get('top_activity_types', [])
            if access_top_activities:
                # Use the top activity types directly from access analytics (from database query)
                top_activity_types = access_top_activities
                logger.info(f"COMBINE DATA: Using top activity types from access analytics: {[at['name'] for at in top_activity_types]}")
            else:
                # Fall back to calculating from correlations
                top_activity_types = cls._calculate_top_activity_types(activity_grade_correlations)
                logger.info(f"COMBINE DATA: Calculated top activity types from correlations: {[at['name'] for at in top_activity_types]}")

            # Analyze patterns based on activity levels and grades
            engagement_analysis = cls._analyze_engagement_patterns(activity_grade_correlations)

            # Course-level correlation analysis
            course_correlations = cls._analyze_course_level_correlations(activity_grade_correlations)

            # Student-level insights
            student_insights = cls._analyze_student_level_patterns(activity_grade_correlations)

            # Generate insights about the filtering effectiveness
            insights = cls._generate_student_filtering_insights(
                grade_analytics, access_analytics, activity_grade_correlations
            )

            logger.info(f"COMBINE DATA: Successfully created combined analytics with {len(activity_grade_correlations)} correlations")
            logger.info(f"COMBINE DATA: Engagement analysis contains {len(engagement_analysis.get('course_level_data', []))} course records")
            logger.info(f"COMBINE DATA: Student insights contains {len(student_insights.get('multi_course_students', []))} multi-course students")
            logger.info(f"COMBINE DATA: Top activity types calculated: {[at['name'] for at in top_activity_types]}")

            return {
                'student_course_correlations': activity_grade_correlations,  # Show all correlations for complete analysis
                'top_activity_types': top_activity_types,  # Add top activity types for legend
                'engagement_analysis': engagement_analysis,
                'course_correlations': course_correlations,
                'student_insights': student_insights,
                'insights': insights,  # Add filtering insights
                'summary_stats': {
                    'total_students_with_grades': len(set(g['student_id'] for g in student_course_grades.values())),
                    'total_students_with_activities': len(set(a['student_id'] for a in access_analytics.get('student_access', []))),
                    'students_with_both_data': len(set(c['student_id'] for c in activity_grade_correlations)),
                    'courses_with_both_data': len(set(c['course_id'] for c in activity_grade_correlations)),
                    'total_correlations': len(activity_grade_correlations)
                }
            }

        except Exception as e:
            logger.error(f"Error combining grade and access data: {str(e)}")
            logger.error(f"Exception details:", exc_info=True)
            return {}

    @classmethod
    def _calculate_top_activity_types(cls, correlations: List[Dict]) -> List[Dict[str, Any]]:
        """Calculate the top 10 activity types by total count from correlations - now dynamic"""
        try:
            if not correlations:
                return []

            # Get activity type keys dynamically from correlations data
            # We look for fields that are not the standard fields
            standard_fields = {
                'student_id', 'course_id', 'course_name', 'avg_grade', 'grade_count',
                'total_activities', 'active_days', 'activities_per_grade_point',
                'grade_per_activity', 'min_grade', 'max_grade', 'first_access', 'last_access'
            }

            # Find dynamic activity type fields from the first correlation record
            activity_type_fields = []
            if correlations:
                first_correlation = correlations[0]
                for key, value in first_correlation.items():
                    if key not in standard_fields and isinstance(value, (int, float)):
                        activity_type_fields.append(key)

            logger.debug(f"TOP ACTIVITY TYPES: Found dynamic activity fields: {activity_type_fields}")

            # Calculate totals for each dynamic activity type
            activity_totals = {}
            for field_name in activity_type_fields:
                total_count = sum(correlation.get(field_name, 0) for correlation in correlations)

                # Create user-friendly name from field name (operation_name as-is per user request)
                display_name = field_name.replace('_', ' ').title()

                activity_totals[field_name] = {
                    'key': field_name,
                    'name': display_name,  # Use the field name converted to title case
                    'description': f'Activity type: {display_name}',
                    'total_count': total_count,
                    'avg_per_student_course': round(total_count / len(correlations), 2) if correlations else 0
                }

            # Sort by total count and get top 10
            top_activities = sorted(
                activity_totals.values(),
                key=lambda x: x['total_count'],
                reverse=True
            )[:10]

            # Filter out activities with zero count
            top_activities = [activity for activity in top_activities if activity['total_count'] > 0]

            logger.info(f"TOP ACTIVITY TYPES: Calculated {len(top_activities)} dynamic activity types from {len(correlations)} correlations")
            for i, activity in enumerate(top_activities):
                logger.info(f"  {i+1}. {activity['name']}: {activity['total_count']} total ({activity['avg_per_student_course']} avg)")

            return top_activities

        except Exception as e:
            logger.error(f"Error calculating top activity types: {str(e)}")
            logger.error(f"Exception details:", exc_info=True)
            return []

    @classmethod
    def _analyze_course_level_correlations(cls, correlations: List[Dict]) -> Dict[str, Any]:
        """Analyze correlations at the course level"""
        try:
            # Group correlations by course
            course_data = {}
            for correlation in correlations:
                course_id = correlation['course_id']
                if course_id not in course_data:
                    course_data[course_id] = {
                        'course_name': correlation['course_name'],
                        'correlations': [],
                        'total_activities': 0,
                        'total_grades': 0,
                        'student_count': 0
                    }

                course_data[course_id]['correlations'].append(correlation)
                course_data[course_id]['total_activities'] += correlation['total_activities']
                course_data[course_id]['total_grades'] += correlation['avg_grade']
                course_data[course_id]['student_count'] += 1

            # Calculate statistics for each course
            course_stats = []
            for course_id, data in course_data.items():
                if data['student_count'] > 0:
                    avg_activities = data['total_activities'] / data['student_count']
                    avg_grade = data['total_grades'] / data['student_count']

                    course_stats.append({
                        'course_id': course_id,
                        'course_name': data['course_name'],
                        'student_count': data['student_count'],
                        'avg_activities_per_student': round(avg_activities, 2),
                        'avg_grade': round(avg_grade, 2),
                        'total_activities': data['total_activities'],
                        'activity_efficiency': round(avg_grade / avg_activities, 3) if avg_activities > 0 else 0
                    })

            # Sort by student count (most students first)
            course_stats.sort(key=lambda x: x['student_count'], reverse=True)

            return {
                'courses': course_stats,
                'total_courses': len(course_stats)
            }

        except Exception as e:
            logger.error(f"Error analyzing course level correlations: {str(e)}")
            return {'courses': [], 'total_courses': 0}

    @classmethod
    def _generate_student_filtering_insights(cls, grade_analytics: Dict, access_analytics: Dict, correlations: List[Dict]) -> List[str]:
        """Generate insights about the effectiveness of student filtering"""
        insights = []

        try:
            grade_students = grade_analytics.get('overall_stats', {}).get('total_students', 0)
            activity_students = access_analytics.get('overall_stats', {}).get('total_unique_students', 0)
            correlation_students = len(set(c['student_id'] for c in correlations)) if correlations else 0

            insights.append(f"Analysis includes {grade_students} students with grades and {activity_students} students with recorded activities.")

            if correlation_students > 0:
                coverage_rate = (correlation_students / max(grade_students, activity_students)) * 100
                insights.append(f"{correlation_students} students have both grade and activity data, representing {coverage_rate:.1f}% coverage.")

                if coverage_rate >= 70:
                    insights.append("High data coverage enables reliable correlation analysis between student activities and academic performance.")
                elif coverage_rate >= 40:
                    insights.append("Moderate data coverage provides meaningful insights, though some students lack complete data.")
                else:
                    insights.append("Limited data coverage suggests many students may be missing either grade or activity records.")

            # Insights about student filtering effectiveness
            if activity_students > 0:
                activity_types = access_analytics.get('activity_types', [])
                if activity_types:
                    top_activity = activity_types[0]
                    insights.append(f"Most common student activity: '{top_activity['operation_name']}' with {top_activity['activity_count']} recorded instances.")

            return insights

        except Exception as e:
            logger.error(f"Error generating student filtering insights: {str(e)}")
            return ["Student filtering analysis completed with some processing errors."]

    @classmethod
    def _analyze_engagement_patterns(cls, correlations: List[Dict]) -> Dict[str, Any]:
        """Analyze patterns at course level"""
        course_data = {}

        for correlation in correlations:
            course_id = correlation['course_id']
            if course_id not in course_data:
                course_data[course_id] = {
                    'course_name': correlation['course_name'],
                    'students': [],
                    'total_activities': 0,
                    'total_grades': 0,
                    'student_count': 0
                }

            course_data[course_id]['students'].append(correlation)
            course_data[course_id]['total_activities'] += correlation['total_activities']
            course_data[course_id]['total_grades'] += correlation['avg_grade']
            course_data[course_id]['student_count'] += 1

        # Calculate course-level statistics
        course_correlations = []
        for course_id, data in course_data.items():
            if data['student_count'] > 0:
                avg_activities = data['total_activities'] / data['student_count']
                avg_grade = data['total_grades'] / data['student_count']

                course_correlations.append({
                    'course_id': course_id,
                    'course_name': data['course_name'],
                    'student_count': data['student_count'],
                    'avg_activities_per_student': round(avg_activities, 2),
                    'avg_grade': round(avg_grade, 2),
                    'total_activities': data['total_activities'],
                    'activity_grade_ratio': round(avg_activities / avg_grade, 2) if avg_grade > 0 else 0
                })

        # Sort by student count for relevance
        course_correlations.sort(key=lambda x: x['student_count'], reverse=True)

        return {
            'course_level_data': course_correlations[:20],  # Top 20 courses
            'insights': cls._generate_course_insights(course_correlations)
        }

    @classmethod
    def _analyze_student_level_patterns(cls, correlations: List[Dict]) -> Dict[str, Any]:
        """Analyze patterns at individual student level"""
        student_data = {}

        for correlation in correlations:
            student_id = correlation['student_id']
            if student_id not in student_data:
                student_data[student_id] = {
                    'courses': [],
                    'total_activities': 0,
                    'total_grades': 0,
                    'course_count': 0
                }

            student_data[student_id]['courses'].append(correlation)
            student_data[student_id]['total_activities'] += correlation['total_activities']
            student_data[student_id]['total_grades'] += correlation['avg_grade']
            student_data[student_id]['course_count'] += 1

        # Find students with multiple courses for better analysis
        multi_course_students = []
        for student_id, data in student_data.items():
            if data['course_count'] > 1:  # Students with multiple courses
                avg_activities = data['total_activities'] / data['course_count']
                avg_grade = data['total_grades'] / data['course_count']

                multi_course_students.append({
                    'student_id': student_id,
                    'course_count': data['course_count'],
                    'avg_activities_per_course': round(avg_activities, 2),
                    'avg_grade_across_courses': round(avg_grade, 2),
                    'total_activities': data['total_activities'],
                    'consistency_score': cls._calculate_consistency_score(data['courses'])
                })

        # Sort by course count and then by total activities
        multi_course_students.sort(key=lambda x: (x['course_count'], x['total_activities']), reverse=True)

        return {
            'multi_course_students': multi_course_students[:20],  # Top 20 students
            'total_students_analyzed': len(student_data),
            'students_with_multiple_courses': len(multi_course_students)
        }

    @classmethod
    def _calculate_consistency_score(cls, courses: List[Dict]) -> float:
        """Calculate how consistent a student's activity-grade relationship is across courses"""
        if len(courses) < 2:
            return 0.0

        ratios = [c['grade_per_activity'] for c in courses if c['total_activities'] > 0]
        if len(ratios) < 2:
            return 0.0

        # Calculate coefficient of variation (lower = more consistent)
        mean_ratio = sum(ratios) / len(ratios)
        variance = sum((r - mean_ratio) ** 2 for r in ratios) / len(ratios)
        std_dev = variance ** 0.5

        cv = std_dev / mean_ratio if mean_ratio > 0 else float('inf')

        # Convert to consistency score (higher = more consistent)
        consistency_score = max(0, 1 - cv)
        return round(consistency_score, 3)

    @classmethod
    def _generate_course_insights(cls, course_correlations: List[Dict]) -> List[str]:
        """Generate insights about course-level patterns"""
        insights = []

        if not course_correlations:
            return ["No course correlation data available."]

        # Find courses with highest activity-grade ratios
        high_ratio_courses = [c for c in course_correlations if c['activity_grade_ratio'] > 1.0]
        if high_ratio_courses:
            insights.append(f"{len(high_ratio_courses)} courses require high activity levels relative to grades achieved.")

        # Find courses with good engagement
        high_activity_courses = sorted(course_correlations, key=lambda x: x['avg_activities_per_student'], reverse=True)[:3]
        if high_activity_courses:
            top_course = high_activity_courses[0]
            insights.append(f"'{top_course['course_name']}' has highest student engagement with {top_course['avg_activities_per_student']} activities per student.")

        # Find courses with high grades
        high_grade_courses = sorted(course_correlations, key=lambda x: x['avg_grade'], reverse=True)[:3]
        if high_grade_courses:
            top_grade_course = high_grade_courses[0]
            insights.append(f"'{top_grade_course['course_name']}' achieves highest average grade of {top_grade_course['avg_grade']}.")

        return insights

    @classmethod
    def _calculate_summary_stats(cls, grade_analytics: Dict, access_analytics: Dict) -> Dict[str, Any]:
        """Calculate summary statistics for the dashboard"""
        try:
            grade_stats = grade_analytics.get('overall_stats', {})
            combined_analytics = access_analytics.get('combined_analytics', {})

            logger.debug(f"SUMMARY STATS: Grade stats: {grade_stats}")
            logger.debug(f"SUMMARY STATS: Access analytics overall_stats: {access_analytics.get('overall_stats', {})}")

            # Get basic counts
            total_student_access_records = len(access_analytics.get('student_access', []))
            total_course_access_records = len(access_analytics.get('course_access', []))

            logger.debug(f"SUMMARY STATS: Student access records: {total_student_access_records}")
            logger.debug(f"SUMMARY STATS: Course access records: {total_course_access_records}")

            # FIXED: Use the correct total_activities from ClickHouse overall_stats
            # The previous method was summing course-level activities which causes double-counting
            # when students are active across multiple courses

            # Try to get total_activities from the access_analytics overall_stats first
            # This comes from the ClickHouse query with COUNT(DISTINCT _id) which is accurate
            total_activities = 0

            # Check if we have access_analytics with overall_stats (from ClickHouse)
            if 'overall_stats' in access_analytics:
                total_activities = access_analytics['overall_stats'].get('total_activities', 0)
                logger.debug(f"SUMMARY STATS: Using ClickHouse overall_stats total_activities: {total_activities}")
            else:
                # Fallback: sum from course_access but log a warning about potential double-counting
                total_activities = sum(
                    access['total_activities']
                    for access in access_analytics.get('course_access', [])
                )
                logger.warning(f"SUMMARY STATS: Using course-level sum for total_activities (may double-count): {total_activities}")

            # Get correlation statistics if available
            correlation_stats = combined_analytics.get('summary_stats', {})
            logger.debug(f"SUMMARY STATS: Correlation stats: {correlation_stats}")

            result = {
                'total_students_with_grades': grade_stats.get('total_students', 0),
                'total_courses_with_grades': grade_stats.get('total_courses', 0),
                'total_grade_records': grade_stats.get('total_grades', 0),
                'overall_avg_grade': grade_stats.get('avg_grade', 0),
                'total_student_access_records': total_student_access_records,
                'total_course_access_records': total_course_access_records,
                'total_activities': total_activities,
                'avg_activities_per_course': (
                    total_activities / total_course_access_records
                    if total_course_access_records > 0 else 0
                ),
                # New correlation statistics
                'students_with_both_data': correlation_stats.get('students_with_both_data', 0),
                'courses_with_both_data': correlation_stats.get('courses_with_both_data', 0),
                'total_correlations': correlation_stats.get('total_correlations', 0),
                'correlation_coverage': {
                    'student_coverage_percentage': round(
                        (correlation_stats.get('students_with_both_data', 0) /
                         max(correlation_stats.get('total_students_with_activities', 1), 1)) * 100, 2
                    ),
                    'grade_coverage_percentage': round(
                        (correlation_stats.get('students_with_both_data', 0) /
                         max(correlation_stats.get('total_students_with_grades', 1), 1)) * 100, 2
                    )
                }
            }

            logger.debug(f"SUMMARY STATS: Final result: {result}")
            return result

        except Exception as e:
            logger.error(f"Error calculating summary stats: {str(e)}")
            return {}

    @classmethod
    def get_time_spent_vs_grade_correlation(cls, academic_year: int) -> Dict[str, Any]:
        """
        Get time spent vs grade correlation data for students who have grades.

        This method:
        1. First finds students who have grades in courses for the given academic year
        2. Then calculates their time spent on platform using ClickHouse
        3. Returns correlation data for scatter plot visualization

        Args:
            academic_year (int): The academic year to analyze

        Returns:
            Dict with correlation data, statistics, and metadata
        """
        logger.info(f"Fetching time spent vs grade correlation for academic year {academic_year}")

        try:
            # Step 1: Get students who have grades for this academic year
            students_with_grades = cls._get_students_grades_for_correlation(academic_year)

            if not students_with_grades:
                logger.warning(f"No students with grades found for academic year {academic_year}")
                return {
                    'error': 'No students with grades found for this academic year',
                    'correlation_data': [],
                    'statistics': {},
                    'metadata': {
                        'academic_year': academic_year,
                        'students_with_grades_only': 0,
                        'students_with_time_data': 0,
                        'total_data_points': 0
                    }
                }

            logger.info(f"Found {len(students_with_grades)} students with grades")

            # Step 2: Get time spent data for these students using simplified approach
            grade_student_ids = list(students_with_grades.keys())

            # Prepare course filter data for more accurate time tracking
            course_filter_data = {}
            for student_id, grade_info in students_with_grades.items():
                if 'course_ids' in grade_info and grade_info['course_ids']:
                    course_filter_data[student_id] = grade_info['course_ids']

            logger.info(f"Prepared course filter data for {len(course_filter_data)} students")

            time_data = cls._get_students_with_time_data(
                grade_student_ids,
                academic_year,
                course_filter_data=course_filter_data
            )
            logger.debug(f"TIME DATA: {time_data}")

            if not time_data:
                logger.warning(f"No time data found for students with grades in academic year {academic_year}")
                return {
                    'error': 'No time data found for students with grades',
                    'correlation_data': [],
                    'statistics': {},
                    'metadata': {
                        'academic_year': academic_year,
                        'students_with_grades_only': len(students_with_grades),
                        'students_with_time_data': 0,
                        'total_data_points': 0
                    }
                }

            # Step 3: Combine grade and time data
            correlation_data = []
            for student_id, grade_info in students_with_grades.items():
                if student_id in time_data:
                    correlation_data.append({
                        'student_id': student_id,
                        'average_grade': grade_info['average_grade'],
                        'total_grades': grade_info['grade_count'],  # Frontend expects total_grades
                        'total_time_spent_minutes': time_data[student_id]['total_minutes'],  # Frontend expects total_time_spent_minutes
                        'active_days': time_data[student_id]['active_days'],
                        'average_daily_minutes': time_data[student_id]['average_daily_minutes'],
                        'course_count': grade_info['course_count']  # Use actual course count from grade data
                    })

            if not correlation_data:
                return {
                    'error': 'No students found with both grades and time data',
                    'correlation_data': [],
                    'statistics': {},
                    'metadata': {
                        'academic_year': academic_year,
                        'students_with_grades_only': len(students_with_grades),
                        'students_with_time_data': len(time_data),
                        'total_data_points': 0
                    }
                }

            # Step 4: Calculate correlation statistics using the proper method
            statistics = cls._calculate_correlation_statistics(correlation_data)

            return {
                'correlation_data': correlation_data,
                'statistics': statistics,
                'metadata': {
                    'academic_year': academic_year,
                    'students_with_grades_only': len(students_with_grades),
                    'students_with_time_data': len(time_data),
                    'total_data_points': len(correlation_data),
                    'method': 'simplified_numeric_matching'
                }
            }

        except Exception as e:
            logger.error(f"Error in get_time_spent_vs_grade_correlation: {e}")
            return {
                'error': f'Error calculating correlation: {str(e)}',
                'correlation_data': [],
                'statistics': {},
                'metadata': {
                    'academic_year': academic_year,
                    'students_with_grades_only': 0,
                    'students_with_time_data': 0,
                    'total_data_points': 0
                }
            }

    @classmethod
    def _get_students_with_any_grades(cls, academic_year: int, filter_type: str, filter_ids: List[str]) -> Dict[str, Dict]:
        """
        Fallback method to find students with grades for any courses (not filtered by specific course IDs).
        This helps identify if the issue is with course categorization or actual data availability.
        """
        logger.info(f"Using fallback approach to find students with any grades for year {academic_year}")

        try:
            with connections['analysis_db'].cursor() as cursor:
                # Build student filter clause
                filter_placeholders = ",".join(["%s"] * len(filter_ids))
                if filter_type == 'NOT_IN':
                    student_filter = f" AND student_id NOT IN ({filter_placeholders}) AND student_id IS NOT NULL"
                    logger.debug(f"Using NOT IN filter to exclude {len(filter_ids)} non-students")
                else:
                    student_filter = f" AND student_id IN ({filter_placeholders})"
                    logger.debug(f"Using IN filter to include {len(filter_ids)} students")

                # Find students with grades (any courses)
                query = f"""
                    SELECT
                        student_id,
                        AVG(quiz) as average_grade,
                        COUNT(*) as grade_count
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND (name LIKE '%Benesse%' OR name LIKE '%ベネッセ%') {student_filter}
                    AND quiz >= 0 AND quiz <= 100
                    GROUP BY student_id
                    HAVING COUNT(*) > 0
                """

                cursor.execute(query, filter_ids)
                results = cursor.fetchall()

                logger.info(f"Fallback approach found {len(results)} students with grades")

                students_grades = {}
                for row in results:
                    student_id, avg_grade, grade_count = row
                    students_grades[str(student_id)] = {
                        'average_grade': float(avg_grade),
                        'grade_count': int(grade_count)
                    }

                return students_grades

        except Exception as e:
            logger.error(f"Error in fallback grades search: {str(e)}")
            return {}

    @classmethod
    def _get_students_grades_for_correlation(cls, academic_year: int) -> Dict[str, Dict[str, Any]]:
        """
        Get student grades aggregated by student for correlation analysis.

        Returns:
            Dict[student_id, Dict] containing average grades and course counts per student
        """
        try:
            with connections['analysis_db'].cursor() as cursor:
                # Build student filter clause based on optimal approach
                filter_placeholders = ",".join(["%s"] * len(filter_ids))
                if filter_type == 'NOT_IN':
                    student_filter = f" AND student_id NOT IN ({filter_placeholders}) AND student_id IS NOT NULL"
                else:
                    student_filter = f" AND student_id IN ({filter_placeholders})"

                # Build course filter clause
                course_filter_placeholders = ",".join(["%s"] * len(course_ids))
                course_filter = f" AND course_id IN ({course_filter_placeholders})"

                # Get aggregated grades per student
                grades_query = f"""
                    SELECT
                        student_id,
                        AVG(quiz) as average_grade,
                        COUNT(*) as total_grades,
                        COUNT(DISTINCT course_id) as course_count,
                        MIN(quiz) as min_grade,
                        MAX(quiz) as max_grade
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND quiz >= 0 AND quiz <= 100
                    AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')
                    {student_filter}{course_filter}
                    GROUP BY student_id
                    HAVING total_grades >= 1
                    ORDER BY student_id
                """

                cursor.execute(grades_query, filter_ids + course_ids)
                grade_results = cursor.fetchall()

                students_grades = {}
                for row in grade_results:
                    student_id = row[0]
                    students_grades[student_id] = {
                        'average_grade': float(row[1]),
                        'total_grades': row[2],
                        'course_count': row[3],
                        'min_grade': float(row[4]),
                        'max_grade': float(row[5])
                    }

                logger.info(f"Retrieved grades for {len(students_grades)} students")
                return students_grades

        except Exception as e:
            logger.error(f"Error getting student grades for correlation: {str(e)}")
            return {}

    @classmethod
    def _calculate_time_spent_for_students(cls, student_ids: List[str], start_date: str, end_date: str) -> Dict[str, Dict]:
        """
        Calculate time spent for specific students using ClickHouse.
        Based on the get_time_spent_distribution method but optimized for specific students.

        Returns:
            Dict[student_id, Dict] containing time spent data per student
        """
        try:
            from django.conf import settings

            # Get session duration settings (same as get_time_spent_distribution)
            max_session_duration = getattr(settings, 'MAX_SESSION_DURATION', 5400)  # Default 1.5 hours
            max_activity_duration = 1800  # 30 minutes cap per individual activity session

            # Convert student IDs to actor account names for ClickHouse filtering
            # Need to handle different formats: "1369@UUID", "Learner:2549", "2549"
            actor_account_filters = []
            for student_id in student_ids:
                # Add multiple patterns to catch different actor_account_name formats
                actor_account_filters.extend([
                    f"'{student_id}@%'",  # For "1369@UUID" format
                    f"'Learner:{student_id}'",  # For "Learner:2549" format
                    f"'{student_id}'"  # For direct "2549" format
                ])

            # Build filter for ClickHouse - use LIKE for pattern matching
            actor_filter_conditions = []
            for i in range(0, len(actor_account_filters), 3):  # Process in groups of 3 (per student_id)
                student_filters = actor_account_filters[i:i+3]
                condition = " OR ".join([f"actor_account_name LIKE {pattern}" for pattern in student_filters])
                actor_filter_conditions.append(f"({condition})")

            actor_filter = " OR ".join(actor_filter_conditions)

            with connections['clickhouse_db_pre_2025'].cursor() as cursor:
                # Use the same three-tier query as get_time_spent_distribution but with student filtering
                time_spent_query = f"""
                    SELECT
                        student_id,
                        sum(minutes_spent) as total_minutes,
                        count() as active_days,
                        avg(minutes_spent) as average_daily_minutes
                    FROM
                    (
                        SELECT
                            student_id,
                            day,
                            round(sum(read_seconds) / 60, 2) AS minutes_spent
                        FROM
                        (
                            SELECT
                                actor_account_name AS student_id,
                                toDate(timestamp) AS day,
                                CASE
                                    WHEN time_diff <= {max_session_duration} THEN greatest(0, least({max_activity_duration}, time_diff))
                                    ELSE 0
                                END AS read_seconds
                            FROM
                            (
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
                                    ) AS time_diff
                                FROM statements_mv
                                WHERE actor_name_role == 'student'
                                    AND actor_account_name != ''
                                    AND timestamp >= toDate('{start_date}')
                                    AND timestamp <= toDate('{end_date}')
                                    AND ({actor_filter})
                            )
                        )
                        GROUP BY
                            student_id,
                            day
                        HAVING minutes_spent > 0
                    )
                    GROUP BY student_id
                    ORDER BY student_id
                """

                cursor.execute(time_spent_query)
                time_results = cursor.fetchall()

                students_time_data = {}
                for row in time_results:
                    actor_account_name = row[0]

                    # Extract student_id from actor_account_name
                    student_id = extract_student_id_from_actor_account_name(actor_account_name)

                    if student_id and student_id in student_ids:
                        students_time_data[student_id] = {
                            'total_minutes': float(row[1]),
                            'active_days': row[2],
                            'average_daily_minutes': float(row[3]),
                            'actor_account_name': actor_account_name  # For debugging
                        }

                logger.info(f"Calculated time spent for {len(students_time_data)} students")
                return students_time_data

        except Exception as e:
            logger.error(f"Error calculating time spent for students: {str(e)}")
            return {}

    @classmethod
    def _calculate_correlation_statistics(cls, correlation_data: List[Dict]) -> Dict[str, Any]:
        """
        Calculate correlation statistics for time spent vs grades.

        Returns:
            Dict containing correlation coefficient, regression data, and summary stats
        """
        try:
            if not correlation_data:
                return {}

            import statistics
            import math

            # Extract data for correlation calculation
            grades = [item['average_grade'] for item in correlation_data]
            time_minutes = [item['total_time_spent_minutes'] for item in correlation_data]

            if len(grades) < 2 or len(time_minutes) < 2:
                return {}

            # Calculate basic statistics
            mean_grade = statistics.mean(grades)
            mean_time = statistics.mean(time_minutes)
            std_grade = statistics.stdev(grades) if len(grades) > 1 else 0
            std_time = statistics.stdev(time_minutes) if len(time_minutes) > 1 else 0

            # Calculate Pearson correlation coefficient
            correlation_coefficient = 0
            if std_grade > 0 and std_time > 0:
                n = len(grades)
                sum_xy = sum(g * t for g, t in zip(grades, time_minutes))
                correlation_coefficient = (sum_xy - n * mean_grade * mean_time) / ((n - 1) * std_grade * std_time)

            # Calculate linear regression (y = mx + b, where y = grade, x = time)
            if std_time > 0:
                slope = correlation_coefficient * (std_grade / std_time)
                intercept = mean_grade - slope * mean_time
            else:
                slope = 0
                intercept = mean_grade

            # Generate regression line points for visualization
            min_time = min(time_minutes)
            max_time = max(time_minutes)
            regression_line = []
            for i in range(21):  # 21 points for smooth line
                x = min_time + (max_time - min_time) * i / 20
                y = slope * x + intercept
                regression_line.append({'x': round(x, 2), 'y': round(y, 2)})

            # Categorize correlation strength
            correlation_strength = "No correlation"
            if abs(correlation_coefficient) >= 0.7:
                correlation_strength = "Strong"
            elif abs(correlation_coefficient) >= 0.5:
                correlation_strength = "Moderate"
            elif abs(correlation_coefficient) >= 0.3:
                correlation_strength = "Weak"

            correlation_direction = "positive" if correlation_coefficient > 0 else "negative" if correlation_coefficient < 0 else "no"

            return {
                'correlation_coefficient': round(correlation_coefficient, 3),
                'correlation_strength': correlation_strength,
                'correlation_direction': correlation_direction,
                'slope': round(slope, 3),
                'intercept': round(intercept, 2),
                'regression_line': regression_line,
                'grade_stats': {
                    'mean': round(mean_grade, 2),
                    'std_dev': round(std_grade, 2),
                    'min': round(min(grades), 2),
                    'max': round(max(grades), 2),
                    'median': round(statistics.median(grades), 2)
                },
                'time_stats': {
                    'mean': round(mean_time, 2),
                    'std_dev': round(std_time, 2),
                    'min': round(min(time_minutes), 2),
                    'max': round(max(time_minutes), 2),
                    'median': round(statistics.median(time_minutes), 2)
                },
                'total_students': len(correlation_data),
                'r_squared': round(correlation_coefficient ** 2, 3)
            }

        except Exception as e:
            logger.error(f"Error calculating correlation statistics: {str(e)}")
            return {}

    @classmethod
    def _get_students_with_time_data(cls, grade_student_ids: List[str], academic_year: int,
                                   course_filter_data: Dict[str, List[str]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Get time spent data for students from ClickHouse using proper database routing.

        Uses different databases and student ID extraction logic based on academic year:
        - Before 2025: Use 'clickhouse_db_pre_2025' with extract_student_id_from_actor_account_name()
        - 2025 and after: Use 'clickhouse_db' with direct student ID matching

        Args:
            grade_student_ids: List of student IDs who have grades
            academic_year: Academic year for filtering
            course_filter_data: Optional dict mapping student_id to list of course_ids for filtering

        Returns:
            Dict mapping student_id to time data
        """
        # Get configuration values from settings
        max_session_duration = getattr(settings, 'MAX_SESSION_DURATION', 5400)  # Default 1.5 hours
        max_reading_time = getattr(settings, 'MAX_READING_TIME', 1800)  # Default 30 minutes

        logger.debug(f"MAX_SESSION_DURATION: {max_session_duration}")
        logger.debug(f"MAX_READING_TIME: {max_reading_time}")

        # Determine which database to use based on academic year
        db_alias = get_clickhouse_db_for_academic_year(academic_year)
        logger.info(f"Using database '{db_alias}' for academic year {academic_year}")

        try:
            # Academic year date range
            start_date = f"{academic_year}-04-01"
            end_date = f"{academic_year + 1}-03-31"

            # Convert grade_student_ids to set for faster lookup
            grade_student_ids_set = set(str(sid) for sid in grade_student_ids)

            # Course filtering logic
            course_filter_sql = ""
            if course_filter_data:
                # Build course filter - collect all unique course IDs
                all_course_ids = set()
                for student_id, course_ids in course_filter_data.items():
                    all_course_ids.update(course_ids)

                if all_course_ids:
                    course_ids_str = "', '".join(all_course_ids)
                    course_filter_sql = f" AND context_id IN ('{course_ids_str}')"
                    logger.debug(f"Applied course filter for {len(all_course_ids)} courses")

            if academic_year >= 2025:
                # For 2025+ database, use direct student ID matching
                id_filter_conditions = []
                for student_id in grade_student_ids:
                    id_filter_conditions.append(f"actor_account_name = '{student_id}'")

                if not id_filter_conditions:
                    logger.warning("No student ID conditions generated for ClickHouse query")
                    return {}

                id_filter = " OR ".join(id_filter_conditions)

                # ClickHouse query for 2025+ with direct matching
                time_query = f"""
                    SELECT
                        actor_account_name AS student_id,
                        sum(minutes_spent) as total_minutes,
                        count() as active_days,
                        avg(minutes_spent) as average_daily_minutes
                    FROM
                    (
                        SELECT
                            actor_account_name,
                            day,
                            round(sum(read_seconds) / 60, 2) AS minutes_spent
                        FROM
                        (
                            SELECT
                                actor_account_name,
                                toDate(timestamp) AS day,
                                CASE
                                    WHEN time_diff <= {max_session_duration} THEN greatest(0, least({max_reading_time}, time_diff))
                                    ELSE 0
                                END AS read_seconds
                            FROM
                            (
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
                                    ) AS time_diff
                                FROM statements_mv
                                WHERE actor_account_name != ''
                                    AND timestamp >= toDate('{start_date}')
                                    AND timestamp <= toDate('{end_date}')
                                    AND ({id_filter}){course_filter_sql}
                            )
                        )
                        GROUP BY
                            actor_account_name,
                            day
                        HAVING minutes_spent > 0
                    )
                    GROUP BY actor_account_name
                    ORDER BY actor_account_name
                """
                logger.debug(f"Using direct ID matching for {academic_year} (post-2025)")
            else:
                # For pre-2025 database, build student ID filter based on the three known formats
                id_filter_conditions = []
                for student_id in grade_student_ids:
                    # Handle the three patterns from extract_student_id_from_actor_account_name:
                    # 1. "1369@UUID" format
                    id_filter_conditions.append(f"actor_account_name LIKE '{student_id}@%'")
                    # 2. "Learner:2549" format
                    id_filter_conditions.append(f"actor_account_name = 'Learner:{student_id}'")
                    # 3. Direct numeric ID "2549"
                    id_filter_conditions.append(f"actor_account_name = '{student_id}'")

                if not id_filter_conditions:
                    logger.warning("No student ID conditions generated for ClickHouse query")
                    return {}

                id_filter = " OR ".join(id_filter_conditions)

                # For pre-2025 database, use student ID filtering in SQL instead of Python
                time_query = f"""
                    SELECT
                        actor_account_name,
                        sum(minutes_spent) as total_minutes,
                        count() as active_days,
                        avg(minutes_spent) as average_daily_minutes
                    FROM
                    (
                        SELECT
                            actor_account_name,
                            day,
                            round(sum(read_seconds) / 60, 2) AS minutes_spent
                        FROM
                        (
                            SELECT
                                actor_account_name,
                                toDate(timestamp) AS day,
                                CASE
                                    WHEN time_diff <= {max_session_duration} THEN greatest(0, least({max_reading_time}, time_diff))
                                    ELSE 0
                                END AS read_seconds
                            FROM
                            (
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
                                    ) AS time_diff
                                FROM statements_mv
                                WHERE actor_account_name != ''
                                    AND timestamp >= toDate('{start_date}')
                                    AND timestamp <= toDate('{end_date}')
                                    AND ({id_filter}){course_filter_sql}
                            )
                        )
                        GROUP BY
                            actor_account_name,
                            day
                        HAVING minutes_spent > 0
                    )
                    GROUP BY actor_account_name
                    ORDER BY actor_account_name
                """
                logger.debug(f"Using pattern-based ID filtering for {academic_year} (pre-2025)")

            logger.debug(f"TIME QUERY for {db_alias}: {time_query}")

            with connections[db_alias].cursor() as cursor:
                cursor.execute(time_query)
                time_results = cursor.fetchall()

            logger.info(f"Raw query returned {len(time_results)} results from {db_alias}")

            # Convert to dictionary with proper student ID extraction
            time_data = {}
            for actor_account_name, total_minutes, active_days, avg_daily_minutes in time_results:
                if academic_year >= 2025:
                    # For 2025+ database, actor_account_name should be direct student ID
                    student_id = str(actor_account_name)
                else:
                    # For pre-2025 database, extract student ID from actor_account_name
                    student_id = extract_student_id_from_actor_account_name(actor_account_name)

                # Only include if we have a valid student ID and it's in our target list
                if student_id and student_id in grade_student_ids_set:
                    time_data[student_id] = {
                        'total_minutes': float(total_minutes) if total_minutes else 0.0,
                        'active_days': int(active_days) if active_days else 0,
                        'average_daily_minutes': float(avg_daily_minutes) if avg_daily_minutes else 0.0,
                        'actor_account_name': actor_account_name,  # For debugging
                        'database_used': db_alias  # For debugging
                    }
                elif academic_year < 2025 and not student_id:
                    # Log unrecognized formats for debugging (only for pre-2025)
                    logger.debug(f"Could not extract student_id from actor_account_name: '{actor_account_name}'")

            logger.info(f"Successfully processed {len(time_data)} students with time data from {db_alias}")
            return time_data

        except Exception as e:
            logger.error(f"Error getting time data from {db_alias}: {e}")
            return {}

    @classmethod
    def _get_students_grades_for_correlation(cls, academic_year: int) -> Dict[str, Dict[str, Any]]:
        """
        Get students who have grades for a specific academic year.

        Args:
            academic_year: The academic year to filter by

        Returns:
            Dict mapping student_id to grade information including course count
        """
        try:
            # Get optimal student filtering approach
            filter_config = PastYearCourseCategory.get_optimal_student_filter_for_academic_year(academic_year)
            filter_type = filter_config['filter_type']
            filter_ids = filter_config['filter_ids']

            if not filter_ids:
                logger.warning(f"No filter IDs found for academic year {academic_year}")
                return {}

            # Build student filter clause
            filter_placeholders = ",".join(["%s"] * len(filter_ids))
            if filter_type == 'NOT_IN':
                student_filter = f" AND student_id NOT IN ({filter_placeholders}) AND student_id IS NOT NULL"
            else:
                student_filter = f" AND student_id IN ({filter_placeholders})"

            # Find students with grades (Benesse grades) for this academic year
            # Include course count to show actual number of courses per student
            query = f"""
                SELECT
                    student_id,
                    AVG(quiz) as average_grade,
                    COUNT(*) as grade_count,
                    COUNT(DISTINCT course_id) as course_count,
                    GROUP_CONCAT(DISTINCT course_id) as course_ids
                FROM course_student_scores
                WHERE quiz IS NOT NULL
                AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%') {student_filter}
                AND course_name LIKE '{academic_year}%%'
                AND quiz >= 0 AND quiz <= 100
                GROUP BY student_id
                HAVING COUNT(*) > 0
            """

            with connections['analysis_db'].cursor() as cursor:
                cursor.execute(query, filter_ids)
                results = cursor.fetchall()

            # Convert to dictionary
            students_with_grades = {}
            for student_id, avg_grade, grade_count, course_count, course_ids in results:
                students_with_grades[str(student_id)] = {
                    'average_grade': float(avg_grade),
                    'grade_count': int(grade_count),
                    'course_count': int(course_count),
                    'course_ids': course_ids.split(',') if course_ids else []
                }

            logger.info(f"Retrieved grades for {len(students_with_grades)} students")
            return students_with_grades

        except Exception as e:
            logger.error(f"Error getting students with grades: {e}")
            return {}

    @classmethod
    def _generate_demo_correlation_data(cls, academic_year: int, num_students: int = 50) -> Dict[str, Any]:
        """
        Generate realistic demo correlation data for visualization purposes.

        This is a temporary solution while we resolve student ID mapping issues.
        The data simulates realistic patterns where higher time spent generally
        correlates with better grades, but with realistic variance.

        Args:
            academic_year: The academic year to generate data for
            num_students: Number of students to generate data for

        Returns:
            Dict with correlation data, statistics, and metadata
        """
        import random
        import numpy as np

        try:
            # Set seed for reproducible demo data
            random.seed(academic_year + 12345)
            np.random.seed(academic_year + 12345)

            # Generate realistic correlation data
            correlation_data = []

            for i in range(num_students):
                student_id = f"demo_{academic_year}_{i+1:03d}"

                # Generate minutes with realistic distribution (30 to 3000 minutes)
                # Most students have 300-1200 minutes (5-20 hours), some outliers
                if random.random() < 0.1:  # 10% outliers with high usage
                    total_minutes = random.uniform(1500, 3000)  # 25-50 hours
                elif random.random() < 0.2:  # 20% low usage
                    total_minutes = random.uniform(30, 300)  # 0.5-5 hours
                else:  # 70% normal usage
                    total_minutes = random.uniform(300, 1500)  # 5-25 hours

                # Generate grades with correlation to minutes but with realistic variance
                # Base grade influenced by time spent, but with noise
                base_grade_from_time = min(90, 40 + (total_minutes * 0.03))  # Minutes to grade conversion
                noise = random.normalvariate(0, 12)  # Grade variance
                average_grade = max(20, min(100, base_grade_from_time + noise))

                # Add some completely random cases (students who study a lot but still struggle, or vice versa)
                if random.random() < 0.05:  # 5% inverse correlation cases
                    average_grade = 100 - average_grade + 40  # Flip the relationship

                correlation_data.append({
                    'student_id': student_id,
                    'average_grade': round(average_grade, 2),
                    'grade_count': random.randint(3, 8),  # Number of exams taken
                    'total_time_spent_minutes': round(total_minutes, 2),
                    'active_days': random.randint(10, 60),  # Days active on platform
                    'average_daily_minutes': round(total_minutes / random.randint(15, 45), 2),
                    'course_count': random.randint(2, 6)  # Realistic course count (2-6 courses)
                })

            # Calculate correlation statistics
            grades = [item['average_grade'] for item in correlation_data]
            minutes = [item['total_time_spent_minutes'] for item in correlation_data]

            correlation_coefficient = float(np.corrcoef(grades, minutes)[0, 1])
            if np.isnan(correlation_coefficient):
                correlation_coefficient = 0.0

            statistics = {
                'correlation_coefficient': correlation_coefficient,
                'average_grade': sum(grades) / len(grades),
                'average_minutes': sum(minutes) / len(minutes),
                'grade_range': [min(grades), max(grades)],
                'minutes_range': [min(minutes), max(minutes)],
                'sample_size': len(correlation_data)
            }

            return {
                'correlation_data': correlation_data,
                'statistics': statistics,
                'metadata': {
                    'academic_year': academic_year,
                    'students_with_grades_only': num_students,
                    'students_with_time_data': num_students,
                    'total_data_points': num_students,
                    'method': 'demo_synthetic_data',
                    'is_demo': True,
                    'demo_note': 'This is synthetic demo data. Real correlation will be available once student ID mapping is resolved.'
                }
            }

        except Exception as e:
            logger.error(f"Error generating demo correlation data: {e}")
            return {
                'error': f'Error generating demo data: {str(e)}',
                'correlation_data': [],
                'statistics': {},
                'metadata': {
                    'academic_year': academic_year,
                    'students_with_grades_only': 0,
                    'students_with_time_data': 0,
                    'total_data_points': 0,
                    'is_demo': True
                }
            }


def extract_student_id_from_actor_account_name(actor_account_name: str) -> Optional[str]:
    """
    Extract student ID from actor_account_name field.

    Handles three formats:
    1. "1369@0122CF32-84AF-E55C-3CED-647BBC4F44A7" -> "1369"
    2. "Learner:2549" -> "2549"
    3. "2549" -> "2549"

    Args:
        actor_account_name (str): The actor account name from ClickHouse

    Returns:
        Optional[str]: The extracted student ID, or None if no valid ID found
    """
    if not actor_account_name or not isinstance(actor_account_name, str):
        return None

    # Remove whitespace
    actor_account_name = actor_account_name.strip()

    # Pattern 1: "1369@UUID" format
    if '@' in actor_account_name:
        student_id = actor_account_name.split('@')[0]
        if student_id.isdigit():
            return student_id

    # Pattern 2: "Learner:2549" format
    if actor_account_name.startswith('Learner:'):
        student_id = actor_account_name.replace('Learner:', '')
        if student_id.isdigit():
            return student_id

    # Pattern 3: Direct numeric ID "2549"
    if actor_account_name.isdigit():
        return actor_account_name

    # Log unrecognized format for debugging
    logger.debug(f"Unrecognized actor_account_name format: '{actor_account_name}'")
    return None


class PastYearLogAnalytics(CachedModelMixin, models.Model):
    """Model to get log analytics from ClickHouse databases with academic year support."""

    class Meta:
        managed = False
        app_label = 'clickhouse_app'

    @classmethod
    def get_log_counts_by_period(cls, view_type: str = 'month') -> Dict[str, Any]:
        """
        Get unique log counts by month or year from both ClickHouse databases with Redis caching.
        """
        cache_key = generate_cache_key('log_counts_by_period', view_type)

        def fetch_log_counts():
            return cls._fetch_log_counts_by_period(view_type)

        return cls.get_cached_data(
            cache_key,
            fetch_log_counts,
            ttl=CACHE_CONFIG['LOG_ANALYTICS_TTL']
        )

    @classmethod
    def _fetch_log_counts_by_period(cls, view_type: str = 'month') -> Dict[str, Any]:
        """Original implementation for fetching log counts"""
        logger.info(f"Fetching log counts by {view_type} from both ClickHouse databases")

        try:
            # Get current year to determine which databases to query
            current_year = datetime.datetime.now().year

            # Prepare result structure
            result = {
                'view_type': view_type,
                'data': [],
                'total_logs': 0,
                'date_range': {
                    'earliest': None,
                    'latest': None
                },
                'database_info': {
                    'pre_2025_logs': 0,
                    'post_2025_logs': 0
                }
            }

            # Query pre-2025 database
            pre_2025_data = cls._query_clickhouse_logs('clickhouse_db_pre_2025', view_type)

            # Query 2025+ database (only if current year >= 2025)
            post_2025_data = []
            if current_year >= 2025:
                post_2025_data = cls._query_clickhouse_logs('clickhouse_db', view_type)

            # Combine and process data
            all_data = pre_2025_data + post_2025_data

            if view_type == 'month':
                result['data'] = cls._process_monthly_data(all_data)
            else:  # year
                result['data'] = cls._process_yearly_data(all_data)

            # Calculate totals
            result['total_logs'] = sum(item['count'] for item in result['data'])
            result['database_info']['pre_2025_logs'] = sum(item['count'] for item in pre_2025_data)
            result['database_info']['post_2025_logs'] = sum(item['count'] for item in post_2025_data)

            # Set date range
            if result['data']:
                result['date_range']['earliest'] = result['data'][0]['period']
                result['date_range']['latest'] = result['data'][-1]['period']

            logger.info(f"Log counts by {view_type} completed: {result['total_logs']} total logs, {len(result['data'])} periods")

            return result

        except Exception as e:
            logger.error(f"Error fetching log counts by {view_type}: {str(e)}")
            return {
                'view_type': view_type,
                'data': [],
                'total_logs': 0,
                'date_range': {'earliest': None, 'latest': None},
                'database_info': {'pre_2025_logs': 0, 'post_2025_logs': 0},
                'error': str(e)
            }

    @classmethod
    def get_log_summary_stats(cls) -> Dict[str, Any]:
        """
        Get summary statistics for logs across both databases with Redis caching.
        """
        cache_key = generate_cache_key('log_summary_stats')

        def fetch_summary_stats():
            return cls._fetch_log_summary_stats()

        return cls.get_cached_data(
            cache_key,
            fetch_summary_stats,
            ttl=CACHE_CONFIG['LOG_ANALYTICS_TTL']
        )

    @classmethod
    def _fetch_log_summary_stats(cls) -> Dict[str, Any]:
        """Original implementation for fetching log summary stats"""
        logger.info("Fetching log summary statistics")

        try:
            stats = {
                'total_unique_logs': 0,
                'databases': {
                    'pre_2025': {'logs': 0, 'available': False},
                    'post_2025': {'logs': 0, 'available': False}
                },
                'date_ranges': {
                    'pre_2025': {'earliest': None, 'latest': None},
                    'post_2025': {'earliest': None, 'latest': None}
                }
            }

            # Query pre-2025 database
            try:
                with connections['clickhouse_db_pre_2025'].cursor() as cursor:
                    cursor.execute("""
                        SELECT
                            COUNT(DISTINCT _id) as total_logs,
                            MIN(timestamp) as earliest_date,
                            MAX(timestamp) as latest_date
                        FROM statements_mv
                        WHERE _id IS NOT NULL AND _id != ''
                        AND timestamp >= toDate('2018-01-01')
                    """)
                    row = cursor.fetchone()
                    if row:
                        stats['databases']['pre_2025']['logs'] = row[0]
                        stats['databases']['pre_2025']['available'] = True
                        stats['date_ranges']['pre_2025']['earliest'] = row[1]
                        stats['date_ranges']['pre_2025']['latest'] = row[2]

            except Exception as e:
                logger.warning(f"Could not query pre-2025 database: {str(e)}")

            # Query 2025+ database (only if current year >= 2025)
            current_year = datetime.datetime.now().year
            if current_year >= 2025:
                try:
                    with connections['clickhouse_db'].cursor() as cursor:
                        cursor.execute("""
                            SELECT
                                COUNT(DISTINCT _id) as total_logs,
                                MIN(timestamp) as earliest_date,
                                MAX(timestamp) as latest_date
                            FROM statements_mv
                            WHERE _id IS NOT NULL AND _id != ''
                            AND timestamp >= toDate('2018-01-01')
                        """)
                        row = cursor.fetchone()
                        if row:
                            stats['databases']['post_2025']['logs'] = row[0]
                            stats['databases']['post_2025']['available'] = True
                            stats['date_ranges']['post_2025']['earliest'] = row[1]
                            stats['date_ranges']['post_2025']['latest'] = row[2]

                except Exception as e:
                    logger.warning(f"Could not query 2025+ database: {str(e)}")

            # Calculate total
            stats['total_unique_logs'] = (
                stats['databases']['pre_2025']['logs'] +
                stats['databases']['post_2025']['logs']
            )

            logger.info(f"Log summary completed: {stats['total_unique_logs']} total unique logs")

            return stats

        except Exception as e:
            logger.error(f"Error fetching log summary stats: {str(e)}")
            return {
                'total_unique_logs': 0,
                'databases': {
                    'pre_2025': {'logs': 0, 'available': False},
                    'post_2025': {'logs': 0, 'available': False}
                },
                'date_ranges': {
                    'pre_2025': {'earliest': None, 'latest': None},
                    'post_2025': {'earliest': None, 'latest': None}
                },
                'error': str(e)
            }

    @classmethod
    def _query_clickhouse_logs(cls, db_alias: str, view_type: str) -> List[Dict[str, Any]]:
        """
        Query a specific ClickHouse database for log counts.

        Args:
            db_alias (str): Database alias ('clickhouse_db' or 'clickhouse_db_pre_2025')
            view_type (str): 'month' or 'year'

        Returns:
            List of dictionaries with period and count data
        """
        try:
            with connections[db_alias].cursor() as cursor:
                if view_type == 'month':
                    # Group by year-month with academic year consideration
                    query = """
                        SELECT
                            toYYYYMM(timestamp) as period,
                            COUNT(DISTINCT _id) as log_count
                        FROM statements_mv
                        WHERE _id IS NOT NULL
                        AND _id != ''
                        AND timestamp >= toDate('2018-01-01')
                        GROUP BY period
                        ORDER BY period
                    """
                else:  # year
                    # Group by academic year (April 1 - March 31)
                    query = """
                        SELECT
                            CASE
                                WHEN toMonth(timestamp) >= 4 THEN toYear(timestamp)
                                ELSE toYear(timestamp) - 1
                            END as academic_year,
                            COUNT(DISTINCT _id) as log_count
                        FROM statements_mv
                        WHERE _id IS NOT NULL
                        AND _id != ''
                        AND timestamp >= toDate('2018-01-01')
                        GROUP BY academic_year
                        HAVING academic_year >= 2018
                        ORDER BY academic_year
                    """

                logger.debug(f"Executing query on {db_alias}: {query}")
                cursor.execute(query)
                rows = cursor.fetchall()

                result = []
                for row in rows:
                    result.append({
                        'period': str(row[0]),
                        'count': row[1],
                        'database': db_alias
                    })

                logger.info(f"Retrieved {len(result)} records from {db_alias}")
                return result

        except Exception as e:
            logger.error(f"Error querying {db_alias} for {view_type} data: {str(e)}")
            return []

    @classmethod
    def _process_monthly_data(cls, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process monthly data and combine counts from both databases.

        Args:
            data: Raw data from both databases

        Returns:
            Processed monthly data with academic year context
        """
        # Group by period (YYYYMM format)
        monthly_counts = {}

        for item in data:
            period = item['period']
            count = item['count']

            if period in monthly_counts:
                monthly_counts[period] += count
            else:
                monthly_counts[period] = count

        # Convert to list and add academic year information
        result = []
        for period, count in sorted(monthly_counts.items()):
            # Convert YYYYMM to year and month
            year = int(period[:4])
            month = int(period[4:])

            # Calculate academic year (April 1 - March 31)
            if month >= 4:
                academic_year = year
            else:
                academic_year = year - 1

            # Format period for display
            period_display = f"{year}-{month:02d}"

            result.append({
                'period': period_display,
                'count': count,
                'academic_year': academic_year,
                'year': year,
                'month': month
            })

        return result

    @classmethod
    def _process_yearly_data(cls, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process yearly data (academic years) and combine counts from both databases.

        Args:
            data: Raw data from both databases

        Returns:
            Processed yearly data
        """
        # Group by academic year
        yearly_counts = {}

        for item in data:
            academic_year = int(item['period'])
            count = item['count']

            if academic_year in yearly_counts:
                yearly_counts[academic_year] += count
            else:
                yearly_counts[academic_year] = count

        # Convert to list
        result = []
        for academic_year, count in sorted(yearly_counts.items()):
            result.append({
                'period': str(academic_year),
                'count': count,
                'academic_year': academic_year,
                'period_display': f"{academic_year}年度"
            })

        return result

    @classmethod
    def get_log_summary_stats(cls) -> Dict[str, Any]:
        """
        Get summary statistics for logs across both databases with Redis caching.
        """
        cache_key = generate_cache_key('log_summary_stats')

        def fetch_summary_stats():
            return cls._fetch_log_summary_stats()

        return cls.get_cached_data(
            cache_key,
            fetch_summary_stats,
            ttl=CACHE_CONFIG['LOG_ANALYTICS_TTL']
        )

    @classmethod
    def _fetch_log_summary_stats(cls) -> Dict[str, Any]:
        """Original implementation for fetching log summary stats"""
        logger.info("Fetching log summary statistics")

        try:
            stats = {
                'total_unique_logs': 0,
                'databases': {
                    'pre_2025': {'logs': 0, 'available': False},
                    'post_2025': {'logs': 0, 'available': False}
                },
                'date_ranges': {
                    'pre_2025': {'earliest': None, 'latest': None},
                    'post_2025': {'earliest': None, 'latest': None}
                }
            }

            # Query pre-2025 database
            try:
                with connections['clickhouse_db_pre_2025'].cursor() as cursor:
                    cursor.execute("""
                        SELECT
                            COUNT(DISTINCT _id) as total_logs,
                            MIN(timestamp) as earliest_date,
                            MAX(timestamp) as latest_date
                        FROM statements_mv
                        WHERE _id IS NOT NULL AND _id != ''
                        AND timestamp >= toDate('2018-01-01')
                    """)
                    row = cursor.fetchone()
                    if row:
                        stats['databases']['pre_2025']['logs'] = row[0]
                        stats['databases']['pre_2025']['available'] = True
                        stats['date_ranges']['pre_2025']['earliest'] = row[1]
                        stats['date_ranges']['pre_2025']['latest'] = row[2]

            except Exception as e:
                logger.warning(f"Could not query pre-2025 database: {str(e)}")

            # Query 2025+ database (only if current year >= 2025)
            current_year = datetime.datetime.now().year
            if current_year >= 2025:
                try:
                    with connections['clickhouse_db'].cursor() as cursor:
                        cursor.execute("""
                            SELECT
                                COUNT(DISTINCT _id) as total_logs,
                                MIN(timestamp) as earliest_date,
                                MAX(timestamp) as latest_date
                            FROM statements_mv
                            WHERE _id IS NOT NULL AND _id != ''
                            AND timestamp >= toDate('2018-01-01')
                        """)
                        row = cursor.fetchone()
                        if row:
                            stats['databases']['post_2025']['logs'] = row[0]
                            stats['databases']['post_2025']['available'] = True
                            stats['date_ranges']['post_2025']['earliest'] = row[1]
                            stats['date_ranges']['post_2025']['latest'] = row[2]

                except Exception as e:
                    logger.warning(f"Could not query 2025+ database: {str(e)}")

            # Calculate total
            stats['total_unique_logs'] = (
                stats['databases']['pre_2025']['logs'] +
                stats['databases']['post_2025']['logs']
            )

            logger.info(f"Log summary completed: {stats['total_unique_logs']} total unique logs")

            return stats

        except Exception as e:
            logger.error(f"Error fetching log summary stats: {str(e)}")
            return {
                'total_unique_logs': 0,
                'databases': {
                    'pre_2025': {'logs': 0, 'available': False},
                    'post_2025': {'logs': 0, 'available': False}
                },
                'date_ranges': {
                    'pre_2025': {'earliest': None, 'latest': None},
                    'post_2025': {'earliest': None, 'latest': None}
                },
                'error': str(e)
            }


class PastYearGradeAnalytics(CachedModelMixin, models.Model):
    """Model to get grade performance analytics for top and bottom performing students."""

    class Meta:
        managed = False
        app_label = 'analysis_app'

    @classmethod
    def _get_valid_grade_filter_clause(cls) -> str:
        """
        Get reusable SQL clause for filtering valid grades (0-100).
        This method ensures consistency across all grade queries.

        Returns:
            str: SQL WHERE clause for valid grade filtering
        """
        return "AND quiz >= 0 AND quiz <= 100"

    @classmethod
    def _build_student_grade_query(cls, student_filter_placeholders: str, additional_where: str = "",
                                 group_by: str = "", having_clause: str = "", order_by: str = "") -> str:
        """
        Build a reusable student grade query with consistent filtering.

        Args:
            student_filter_placeholders (str): Placeholder string for student ID filtering
            additional_where (str): Additional WHERE conditions
            group_by (str): GROUP BY clause
            having_clause (str): HAVING clause
            order_by (str): ORDER BY clause

        Returns:
            str: Complete SQL query
        """
        base_query = f"""
            SELECT
                {{select_fields}}
            FROM course_student_scores
            WHERE quiz IS NOT NULL
            AND student_id IS NOT NULL
            AND student_id IN ({student_filter_placeholders})
            {cls._get_valid_grade_filter_clause()}
            AND created_at >= '2018-01-01'
            AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')
            {additional_where}
            {group_by}
            {having_clause}
            {order_by}
        """

        # DEBUG LOGGING
        logger.debug(f"🔍 GRADE QUERY DEBUG - _build_student_grade_query base template:")
        logger.debug(f"    Student filter placeholders count: {student_filter_placeholders.count('%s')}")
        logger.debug(f"    Additional WHERE: '{additional_where}'")
        logger.debug(f"    GROUP BY: '{group_by}'")
        logger.debug(f"    HAVING: '{having_clause}'")
        logger.debug(f"    ORDER BY: '{order_by}'")
        logger.debug(f"    Base query template: {base_query}")

        return base_query

    @classmethod
    def get_grade_performance_by_period(cls) -> Dict[str, Any]:
        """
        Get grade performance trends for top 25% and bottom 25% students by academic year with Redis caching.
        Only supports yearly academic year-based analysis.

        Returns:
            Dict containing performance data for both groups
        """
        cache_key = generate_cache_key('grade_performance_yearly')

        def fetch_grade_performance():
            return cls._fetch_grade_performance_yearly()

        return cls.get_cached_data(
            cache_key,
            fetch_grade_performance,
            ttl=CACHE_CONFIG['LOG_ANALYTICS_TTL']
        )

    @classmethod
    def get_grade_performance_normal_distribution(cls) -> Dict[str, Any]:
        """
        Get grade performance trends using normal distribution analysis (mean ± standard deviation).
        Provides statistical insights into grade distribution patterns by academic year.

        Returns:
            Dict containing normal distribution performance data
        """
        cache_key = generate_cache_key('grade_performance_normal_distribution')

        def fetch_normal_distribution_performance():
            return cls._fetch_grade_performance_normal_distribution()

        return cls.get_cached_data(
            cache_key,
            fetch_normal_distribution_performance,
            ttl=CACHE_CONFIG['LOG_ANALYTICS_TTL']
        )

    @classmethod
    def _fetch_grade_performance_yearly(cls) -> Dict[str, Any]:
        """Fetch yearly grade performance data for academic years only"""
        logger.info(f"🔍 STARTING YEARLY GRADE PERFORMANCE FETCH - Academic year-based analysis only")

        try:
            result = {
                'view_type': 'year',
                'top_25_data': [],
                'bottom_25_data': [],
                'total_students_analyzed': 0,
                'date_range': {
                    'earliest': None,
                    'latest': None
                },
                'performance_summary': {
                    'top_25_avg_grade': 0,
                    'bottom_25_avg_grade': 0,
                    'performance_gap': 0
                }
            }

            # Get all available academic years to determine student filtering
            available_years = PastYearCourseCategory.get_available_academic_years()
            logger.debug(f"🔍 AVAILABLE YEARS: {available_years}")

            if not available_years:
                logger.warning("❌ No academic years available for grade performance analysis")
                return result

            # Get performance data for each academic year
            logger.debug(f"🔍 CALLING yearly_performance_data with years: {available_years}")
            result['top_25_data'], result['bottom_25_data'] = cls._get_yearly_performance_data(available_years)

            logger.debug(f"🔍 PERFORMANCE DATA RECEIVED:")
            logger.debug(f"    Top 25% records: {len(result['top_25_data'])}")
            logger.debug(f"    Bottom 25% records: {len(result['bottom_25_data'])}")

            # Calculate summary statistics
            if result['top_25_data'] and result['bottom_25_data']:
                top_grades = [item['avg_grade'] for item in result['top_25_data'] if item['avg_grade'] > 0]
                bottom_grades = [item['avg_grade'] for item in result['bottom_25_data'] if item['avg_grade'] > 0]

                logger.debug(f"🔍 SUMMARY CALCULATION:")
                logger.debug(f"    Valid top grades: {len(top_grades)}")
                logger.debug(f"    Valid bottom grades: {len(bottom_grades)}")

                if top_grades and bottom_grades:
                    result['performance_summary']['top_25_avg_grade'] = round(sum(top_grades) / len(top_grades), 2)
                    result['performance_summary']['bottom_25_avg_grade'] = round(sum(bottom_grades) / len(bottom_grades), 2)
                    result['performance_summary']['performance_gap'] = round(
                        result['performance_summary']['top_25_avg_grade'] - result['performance_summary']['bottom_25_avg_grade'], 2
                    )

                # Set date range
                all_periods = [item['period'] for item in result['top_25_data'] + result['bottom_25_data']]
                if all_periods:
                    result['date_range']['earliest'] = min(all_periods)
                    result['date_range']['latest'] = max(all_periods)

                logger.debug(f"🔍 FINAL SUMMARY:")
                logger.debug(f"    Top 25% avg: {result['performance_summary']['top_25_avg_grade']}")
                logger.debug(f"    Bottom 25% avg: {result['performance_summary']['bottom_25_avg_grade']}")
                logger.debug(f"    Performance gap: {result['performance_summary']['performance_gap']}")
                logger.debug(f"    Date range: {result['date_range']['earliest']} to {result['date_range']['latest']}")
            else:
                logger.warning(f"❌ NO DATA FOUND for grade performance analysis:")
                logger.warning(f"    Top 25% data empty: {len(result['top_25_data']) == 0}")
                logger.warning(f"    Bottom 25% data empty: {len(result['bottom_25_data']) == 0}")

            logger.info(f"✅ Grade performance by year completed: {len(result['top_25_data'])} years analyzed")

            return result

        except Exception as e:
            logger.error(f"❌ Error fetching grade performance by year: {str(e)}")
            logger.error(f"Exception details:", exc_info=True)
            return {
                'view_type': 'year',
                'top_25_data': [],
                'bottom_25_data': [],
                'total_students_analyzed': 0,
                'date_range': {'earliest': None, 'latest': None},
                'performance_summary': {'top_25_avg_grade': 0, 'bottom_25_avg_grade': 0, 'performance_gap': 0},
                'error': str(e)
            }

    @classmethod
    def _get_yearly_performance_data(cls, available_years: List[int]) -> tuple:
        """Get yearly performance data using course name-based academic year detection - completely dynamic approach"""
        top_25_data = []
        bottom_25_data = []

        try:
            with connections['analysis_db'].cursor() as cursor:
                # STEP 1: Dynamically get academic years that have grade data
                logger.debug("🔍 DYNAMICALLY GETTING ACADEMIC YEARS FROM GRADE DATA...")

                cursor.execute("""
                    SELECT DISTINCT
                        SUBSTRING(course_name, 1, 4) as year_str,
                        COUNT(DISTINCT course_id) as course_count,
                        COUNT(DISTINCT student_id) as student_count,
                        COUNT(*) as grade_count
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND quiz >= 0 AND quiz <= 100
                    AND course_name LIKE '%年度%'
                    AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')
                    GROUP BY year_str
                    HAVING grade_count >= 50
                    ORDER BY year_str DESC
                """)

                grade_data_years = cursor.fetchall()

                # Convert to list of academic years that have data
                years_with_data = []
                for year_row in grade_data_years:
                    try:
                        year_int = int(year_row[0])
                        years_with_data.append({
                            'year': year_int,
                            'courses': year_row[1],
                            'students': year_row[2],
                            'grades': year_row[3]
                        })
                        logger.info(f"📊 Found academic year {year_int}: {year_row[1]} courses, {year_row[2]} students, {year_row[3]} grades")
                    except (ValueError, TypeError):
                        continue

                if not years_with_data:
                    logger.warning("❌ No academic years found with grade data")
                    return top_25_data, bottom_25_data

                logger.info(f"✅ Processing {len(years_with_data)} academic years with data: {[y['year'] for y in years_with_data]}")

                # STEP 2: Process each academic year that has grade data
                for year_info in years_with_data:
                    academic_year = year_info['year']
                    year_pattern = f"{academic_year}年度"

                    logger.debug(f"📈 Processing academic year {academic_year}...")

                    # Get ALL students who have grades in courses from this academic year
                    # NO student filtering by Moodle - use all students with grades in year-pattern courses
                    yearly_query = """
                        SELECT
                            student_id,
                            AVG(quiz) as avg_grade,
                            COUNT(*) as grade_count
                        FROM course_student_scores
                        WHERE quiz IS NOT NULL
                        AND quiz >= 0 AND quiz <= 100
                        AND course_name LIKE %s
                        AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')
                        GROUP BY student_id
                        ORDER BY avg_grade DESC
                    """

                    # Get course details for transparency
                    course_details_query = """
                        SELECT
                            course_id,
                            course_name,
                            COUNT(DISTINCT student_id) as students_count,
                            COUNT(*) as grades_count,
                            AVG(quiz) as avg_grade,
                            GROUP_CONCAT(DISTINCT name SEPARATOR ', ') as grade_file_names
                        FROM course_student_scores
                        WHERE quiz IS NOT NULL
                        AND quiz >= 0 AND quiz <= 100
                        AND course_name LIKE %s
                        AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')
                        GROUP BY course_id, course_name
                        HAVING students_count >= 3
                        ORDER BY students_count DESC
                    """

                    logger.debug(f"🔍 Getting all students with grades in {year_pattern} courses...")
                    cursor.execute(yearly_query, [f'%{year_pattern}%'])
                    student_results = cursor.fetchall()

                    # Get course details for transparency
                    cursor.execute(course_details_query, [f'%{year_pattern}%'])
                    course_results = cursor.fetchall()

                    logger.debug(f"Found {len(student_results)} students with grades in {year_pattern} courses")
                    logger.debug(f"Found {len(course_results)} courses for {year_pattern}")

                    if len(student_results) < 4:  # Need at least 4 students for percentiles
                        logger.debug(f"Insufficient students ({len(student_results)}) for {academic_year}")
                        continue

                    # Process course details for transparency
                    year_course_details = []
                    for course_row in course_results:
                        year_course_details.append({
                            'id': str(course_row[0]),
                            'name': course_row[1] if course_row[1] else f"Course {course_row[0]}",
                            'students_in_year': course_row[2],
                            'grades_in_year': course_row[3],
                            'avg_grade_in_year': round(float(course_row[4]), 2) if course_row[4] else 0,
                            'grade_file_names': course_row[5] if course_row[5] else 'No grade file name'
                        })

                    # Calculate top 25% and bottom 25%
                    total_students = len(student_results)
                    top_25_count = max(1, total_students // 4)
                    bottom_25_count = max(1, total_students // 4)

                    # Get top 25% students (already sorted by avg_grade DESC)
                    top_25_students = student_results[:top_25_count]
                    top_25_avg = sum(float(s[1]) for s in top_25_students) / len(top_25_students)

                    # Get bottom 25% students
                    bottom_25_students = student_results[-bottom_25_count:]
                    bottom_25_avg = sum(float(s[1]) for s in bottom_25_students) / len(bottom_25_students)

                    # Add to results
                    top_25_data.append({
                        'period': str(academic_year),
                        'avg_grade': round(top_25_avg, 2),
                        'student_count': len(top_25_students),
                        'academic_year': academic_year,
                        'period_display': f"{academic_year}年度",
                        'course_count': len(year_course_details),
                        'courses_used': year_course_details,
                        'total_students_analyzed': total_students,
                        'categorization_method': 'course_name_pattern_matching'
                    })

                    bottom_25_data.append({
                        'period': str(academic_year),
                        'avg_grade': round(bottom_25_avg, 2),
                        'student_count': len(bottom_25_students),
                        'academic_year': academic_year,
                        'period_display': f"{academic_year}年度",
                        'course_count': len(year_course_details),
                        'courses_used': year_course_details,
                        'total_students_analyzed': total_students,
                        'categorization_method': 'course_name_pattern_matching'
                    })

                    logger.info(f"✅ {academic_year}: Top 25% = {round(top_25_avg, 2)} ({len(top_25_students)} students), Bottom 25% = {round(bottom_25_avg, 2)} ({len(bottom_25_students)} students), Courses = {len(year_course_details)}")

                # Sort by academic year
                top_25_data.sort(key=lambda x: x['academic_year'])
                bottom_25_data.sort(key=lambda x: x['academic_year'])

                logger.info(f"🎉 Generated performance data for {len(top_25_data)} academic years")

        except Exception as e:
            logger.error(f"Error in yearly performance data generation: {str(e)}")
            logger.error(f"Exception details:", exc_info=True)

        return top_25_data, bottom_25_data

    @classmethod
    def get_grade_performance_summary_stats(cls) -> Dict[str, Any]:
        """
        Get summary statistics for grade performance trends with Redis caching.
        """
        cache_key = generate_cache_key('grade_performance_summary_stats')

        def fetch_summary_stats():
            return cls._fetch_grade_performance_summary_stats()

        return cls.get_cached_data(
            cache_key,
            fetch_summary_stats,
            ttl=CACHE_CONFIG['LOG_ANALYTICS_TTL']
        )

    @classmethod
    def _fetch_grade_performance_summary_stats(cls) -> Dict[str, Any]:
        """Fetch grade performance summary stats using dynamic course name-based year matching"""
        logger.info("Fetching grade performance summary statistics using dynamic course name-based year matching")

        try:
            stats = {
                'total_students_analyzed': 0,
                'total_grade_records': 0,
                'performance_metrics': {
                    'overall_avg_grade': 0,
                    'top_25_avg_grade': 0,
                    'bottom_25_avg_grade': 0,
                    'performance_gap': 0
                },
                'date_ranges': {
                    'earliest': None,
                    'latest': None
                },
                'academic_years_covered': [],
                'categorization_method': 'dynamic_course_name_pattern_matching'
            }

            with connections['analysis_db'].cursor() as cursor:
                # STEP 1: Dynamically get academic years that have grade data
                logger.debug("🔍 DYNAMICALLY GETTING ACADEMIC YEARS FOR SUMMARY STATS...")

                cursor.execute("""
                    SELECT DISTINCT
                        SUBSTRING(course_name, 1, 4) as year_str,
                        COUNT(DISTINCT course_id) as course_count,
                        COUNT(DISTINCT student_id) as student_count,
                        COUNT(*) as grade_count
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND quiz >= 0 AND quiz <= 100
                    AND course_name LIKE '%年度%'
                    AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')
                    GROUP BY year_str
                    HAVING grade_count >= 50
                    ORDER BY year_str DESC
                """)

                grade_data_years = cursor.fetchall()

                # Convert to list of academic years that have data
                years_with_data = []
                for year_row in grade_data_years:
                    try:
                        year_int = int(year_row[0])
                        years_with_data.append(year_int)
                        logger.debug(f"📊 Summary stats - found academic year {year_int}: {year_row[1]} courses, {year_row[2]} students, {year_row[3]} grades")
                    except (ValueError, TypeError):
                        continue

                if not years_with_data:
                    logger.warning("❌ No academic years found with grade data for summary stats")
                    return stats

                stats['academic_years_covered'] = years_with_data
                logger.info(f"✅ Summary stats processing {len(years_with_data)} academic years: {years_with_data}")

                # STEP 2: Get all grade data from courses with academic year patterns
                # Build pattern for all available years: "2022年度" OR "2023年度" OR "2024年度" etc.
                year_patterns = []
                for year in years_with_data:
                    year_patterns.append(f"%{year}年度%")

                # Create OR conditions for all year patterns
                pattern_conditions = " OR ".join(["course_name LIKE %s"] * len(year_patterns))

                # Get overall statistics using ALL students who have grades in courses with year patterns
                overall_query = f"""
                    SELECT
                        COUNT(DISTINCT student_id) as total_students,
                        COUNT(*) as total_records,
                        AVG(quiz) as overall_avg,
                        MIN(created_at) as earliest_date,
                        MAX(created_at) as latest_date
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND quiz >= 0 AND quiz <= 100
                    AND course_id IS NOT NULL
                    AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')
                    AND ({pattern_conditions})
                """

                logger.debug(f"🔍 SUMMARY STATS: Getting overall stats from all students with grades in year-pattern courses")
                cursor.execute(overall_query, year_patterns)
                overall_result = cursor.fetchone()

                if overall_result:
                    stats['total_students_analyzed'] = overall_result[0]
                    stats['total_grade_records'] = overall_result[1]
                    stats['performance_metrics']['overall_avg_grade'] = round(float(overall_result[2]), 2) if overall_result[2] else 0
                    stats['date_ranges']['earliest'] = overall_result[3]
                    stats['date_ranges']['latest'] = overall_result[4]

                # Get student averages for percentile calculation using ALL students in year-pattern courses
                student_avg_query = f"""
                    SELECT
                        student_id,
                        AVG(quiz) as avg_grade
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND quiz >= 0 AND quiz <= 100
                    AND course_id IS NOT NULL
                    AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')
                    AND ({pattern_conditions})
                    GROUP BY student_id
                    ORDER BY avg_grade DESC
                """

                # Debug logging: show both template and complete query
                logger.debug(f"🔍 SUMMARY STATS: Student average query template: {student_avg_query}")
                logger.debug(f"🔍 SUMMARY STATS: Query parameters: {year_patterns}")

                # Create complete query for debugging (substitute parameters)
                complete_query = student_avg_query
                for i, pattern in enumerate(year_patterns):
                    complete_query = complete_query.replace("%s", f"'{pattern}'", 1)
                logger.debug(f"🔍 SUMMARY STATS: Complete substituted query: {complete_query}")

                logger.debug(f"🔍 SUMMARY STATS: Getting student averages from all students in year-pattern courses")
                # Execute the original parameterized query for safety (avoid SQL injection)
                cursor.execute(student_avg_query, year_patterns)
                student_averages = cursor.fetchall()

                if len(student_averages) >= 4:
                    # Calculate top 25% and bottom 25%
                    total_students = len(student_averages)
                    top_25_count = max(1, total_students // 4)
                    bottom_25_count = max(1, total_students // 4)

                    top_25_students = student_averages[:top_25_count]
                    bottom_25_students = student_averages[-bottom_25_count:]

                    top_25_avg = sum(float(s[1]) for s in top_25_students) / len(top_25_students)
                    bottom_25_avg = sum(float(s[1]) for s in bottom_25_students) / len(bottom_25_students)

                    stats['performance_metrics']['top_25_avg_grade'] = round(top_25_avg, 2)
                    stats['performance_metrics']['bottom_25_avg_grade'] = round(bottom_25_avg, 2)
                    stats['performance_metrics']['performance_gap'] = round(top_25_avg - bottom_25_avg, 2)

                    logger.debug(f"SUMMARY STATS: Performance metrics calculated from {len(student_averages)} students using dynamic year patterns")

            logger.info(f"Grade performance summary completed using DYNAMIC YEAR PATTERNS: {stats['total_students_analyzed']} students, {stats['total_grade_records']} grade records across {len(years_with_data)} academic years")

            return stats

        except Exception as e:
            logger.error(f"Error fetching grade performance summary stats: {str(e)}")
            return {
                'total_students_analyzed': 0,
                'total_grade_records': 0,
                'performance_metrics': {'overall_avg_grade': 0, 'top_25_avg_grade': 0, 'bottom_25_avg_grade': 0, 'performance_gap': 0},
                'date_ranges': {'earliest': None, 'latest': None},
                'academic_years_covered': [],
                'categorization_method': 'dynamic_course_name_pattern_matching',
                'error': str(e)
            }

    @classmethod
    def _fetch_grade_performance_normal_distribution(cls) -> Dict[str, Any]:
        """Fetch normal distribution grade performance data for academic years"""
        logger.info(f"🔍 STARTING NORMAL DISTRIBUTION GRADE PERFORMANCE FETCH - Statistical analysis")

        try:
            result = {
                'view_type': 'normal_distribution',
                'high_performers_data': [],
                'low_performers_data': [],
                'distribution_stats': [],
                'total_students_analyzed': 0,
                'date_range': {
                    'earliest': None,
                    'latest': None
                },
                'performance_summary': {
                    'avg_mean_grade': 0,
                    'avg_std_deviation': 0,
                    'high_performers_avg_grade': 0,
                    'low_performers_avg_grade': 0,
                    'statistical_gap': 0
                }
            }

            # Get all available academic years to determine student filtering
            available_years = PastYearCourseCategory.get_available_academic_years()
            logger.debug(f"🔍 AVAILABLE YEARS FOR NORMAL DISTRIBUTION: {available_years}")

            if not available_years:
                logger.warning("❌ No academic years available for normal distribution analysis")
                return result

            # Get normal distribution performance data for each academic year
            logger.debug(f"🔍 CALLING normal distribution analysis with years: {available_years}")
            (result['high_performers_data'],
             result['low_performers_data'],
             result['distribution_stats']) = cls._get_normal_distribution_performance_data(available_years)

            logger.debug(f"🔍 NORMAL DISTRIBUTION DATA RECEIVED:")
            logger.debug(f"    High performers records: {len(result['high_performers_data'])}")
            logger.debug(f"    Low performers records: {len(result['low_performers_data'])}")
            logger.debug(f"    Distribution stats records: {len(result['distribution_stats'])}")

            # Calculate summary statistics
            if result['high_performers_data'] and result['low_performers_data'] and result['distribution_stats']:
                high_grades = [item['avg_grade'] for item in result['high_performers_data'] if item['avg_grade'] > 0]
                low_grades = [item['avg_grade'] for item in result['low_performers_data'] if item['avg_grade'] > 0]
                mean_grades = [item['mean_grade'] for item in result['distribution_stats'] if item['mean_grade'] > 0]
                std_deviations = [item['std_deviation'] for item in result['distribution_stats'] if item['std_deviation'] > 0]

                logger.debug(f"🔍 NORMAL DISTRIBUTION SUMMARY CALCULATION:")
                logger.debug(f"    Valid high performer grades: {len(high_grades)}")
                logger.debug(f"    Valid low performer grades: {len(low_grades)}")
                logger.debug(f"    Valid mean grades: {len(mean_grades)}")
                logger.debug(f"    Valid std deviations: {len(std_deviations)}")

                if high_grades and low_grades and mean_grades and std_deviations:
                    result['performance_summary']['high_performers_avg_grade'] = round(sum(high_grades) / len(high_grades), 2)
                    result['performance_summary']['low_performers_avg_grade'] = round(sum(low_grades) / len(low_grades), 2)
                    result['performance_summary']['avg_mean_grade'] = round(sum(mean_grades) / len(mean_grades), 2)
                    result['performance_summary']['avg_std_deviation'] = round(sum(std_deviations) / len(std_deviations), 2)
                    result['performance_summary']['statistical_gap'] = round(
                        result['performance_summary']['high_performers_avg_grade'] - result['performance_summary']['low_performers_avg_grade'], 2
                    )

                # Set date range
                all_periods = [item['period'] for item in result['high_performers_data'] + result['low_performers_data']]
                if all_periods:
                    result['date_range']['earliest'] = min(all_periods)
                    result['date_range']['latest'] = max(all_periods)

                logger.debug(f"🔍 NORMAL DISTRIBUTION FINAL SUMMARY:")
                logger.debug(f"    High performers avg: {result['performance_summary']['high_performers_avg_grade']}")
                logger.debug(f"    Low performers avg: {result['performance_summary']['low_performers_avg_grade']}")
                logger.debug(f"    Average mean grade: {result['performance_summary']['avg_mean_grade']}")
                logger.debug(f"    Average std deviation: {result['performance_summary']['avg_std_deviation']}")
                logger.debug(f"    Statistical gap: {result['performance_summary']['statistical_gap']}")
            else:
                logger.warning(f"❌ NO DATA FOUND for normal distribution analysis:")
                logger.warning(f"    High performers data empty: {len(result['high_performers_data']) == 0}")
                logger.warning(f"    Low performers data empty: {len(result['low_performers_data']) == 0}")
                logger.warning(f"    Distribution stats empty: {len(result['distribution_stats']) == 0}")

            logger.info(f"✅ Normal distribution analysis completed: {len(result['high_performers_data'])} years analyzed")

            return result

        except Exception as e:
            logger.error(f"❌ Error fetching normal distribution performance: {str(e)}")
            logger.error(f"Exception details:", exc_info=True)
            return {
                'view_type': 'normal_distribution',
                'high_performers_data': [],
                'low_performers_data': [],
                'distribution_stats': [],
                'total_students_analyzed': 0,
                'date_range': {'earliest': None, 'latest': None},
                'performance_summary': {
                    'avg_mean_grade': 0, 'avg_std_deviation': 0,
                    'high_performers_avg_grade': 0, 'low_performers_avg_grade': 0, 'statistical_gap': 0
                },
                'error': str(e)
            }

    @classmethod
    def _get_normal_distribution_performance_data(cls, available_years: List[int]) -> tuple:
        """Get normal distribution performance data using statistical thresholds (mean ± 0.5 * std_dev)"""
        high_performers_data = []
        low_performers_data = []
        distribution_stats_data = []

        try:
            with connections['analysis_db'].cursor() as cursor:
                # STEP 1: Dynamically get academic years that have grade data
                logger.debug("🔍 NORMAL DISTRIBUTION: Getting academic years from grade data...")

                cursor.execute("""
                    SELECT DISTINCT
                        SUBSTRING(course_name, 1, 4) as year_str,
                        COUNT(DISTINCT course_id) as course_count,
                        COUNT(DISTINCT student_id) as student_count,
                        COUNT(*) as grade_count
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND quiz >= 0 AND quiz <= 100
                    AND course_name LIKE '%年度%'
                    AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')
                    GROUP BY year_str
                    HAVING grade_count >= 50
                    ORDER BY year_str DESC
                """)

                grade_data_years = cursor.fetchall()

                # Convert to list of academic years that have data
                years_with_data = []
                for year_row in grade_data_years:
                    try:
                        year_int = int(year_row[0])
                        years_with_data.append({
                            'year': year_int,
                            'courses': year_row[1],
                            'students': year_row[2],
                            'grades': year_row[3]
                        })
                        logger.info(f"📊 NORMAL DISTRIBUTION: Found academic year {year_int}: {year_row[1]} courses, {year_row[2]} students, {year_row[3]} grades")
                    except (ValueError, TypeError):
                        continue

                if not years_with_data:
                    logger.warning("❌ NORMAL DISTRIBUTION: No academic years found with grade data")
                    return high_performers_data, low_performers_data, distribution_stats_data

                logger.info(f"✅ NORMAL DISTRIBUTION: Processing {len(years_with_data)} academic years: {[y['year'] for y in years_with_data]}")

                # STEP 2: Process each academic year with normal distribution analysis
                for year_info in years_with_data:
                    academic_year = year_info['year']
                    year_pattern = f"{academic_year}年度"

                    logger.debug(f"📈 NORMAL DISTRIBUTION: Processing academic year {academic_year}...")

                    # Get ALL students who have grades in courses from this academic year
                    yearly_query = """
                        SELECT
                            student_id,
                            AVG(quiz) as avg_grade,
                            COUNT(*) as grade_count
                        FROM course_student_scores
                        WHERE quiz IS NOT NULL
                        AND quiz >= 0 AND quiz <= 100
                        AND course_name LIKE %s
                        AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')
                        GROUP BY student_id
                        ORDER BY avg_grade DESC
                    """

                    # Get course details for transparency
                    course_details_query = """
                        SELECT
                            course_id,
                            course_name,
                            COUNT(DISTINCT student_id) as students_count,
                            COUNT(*) as grades_count,
                            AVG(quiz) as avg_grade
                        FROM course_student_scores
                        WHERE quiz IS NOT NULL
                        AND quiz >= 0 AND quiz <= 100
                        AND course_name LIKE %s
                        AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')
                        GROUP BY course_id, course_name
                        HAVING students_count >= 3
                        ORDER BY students_count DESC
                    """

                    logger.debug(f"🔍 NORMAL DISTRIBUTION: Getting all students with grades in {year_pattern} courses...")
                    cursor.execute(yearly_query, [f'%{year_pattern}%'])
                    student_results = cursor.fetchall()

                    # Get course details for transparency
                    cursor.execute(course_details_query, [f'%{year_pattern}%'])
                    course_results = cursor.fetchall()

                    logger.debug(f"NORMAL DISTRIBUTION: Found {len(student_results)} students with grades in {year_pattern} courses")
                    logger.debug(f"NORMAL DISTRIBUTION: Found {len(course_results)} courses for {year_pattern}")

                    if len(student_results) < 10:  # Need at least 10 students for meaningful statistical analysis
                        logger.debug(f"NORMAL DISTRIBUTION: Insufficient students ({len(student_results)}) for {academic_year}")
                        continue

                    # Process course details for transparency
                    year_course_details = []
                    for course_row in course_results:
                        year_course_details.append({
                            'id': str(course_row[0]),
                            'name': course_row[1] if course_row[1] else f"Course {course_row[0]}",
                            'students_in_year': course_row[2],
                            'grades_in_year': course_row[3],
                            'avg_grade_in_year': round(float(course_row[4]), 2) if course_row[4] else 0
                        })

                    # NORMAL DISTRIBUTION CALCULATION
                    grades = [float(s[1]) for s in student_results]
                    total_students = len(student_results)

                    # Calculate mean and standard deviation
                    import statistics
                    mean_grade = statistics.mean(grades)
                    std_deviation = statistics.stdev(grades) if len(grades) > 1 else 0

                    # Define thresholds: mean ± 0.5 * standard deviation
                    high_threshold = mean_grade + 0.5 * std_deviation
                    low_threshold = mean_grade - 0.5 * std_deviation

                    # Categorize students based on statistical thresholds
                    high_performers = [s for s in student_results if float(s[1]) >= high_threshold]
                    low_performers = [s for s in student_results if float(s[1]) <= low_threshold]
                    middle_performers = [s for s in student_results if low_threshold < float(s[1]) < high_threshold]

                    # Calculate averages for each group
                    high_performers_avg = statistics.mean([float(s[1]) for s in high_performers]) if high_performers else mean_grade
                    low_performers_avg = statistics.mean([float(s[1]) for s in low_performers]) if low_performers else mean_grade

                    # Calculate percentages
                    high_performers_percentage = (len(high_performers) / total_students) * 100 if total_students > 0 else 0
                    low_performers_percentage = (len(low_performers) / total_students) * 100 if total_students > 0 else 0
                    middle_performers_percentage = (len(middle_performers) / total_students) * 100 if total_students > 0 else 0

                    # Add to results
                    high_performers_data.append({
                        'period': str(academic_year),
                        'avg_grade': round(high_performers_avg, 2),
                        'student_count': len(high_performers),
                        'percentage_of_total': round(high_performers_percentage, 1),
                        'threshold_used': round(high_threshold, 2),
                        'academic_year': academic_year,
                        'period_display': f"{academic_year}年度",
                        'course_count': len(year_course_details),
                        'courses_used': year_course_details,
                        'total_students_analyzed': total_students,
                        'categorization_method': 'normal_distribution_statistical'
                    })

                    low_performers_data.append({
                        'period': str(academic_year),
                        'avg_grade': round(low_performers_avg, 2),
                        'student_count': len(low_performers),
                        'percentage_of_total': round(low_performers_percentage, 1),
                        'threshold_used': round(low_threshold, 2),
                        'academic_year': academic_year,
                        'period_display': f"{academic_year}年度",
                        'course_count': len(year_course_details),
                        'courses_used': year_course_details,
                        'total_students_analyzed': total_students,
                        'categorization_method': 'normal_distribution_statistical'
                    })

                    # Distribution statistics for this year
                    distribution_stats_data.append({
                        'period': str(academic_year),
                        'academic_year': academic_year,
                        'period_display': f"{academic_year}年度",
                        'mean_grade': round(mean_grade, 2),
                        'std_deviation': round(std_deviation, 2),
                        'high_threshold': round(high_threshold, 2),
                        'low_threshold': round(low_threshold, 2),
                        'high_performers_count': len(high_performers),
                        'low_performers_count': len(low_performers),
                        'middle_performers_count': len(middle_performers),
                        'high_performers_percentage': round(high_performers_percentage, 1),
                        'low_performers_percentage': round(low_performers_percentage, 1),
                        'middle_performers_percentage': round(middle_performers_percentage, 1),
                        'total_students': total_students,
                        'coefficient_of_variation': round((std_deviation / mean_grade) * 100, 2) if mean_grade > 0 else 0
                    })

                    logger.info(f"✅ NORMAL DISTRIBUTION {academic_year}: High performers = {round(high_performers_avg, 2)} ({len(high_performers)} students, {round(high_performers_percentage, 1)}%), Low performers = {round(low_performers_avg, 2)} ({len(low_performers)} students, {round(low_performers_percentage, 1)}%), Mean = {round(mean_grade, 2)}, SD = {round(std_deviation, 2)}")

                # Sort by academic year
                high_performers_data.sort(key=lambda x: x['academic_year'])
                low_performers_data.sort(key=lambda x: x['academic_year'])
                distribution_stats_data.sort(key=lambda x: x['academic_year'])

                logger.info(f"🎉 NORMAL DISTRIBUTION: Generated data for {len(high_performers_data)} academic years")

        except Exception as e:
            logger.error(f"NORMAL DISTRIBUTION: Error in yearly performance data generation: {str(e)}")
            logger.error(f"Exception details:", exc_info=True)

        return high_performers_data, low_performers_data, distribution_stats_data

    @classmethod
    def debug_check_name_column_values(cls) -> Dict[str, Any]:
        """
        Debug method to check what values are in the name column
        """
        try:
            with connections['analysis_db'].cursor() as cursor:
                # Check what values exist in the name column
                debug_query = """
                    SELECT DISTINCT name, COUNT(*) as count
                    FROM course_student_scores
                    WHERE name IS NOT NULL AND name != ''
                    GROUP BY name
                    ORDER BY count DESC
                    LIMIT 20
                """
                logger.debug(f"🔍 DEBUG NAME COLUMN QUERY: {debug_query}")
                cursor.execute(debug_query)
                results = cursor.fetchall()
                logger.debug(f"🔍 NAME COLUMN VALUES FOUND: {len(results)} distinct values")
                for row in results:
                    logger.debug(f"🔍   Name: '{row[0]}' - Count: {row[1]}")
                # Also check for any Benesse-like values
                benesse_query = """
                    SELECT DISTINCT name, COUNT(*) as count
                    FROM course_student_scores
                    WHERE name IS NOT NULL
                    AND (name LIKE '%benesse%' OR name LIKE '%Benesse%' OR name LIKE '%BENESSE%' OR name LIKE '%ベネッセ%')
                    GROUP BY name
                    ORDER BY count DESC
                """
                logger.debug(f"🔍 BENESSE SEARCH QUERY: {benesse_query}")
                cursor.execute(benesse_query)
                benesse_results = cursor.fetchall()
                logger.debug(f"🔍 BENESSE-LIKE VALUES FOUND: {len(benesse_results)} values")
                for row in benesse_results:
                    logger.debug(f"🔍   Benesse Name: '{row[0]}' - Count: {row[1]}")
                return {
                    'all_names': results,
                    'benesse_names': benesse_results,
                    'total_distinct_names': len(results)
                }
        except Exception as e:
            logger.error(f"Error in debug_check_name_column_values: {str(e)}")
            return {'error': str(e)}

    @classmethod
    def get_time_spent_vs_grade_correlation(cls, academic_year: int) -> Dict[str, Any]:
        """
        Get time spent vs grade correlation data for a specific academic year with Redis caching.

        This method provides correlation analysis between student time spent on platform
        and their academic performance (grades) for visualization in scatter plots.

        Args:
            academic_year (int): The academic year to analyze (e.g., 2024 for 2024年度)

        Returns:
            Dict containing correlation data, statistics, and metadata
        """
        cache_key = generate_cache_key('time_spent_grade_correlation', academic_year)

        def fetch_correlation_data():
            return PastYearStudentGrades.get_time_spent_vs_grade_correlation(academic_year)

        return cls.get_cached_data(
            cache_key,
            fetch_correlation_data,
            ttl=CACHE_CONFIG['DEFAULT_TTL']
        )
