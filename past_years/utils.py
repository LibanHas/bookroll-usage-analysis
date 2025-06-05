import logging
import datetime
from typing import Dict, List, Any, Optional
from django.db import connections
from django.core.cache import cache

logger = logging.getLogger(__name__)


def get_course_grades_by_year(academic_year: int = None, cache_timeout: int = 7200) -> Dict[str, Any]:
    """
    Get average course grades per course for a specific academic year.

    This function queries the analysis_db's course_student_scores table to calculate
    average grades for each course within a specific academic year.

    Args:
        academic_year (int): The academic year to analyze (e.g., 2024)
        cache_timeout (int): Cache timeout in seconds (default: 2 hours)

    Returns:
        Dict containing:
        - courses: List of course data with average grades
        - summary_stats: Overall statistics
        - metadata: Configuration and cache info
    """
    if academic_year is None:
        academic_year = datetime.datetime.now().year

    # Create cache key
    cache_key = f'course_grades_{academic_year}'
    cached_data = cache.get(cache_key)

    if cached_data:
        logger.info(f"Returning cached course grades data for year {academic_year}")
        return cached_data

    logger.info(f"Calculating course grades for academic year {academic_year}")

    try:
        # Get course IDs for the academic year (courses with year pattern in name)
        year_pattern = f"{academic_year}年度"

        with connections['analysis_db'].cursor() as cursor:
            # First, get all courses that match the academic year pattern
            course_query = """
                SELECT DISTINCT course_id, course_name
                FROM course_student_scores
                WHERE quiz IS NOT NULL
                AND quiz >= 0 AND quiz <= 100
                AND (name LIKE %s OR name LIKE %s)
                AND course_name LIKE %s
                AND course_id IS NOT NULL
                AND student_id IS NOT NULL
                ORDER BY course_name
            """

            cursor.execute(course_query, ['%Benesse%', '%ベネッセ%', f'%{year_pattern}%'])
            year_courses = cursor.fetchall()

            if not year_courses:
                logger.warning(f"No courses found for academic year {academic_year}")
                return {
                    'courses': [],
                    'summary_stats': {
                        'total_courses': 0,
                        'total_students': 0,
                        'total_grades': 0,
                        'overall_average': 0
                    },
                    'metadata': {
                        'academic_year': academic_year,
                        'year_pattern': year_pattern,
                        'cache_timeout': cache_timeout
                    }
                }

            logger.info(f"Found {len(year_courses)} courses for {year_pattern}")

            # Get detailed grade statistics for each course
            course_data = []
            total_students = set()
            total_grades = 0
            overall_grade_sum = 0

            for course_id, course_name in year_courses:
                # Get grade statistics for this specific course
                course_stats_query = """
                    SELECT
                        COUNT(DISTINCT student_id) as student_count,
                        COUNT(*) as grade_count,
                        AVG(quiz) as avg_grade,
                        MIN(quiz) as min_grade,
                        MAX(quiz) as max_grade,
                        STDDEV(quiz) as std_deviation,
                        GROUP_CONCAT(DISTINCT name ORDER BY name SEPARATOR ', ') as grade_file_names
                    FROM course_student_scores
                    WHERE course_id = %s
                    AND quiz IS NOT NULL
                    AND quiz >= 0 AND quiz <= 100
                    AND (name LIKE %s OR name LIKE %s)
                    GROUP BY course_id, course_name
                """

                cursor.execute(course_stats_query, [course_id, '%Benesse%', '%ベネッセ%'])
                stats_result = cursor.fetchone()

                if stats_result:
                    student_count, grade_count, avg_grade, min_grade, max_grade, std_dev, grade_files = stats_result

                    # Get unique student IDs for this course for overall count
                    student_ids_query = """
                        SELECT DISTINCT student_id
                        FROM course_student_scores
                        WHERE course_id = %s
                        AND quiz IS NOT NULL
                        AND quiz >= 0 AND quiz <= 100
                        AND (name LIKE %s OR name LIKE %s)
                    """

                    cursor.execute(student_ids_query, [course_id, '%Benesse%', '%ベネッセ%'])
                    course_student_ids = [row[0] for row in cursor.fetchall()]
                    total_students.update(course_student_ids)

                    course_info = {
                        'course_id': course_id,
                        'course_name': course_name,
                        'student_count': student_count,
                        'grade_count': grade_count,
                        'avg_grade': round(float(avg_grade), 2) if avg_grade else 0,
                        'min_grade': round(float(min_grade), 2) if min_grade else 0,
                        'max_grade': round(float(max_grade), 2) if max_grade else 0,
                        'std_deviation': round(float(std_dev), 2) if std_dev else 0,
                        'grade_file_names': grade_files if grade_files else 'No grade file names'
                    }

                    course_data.append(course_info)
                    total_grades += grade_count
                    overall_grade_sum += float(avg_grade) * grade_count if avg_grade else 0

            # Sort courses by name alphabetically
            course_data.sort(key=lambda x: x['course_name'])

            # Calculate overall statistics
            overall_average = round(overall_grade_sum / total_grades, 2) if total_grades > 0 else 0

            result = {
                'courses': course_data,
                'summary_stats': {
                    'total_courses': len(course_data),
                    'total_students': len(total_students),
                    'total_grades': total_grades,
                    'overall_average': overall_average
                },
                'metadata': {
                    'academic_year': academic_year,
                    'year_pattern': year_pattern,
                    'cache_timeout': cache_timeout,
                    'generated_at': datetime.datetime.now().isoformat()
                }
            }

            # Cache the result
            cache.set(cache_key, result, cache_timeout)
            logger.info(f"Cached course grades data for {len(course_data)} courses in {academic_year}")

            return result

    except Exception as e:
        logger.error(f"Error calculating course grades for academic year {academic_year}: {str(e)}")
        return {
            'courses': [],
            'summary_stats': {
                'total_courses': 0,
                'total_students': 0,
                'total_grades': 0,
                'overall_average': 0
            },
            'metadata': {
                'academic_year': academic_year,
                'year_pattern': f"{academic_year}年度",
                'cache_timeout': cache_timeout,
                'error': str(e)
            }
        }


def get_available_academic_years_for_courses(start_year: int = 2018, end_year: int = None) -> List[int]:
    """
    Get list of academic years that have course grade data available.

    Args:
        start_year (int): Starting year to search from (default: 2018)
        end_year (int): Ending year to search to (default: current year)

    Returns:
        List of academic years with available course data
    """
    if end_year is None:
        end_year = datetime.datetime.now().year

    cache_key = f'available_course_years_{start_year}_{end_year}'
    cached_years = cache.get(cache_key)

    if cached_years:
        return cached_years

    available_years = []

    try:
        with connections['analysis_db'].cursor() as cursor:
            # Check each year for courses with grade data
            for year in range(start_year, end_year + 1):
                year_pattern = f"{year}年度"

                year_check_query = """
                    SELECT COUNT(DISTINCT course_id) as course_count
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND quiz >= 0 AND quiz <= 100
                    AND (name LIKE %s OR name LIKE %s)
                    AND course_name LIKE %s
                    AND course_id IS NOT NULL
                    AND student_id IS NOT NULL
                """

                cursor.execute(year_check_query, ['%Benesse%', '%ベネッセ%', f'%{year_pattern}%'])
                course_count = cursor.fetchone()[0]

                if course_count > 0:
                    available_years.append(year)
                    logger.debug(f"Found {course_count} courses for academic year {year}")

        # Cache for 1 hour
        cache.set(cache_key, available_years, 3600)
        logger.info(f"Found {len(available_years)} academic years with course data: {available_years}")

    except Exception as e:
        logger.error(f"Error getting available academic years: {str(e)}")

    return available_years


def clear_course_grades_cache(academic_year: int = None) -> bool:
    """
    Clear course grades cache.

    Args:
        academic_year (int): If provided, clear cache for specific year

    Returns:
        bool: True if cache was cleared successfully
    """
    try:
        if academic_year:
            cache_key = f'course_grades_{academic_year}'
            cache.delete(cache_key)
            logger.info(f"Cleared course grades cache for year {academic_year}")
        else:
            # Clear all course grades caches
            cache.delete_pattern('course_grades_*')
            cache.delete_pattern('available_course_years_*')
            logger.info("Cleared all course grades caches")
        return True
    except Exception as e:
        logger.error(f"Error clearing course grades cache: {str(e)}")
        return False