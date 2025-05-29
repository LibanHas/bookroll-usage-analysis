from django.db import models, connections
from django.core.cache import cache
import datetime
import logging
import re
from typing import Dict, List, Any, Optional
import statistics

logger = logging.getLogger(__name__)


class PastYearCourseCategory(models.Model):
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
        Get all courses for a specific academic year.
        Only relies on parent category names containing the academic year.
        All courses under matching parent categories are included regardless of course dates.

        Args:
            academic_year (int): The academic year (e.g., 2024 for 2024年度)

        Returns:
            Dict containing categorized courses for the academic year
        """
        cache_key = f'past_year_courses_{academic_year}'

        cached_data = cache.get(cache_key)

        if cached_data:
            logger.info(f"Using cached course data for academic year {academic_year} - {cached_data.get('total_courses', 0)} courses")
            logger.info(f"CACHE DEBUG: Cache key '{cache_key}' found with data: {type(cached_data)}")
            logger.info(f"CACHE DEBUG: Cached categories count: {len(cached_data.get('categories', {}))}")

            # Let's also check if we should bypass cache for debugging
            logger.info(f"CACHE DEBUG: To bypass cache, delete key '{cache_key}' or set BYPASS_CACHE=True")
            return cached_data

        logger.info(f"Fetching course data for academic year {academic_year} - NO CACHE FOUND")
        logger.info(f"CACHE DEBUG: Cache key '{cache_key}' not found, executing fresh query")

        try:
            with connections['moodle_db'].cursor() as cursor:
                # First, let's debug what's in the database
                debug_total_query = """
                    SELECT COUNT(*) as total_courses
                    FROM mdl_course course
                """
                cursor.execute(debug_total_query)
                total_courses_in_db = cursor.fetchone()[0]
                logger.info(f"SQL DEBUG: Total courses in database (all): {total_courses_in_db}")

                debug_visible_query = """
                    SELECT
                        COUNT(CASE WHEN visible = 1 THEN 1 END) as visible_courses,
                        COUNT(CASE WHEN visible = 0 THEN 1 END) as hidden_courses
                    FROM mdl_course course
                """
                cursor.execute(debug_visible_query)
                visibility_stats = cursor.fetchone()
                logger.info(f"SQL DEBUG: Visible courses: {visibility_stats[0]}, Hidden courses: {visibility_stats[1]}")

                # Check total categories
                debug_categories_query = """
                    SELECT
                        COUNT(*) as total_categories,
                        COUNT(CASE WHEN parent = 0 THEN 1 END) as parent_categories,
                        COUNT(CASE WHEN parent != 0 THEN 1 END) as child_categories
                    FROM mdl_course_categories
                """
                cursor.execute(debug_categories_query)
                cat_stats = cursor.fetchone()
                logger.info(f"SQL DEBUG: Categories - Total: {cat_stats[0]}, Parent: {cat_stats[1]}, Child: {cat_stats[2]}")

                # Check what parent categories exist
                parent_cat_query = """
                    SELECT id, name, sortorder
                    FROM mdl_course_categories
                    WHERE parent = 0
                    ORDER BY sortorder
                """
                cursor.execute(parent_cat_query)
                parent_categories = cursor.fetchall()
                logger.info(f"SQL DEBUG: Found {len(parent_categories)} parent categories:")
                for cat in parent_categories:
                    year = cls.get_academic_year_from_category_name(cat[1])
                    logger.info(f"SQL DEBUG: Parent Category - ID: {cat[0]}, Name: '{cat[1]}', Year: {year}")

                # Let's also check how many courses we have per parent category (including all courses)
                courses_per_parent_query = """
                    SELECT
                        parent_cat.id,
                        parent_cat.name,
                        COUNT(CASE WHEN course.visible = 1 THEN 1 END) as visible_courses,
                        COUNT(CASE WHEN course.visible = 0 THEN 1 END) as hidden_courses,
                        COUNT(course.id) as total_courses
                    FROM mdl_course_categories parent_cat
                    JOIN mdl_course_categories child_cat ON child_cat.parent = parent_cat.id
                    LEFT JOIN mdl_course course ON course.category = child_cat.id
                    WHERE parent_cat.parent = 0
                    AND course.id IS NOT NULL
                    GROUP BY parent_cat.id, parent_cat.name
                    ORDER BY parent_cat.id
                """
                cursor.execute(courses_per_parent_query)
                courses_per_parent = cursor.fetchall()
                logger.info(f"SQL DEBUG: Courses per parent category (including all courses):")
                for row in courses_per_parent:
                    year = cls.get_academic_year_from_category_name(row[1])
                    logger.info(f"SQL DEBUG: Parent {row[0]} '{row[1]}' (Year: {year}) -> Visible: {row[2]}, Hidden: {row[3]}, Total: {row[4]}")

                # Specific check for 2024年度 courses
                specific_2024_query = """
                    SELECT
                        child_cat.id,
                        child_cat.name,
                        COUNT(CASE WHEN course.visible = 1 THEN 1 END) as visible_courses,
                        COUNT(CASE WHEN course.visible = 0 THEN 1 END) as hidden_courses
                    FROM mdl_course_categories parent_cat
                    JOIN mdl_course_categories child_cat ON child_cat.parent = parent_cat.id
                    LEFT JOIN mdl_course course ON course.category = child_cat.id
                    WHERE parent_cat.id = 142
                    AND course.id IS NOT NULL
                    GROUP BY child_cat.id, child_cat.name
                    ORDER BY child_cat.id
                """
                cursor.execute(specific_2024_query)
                cat_2024_courses = cursor.fetchall()
                logger.info(f"SQL DEBUG: 2024年度 child categories and their courses:")
                for row in cat_2024_courses:
                    logger.info(f"SQL DEBUG: Child {row[0]} '{row[1]}' -> Visible: {row[2]}, Hidden: {row[3]}")

                # Use the working query to get all courses with their category hierarchy
                # Include ALL courses regardless of visibility for historical analysis
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

                logger.info(f"SQL DEBUG: Executing courses query for all categories")
                logger.info(f"SQL DEBUG: {courses_query}")

                cursor.execute(courses_query)
                rows = cursor.fetchall()
                logger.info(f"SQL DEBUG: Retrieved {len(rows)} total course records from database")

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

                processed_categories = set()
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

                    # Log category info only once per category
                    category_key = f"{parent_id}_{child_id}"
                    if category_key not in processed_categories:
                        logger.info(f"SQL DEBUG: Processing category: {parent_name} > {child_name} (Parent Year: {parent_year})")
                        processed_categories.add(category_key)

                    # Only include courses if parent category matches the academic year
                    if parent_year == academic_year:
                        matched_courses_count += 1
                        visibility_status = "VISIBLE" if course_visible == 1 else "HIDDEN"
                        logger.info(f"SQL DEBUG: ✓ Course '{course_name}' (ID: {course_id}) matches academic year {academic_year} - Status: {visibility_status}")

                        # Convert Unix timestamps to datetime objects (keeping for compatibility)
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
                                'academic_year': parent_year,  # Use parent year, not child year
                                'courses': [],
                                'course_count': 0
                            }

                        # Add course (including both visible and hidden courses)
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
                    else:
                        # Log some examples of non-matching courses for debugging
                        if matched_courses_count < 5:  # Only log first few to avoid spam
                            logger.info(f"SQL DEBUG: ✗ Course '{course_name}' (ID: {course_id}) does NOT match academic year {academic_year} (parent_year={parent_year})")

                logger.info(f"SQL DEBUG: FINAL RESULTS for academic year {academic_year}:")
                logger.info(f"SQL DEBUG: - Total course records processed: {len(rows)}")
                logger.info(f"SQL DEBUG: - Courses matching academic year: {matched_courses_count}")
                logger.info(f"SQL DEBUG: - Categories found: {len(year_courses['categories'])}")
                logger.info(f"SQL DEBUG: - Final total_courses: {year_courses['total_courses']}")

                # Cache the result for 1 hour
                cache.set(cache_key, year_courses, 3600)
                logger.info(f"CACHE DEBUG: Cached results for academic year {academic_year} with key '{cache_key}'")

                return year_courses

        except Exception as e:
            logger.error(f"SQL DEBUG: Error fetching courses for academic year {academic_year}: {str(e)}")
            logger.error(f"SQL DEBUG: Exception details:", exc_info=True)
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
    def clear_cache_for_year(cls, academic_year: int) -> bool:
        """Clear all cache keys for a specific academic year"""
        cache_keys_cleared = []

        # Clear existing cache keys
        course_cache_key = f'past_year_courses_{academic_year}'
        cache.delete(course_cache_key)
        cache_keys_cleared.append(course_cache_key)

        student_cache_key = f'student_user_ids_{academic_year}'
        cache.delete(student_cache_key)
        cache_keys_cleared.append(student_cache_key)

        non_student_cache_key = f'non_student_user_ids_{academic_year}'
        cache.delete(non_student_cache_key)
        cache_keys_cleared.append(non_student_cache_key)

        # Clear new student analytics cache keys for both activity filter settings
        cache_key_base = f'student_analytics_{academic_year}'

        # Clear cache for both activity filter variants
        for activity_suffix in ['_all_activities', '_graded_only']:
            # Main analytics cache
            main_cache_key = f'{cache_key_base}_main{activity_suffix}'
            cache.delete(main_cache_key)
            cache_keys_cleared.append(main_cache_key)

            # Chart data cache
            chart_cache_key = f'{cache_key_base}_charts{activity_suffix}'
            cache.delete(chart_cache_key)
            cache_keys_cleared.append(chart_cache_key)

            # Engagement categories cache
            engagement_cache_key = f'{cache_key_base}_engagement{activity_suffix}'
            cache.delete(engagement_cache_key)
            cache_keys_cleared.append(engagement_cache_key)

        # Clear courses context cache (shared between activity filters)
        courses_context_cache_key = f'{cache_key_base}_courses_context'
        cache.delete(courses_context_cache_key)
        cache_keys_cleared.append(courses_context_cache_key)

        # Clear course-specific grade distribution caches
        # Get cache key registry for this year
        registry_key = f'cache_registry_{academic_year}'
        cached_course_keys = cache.get(registry_key, [])

        for course_cache_key in cached_course_keys:
            cache.delete(course_cache_key)
            cache_keys_cleared.append(course_cache_key)

        # Clear the registry itself
        cache.delete(registry_key)
        if cached_course_keys:
            cache_keys_cleared.append(registry_key)

        logger.info(f"CACHE CLEAR: Cleared {len(cache_keys_cleared)} cache keys for academic year {academic_year}")
        logger.debug(f"CACHE CLEAR: Keys cleared: {cache_keys_cleared}")

        return True

    @classmethod
    def register_course_cache_key(cls, academic_year: int, course_id: str) -> None:
        """Register a course-specific cache key for later clearing"""
        registry_key = f'cache_registry_{academic_year}'
        course_cache_key = f'course_grade_distribution_{academic_year}_{course_id}'

        # Get existing registry or create new one
        cached_keys = cache.get(registry_key, [])

        # Add the new key if not already present
        if course_cache_key not in cached_keys:
            cached_keys.append(course_cache_key)
            # Store registry with longer TTL (4 hours) than individual caches
            cache.set(registry_key, cached_keys, 14400)
            logger.debug(f"CACHE REGISTRY: Added {course_cache_key} to registry for year {academic_year}")

    @classmethod
    def get_available_academic_years(cls) -> List[int]:
        """
        Get all available academic years from course categories.
        Returns a list of academic years found in category names.
        """
        cache_key = 'available_academic_years'
        cached_data = cache.get(cache_key)

        if cached_data:
            logger.info(f"Using cached academic years: {cached_data}")
            return cached_data

        logger.info("Fetching available academic years from categories")

        try:
            with connections['moodle_db'].cursor() as cursor:
                query = """
                    SELECT DISTINCT name
                    FROM mdl_course_categories
                    WHERE parent = 0
                    ORDER BY name DESC
                """

                logger.info(f"SQL DEBUG: Executing academic years query: {query}")
                cursor.execute(query)
                category_names = [row[0] for row in cursor.fetchall()]
                logger.info(f"SQL DEBUG: Retrieved {len(category_names)} top-level categories: {category_names}")

                # Extract academic years from category names
                academic_years = []
                for name in category_names:
                    year = cls.get_academic_year_from_category_name(name)
                    logger.info(f"SQL DEBUG: Category '{name}' -> Academic year: {year}")
                    if year and year not in academic_years:
                        academic_years.append(year)

                # Sort in descending order (most recent first)
                academic_years.sort(reverse=True)
                logger.info(f"SQL DEBUG: Final academic years: {academic_years}")

                # Cache for 1 hour
                cache.set(cache_key, academic_years, 3600)

                return academic_years

        except Exception as e:
            logger.error(f"SQL DEBUG: Error fetching available academic years: {str(e)}")
            return []

    @classmethod
    def get_student_user_ids_for_academic_year(cls, academic_year: int) -> List[str]:
        """
        Get all student user IDs enrolled in courses for a specific academic year.
        This returns only actual students (not teachers, managers, etc.) based on Moodle role assignments.

        Args:
            academic_year (int): The academic year (e.g., 2024 for 2024年度)

        Returns:
            List[str]: List of student user IDs for the academic year
        """
        cache_key = f'student_user_ids_{academic_year}'
        cached_data = cache.get(cache_key)

        if cached_data:
            logger.info(f"Using cached student user IDs for academic year {academic_year}: {len(cached_data)} students")
            return cached_data

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

            logger.info(f"Found {len(course_ids)} courses for academic year {academic_year}")

            # Get students enrolled in these courses
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
                    WHERE r.shortname = 'student'
                    AND ctx.contextlevel = 50
                    AND u.deleted = 0
                    AND u.suspended = 0
                    AND c.id IN ({course_placeholders})
                    ORDER BY u.id
                """

                logger.debug(f"Executing student enrollment query for {len(course_ids)} courses")
                cursor.execute(query, course_ids)
                student_records = cursor.fetchall()

            # Convert to list of strings for consistency with ClickHouse data
            student_user_ids = [str(record[0]) for record in student_records]

            logger.info(f"Found {len(student_user_ids)} students enrolled in academic year {academic_year} courses")

            # Cache for 1 hour
            cache.set(cache_key, student_user_ids, 3600)

            return student_user_ids

        except Exception as e:
            logger.error(f"Error fetching student user IDs for academic year {academic_year}: {str(e)}")
            return []

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
    def test_student_filtering_effectiveness(cls, academic_year: int) -> Dict[str, Any]:
        """
        Test method to show the effectiveness of student filtering.
        This helps verify that the filtering is working correctly by comparing
        filtered vs unfiltered results.

        Args:
            academic_year (int): The academic year to test

        Returns:
            Dict containing comparison of filtered vs unfiltered data
        """
        logger.info(f"Testing student filtering effectiveness for academic year {academic_year}")

        try:
            # Get the student user IDs for this academic year
            student_user_ids = PastYearCourseCategory.get_student_user_ids_for_academic_year(academic_year)

            # Calculate date range
            start_date = f"{academic_year}-04-01"
            end_date = f"{academic_year + 1}-03-31"

            # Get unfiltered grade data (for comparison)
            with connections['analysis_db'].cursor() as cursor:
                unfiltered_query = """
                    SELECT
                        COUNT(DISTINCT student_id) as total_students,
                        COUNT(DISTINCT course_id) as total_courses,
                        COUNT(*) as total_grades
                    FROM course_student_scores
                    WHERE created_at >= %s
                    AND created_at <= %s
                    AND quiz IS NOT NULL
                """
                cursor.execute(unfiltered_query, [start_date, end_date])
                unfiltered_stats = cursor.fetchone()

            # Get filtered grade data using our method
            filtered_analytics = cls._get_grade_analytics(academic_year, start_date, end_date)
            filtered_stats = filtered_analytics.get('overall_stats', {})

            # Get ClickHouse activity data comparison
            with connections['clickhouse_db_pre_2025'].cursor() as cursor:
                # Unfiltered ClickHouse data
                unfiltered_ch_query = """
                    SELECT
                        COUNT(DISTINCT actor_account_name) as total_accounts,
                        COUNT(DISTINCT _id) as total_activities
                    FROM statements_mv
                    WHERE timestamp >= toDate(%s)
                    AND timestamp <= toDate(%s)
                    AND context_id != ''
                    AND context_id IS NOT NULL
                """
                cursor.execute(unfiltered_ch_query, [start_date, end_date])
                unfiltered_ch_stats = cursor.fetchone()

            # Get filtered ClickHouse data using our method
            filtered_ch_analytics = cls._get_course_access_analytics(academic_year, start_date, end_date)
            filtered_ch_stats = filtered_ch_analytics.get('overall_stats', {})

            result = {
                'academic_year': academic_year,
                'student_filter_info': {
                    'total_student_ids_for_year': len(student_user_ids),
                    'sample_student_ids': student_user_ids[:10],  # First 10 for reference
                },
                'grade_data_comparison': {
                    'unfiltered': {
                        'total_students': unfiltered_stats[0] if unfiltered_stats else 0,
                        'total_courses': unfiltered_stats[1] if unfiltered_stats else 0,
                        'total_grades': unfiltered_stats[2] if unfiltered_stats else 0,
                    },
                    'filtered': {
                        'total_students': filtered_stats.get('total_students', 0),
                        'total_courses': filtered_stats.get('total_courses', 0),
                        'total_grades': filtered_stats.get('total_grades', 0),
                    },
                    'filtering_effect': {
                        'students_removed': (unfiltered_stats[0] if unfiltered_stats else 0) - filtered_stats.get('total_students', 0),
                        'grades_removed': (unfiltered_stats[2] if unfiltered_stats else 0) - filtered_stats.get('total_grades', 0),
                    }
                },
                'activity_data_comparison': {
                    'unfiltered': {
                        'total_accounts': unfiltered_ch_stats[0] if unfiltered_ch_stats else 0,
                        'total_activities': unfiltered_ch_stats[1] if unfiltered_ch_stats else 0,
                    },
                    'filtered': {
                        'total_accounts': filtered_ch_stats.get('total_unique_accounts', 0),
                        'total_students': filtered_ch_stats.get('total_unique_students', 0),
                        'total_activities': filtered_ch_stats.get('total_activities', 0),
                    },
                    'filtering_effect': {
                        'accounts_removed': (unfiltered_ch_stats[0] if unfiltered_ch_stats else 0) - filtered_ch_stats.get('total_unique_accounts', 0),
                        'activities_removed': (unfiltered_ch_stats[1] if unfiltered_ch_stats else 0) - filtered_ch_stats.get('total_activities', 0),
                    }
                }
            }

            # Log the results
            logger.info(f"STUDENT FILTERING TEST RESULTS for {academic_year}:")
            logger.info(f"  Student IDs for filtering: {len(student_user_ids)}")
            logger.info(f"  Grade data - Unfiltered: {result['grade_data_comparison']['unfiltered']}")
            logger.info(f"  Grade data - Filtered: {result['grade_data_comparison']['filtered']}")
            logger.info(f"  Grade filtering effect: {result['grade_data_comparison']['filtering_effect']}")
            logger.info(f"  Activity data - Unfiltered: {result['activity_data_comparison']['unfiltered']}")
            logger.info(f"  Activity data - Filtered: {result['activity_data_comparison']['filtered']}")
            logger.info(f"  Activity filtering effect: {result['activity_data_comparison']['filtering_effect']}")

            return result

        except Exception as e:
            logger.error(f"Error testing student filtering effectiveness: {str(e)}")
            return {
                'academic_year': academic_year,
                'error': str(e)
            }

    @classmethod
    def get_course_grade_distribution(cls, course_id: str, academic_year: int) -> Dict[str, Any]:
        """
        Get individual student grades for a specific course to create distribution charts.

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

            # Calculate date range for academic year
            if academic_year == 2023:
                start_date = f"{academic_year}-04-01"
                end_date = f"{academic_year + 2}-03-31"
            else:
                start_date = f"{academic_year}-04-01"
                end_date = f"{academic_year + 1}-03-31"

            with connections['analysis_db'].cursor() as cursor:
                # Build student filter clause
                filter_placeholders = ",".join(["%s"] * len(filter_ids))
                if filter_type == 'NOT_IN':
                    student_filter = f" AND student_id NOT IN ({filter_placeholders}) AND student_id IS NOT NULL"
                else:
                    student_filter = f" AND student_id IN ({filter_placeholders})"

                # Get individual grades for the course
                individual_grades_query = f"""
                    SELECT
                        student_id,
                        quiz as grade,
                        created_at,
                        course_name
                    FROM course_student_scores
                    WHERE course_id = %s
                    AND created_at >= %s
                    AND created_at <= %s
                    AND quiz IS NOT NULL{student_filter}
                    ORDER BY quiz DESC
                """

                cursor.execute(individual_grades_query, [course_id, start_date, end_date] + filter_ids)
                individual_grades = cursor.fetchall()

                if not individual_grades:
                    return {
                        'course_id': course_id,
                        'individual_grades': [],
                        'distribution_data': [],
                        'stats': {},
                        'error': 'No grades found for this course'
                    }

                # Extract just the grade values for statistical analysis
                grade_values = [float(grade[1]) for grade in individual_grades]

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
                        'course_name': grade_record[3]
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
                    'filter_info': filter_config
                }

                logger.info(f"Successfully retrieved {grade_count} grades for course {course_id}")
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
    """Model to access student grades from analysis_db"""
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

        Args:
            academic_year (int): The academic year to analyze
            course_ids (List[str], optional): List of course IDs to filter analysis to specific courses
        """
        logger.info(f"Fetching student analytics for academic year {academic_year} with {len(course_ids) if course_ids else 'all'} courses")
        logger.debug(f"STUDENT ANALYTICS: Input course_ids: {course_ids[:10] if course_ids else 'None'}...")

        try:
            # Calculate date range for academic year (April to March)
            if academic_year == 2023:
                # Based on diagnostic findings, 2023 courses actually have activity data
                # from 2024-01 onwards, so we need to extend the range
                start_date = f"{academic_year}-04-01"  # April 1, 2023
                end_date = f"{academic_year + 2}-03-31"  # March 31, 2025 (extended to capture actual activity)
                logger.debug(f"STUDENT ANALYTICS: Extended date range for 2023: {start_date} to {end_date}")
            else:
                start_date = f"{academic_year}-04-01"  # April 1 of academic year
                end_date = f"{academic_year + 1}-03-31"  # March 31 of following year
            logger.debug(f"STUDENT ANALYTICS: Date range: {start_date} to {end_date}")

            # Get grade analytics from analysis_db
            logger.debug(f"STUDENT ANALYTICS: Starting grade analytics...")
            grade_analytics = cls._get_grade_analytics(academic_year, start_date, end_date, course_ids)
            logger.debug(f"STUDENT ANALYTICS: Grade analytics completed")

            # Get course access analytics from clickhouse_db_pre_2025 - NOW WITH COURSE FILTERING
            logger.debug(f"STUDENT ANALYTICS: Starting access analytics...")
            access_analytics = cls._get_course_access_analytics(academic_year, start_date, end_date, course_ids)
            logger.debug(f"STUDENT ANALYTICS: Access analytics completed")

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
                    'end': end_date
                },
                'grade_analytics': grade_analytics,
                'access_analytics': access_analytics,
                'combined_analytics': combined_analytics,
                'summary_stats': summary_stats
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
                'error': str(e)
            }

    @classmethod
    def _get_grade_analytics(cls, academic_year: int, start_date: str, end_date: str, course_ids: List[str] = None) -> Dict[str, Any]:
        """Get grade analytics from analysis_db (MySQL)"""
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
                    'monthly_trends': []
                }

            logger.debug(f"GRADE ANALYTICS: Using {filter_type} filtering with {filter_count} IDs")
            logger.debug(f"GRADE ANALYTICS: {filter_config['efficiency_reason']}")

            with connections['analysis_db'].cursor() as cursor:
                # Build course filter clause
                course_filter = ""
                course_params = []
                if course_ids:
                    course_filter = " AND course_id IN (" + ",".join(["%s"] * len(course_ids)) + ")"
                    course_params = course_ids
                    logger.debug(f"GRADE ANALYTICS: Filtering by {len(course_ids)} course IDs: {course_ids[:10]}...")
                else:
                    logger.debug(f"GRADE ANALYTICS: No course filtering applied")

                # Build student filter clause based on optimal approach
                filter_placeholders = ",".join(["%s"] * len(filter_ids))
                if filter_type == 'NOT_IN':
                    student_filter = f" AND student_id NOT IN ({filter_placeholders}) AND student_id IS NOT NULL"
                    logger.debug(f"GRADE ANALYTICS: Using NOT IN filter to exclude {filter_count} non-students")
                else:
                    student_filter = f" AND student_id IN ({filter_placeholders})"
                    logger.debug(f"GRADE ANALYTICS: Using IN filter to include {filter_count} students")

                filter_params = filter_ids

                # Overall grade statistics - MySQL compatible with optimized student filtering
                overall_stats_query = f"""
                    SELECT
                        COUNT(DISTINCT student_id) as total_students,
                        COUNT(DISTINCT course_id) as total_courses,
                        COUNT(*) as total_grades,
                        AVG(quiz) as avg_grade,
                        MIN(quiz) as min_grade,
                        MAX(quiz) as max_grade
                    FROM course_student_scores
                    WHERE created_at >= %s
                    AND created_at <= %s
                    AND quiz IS NOT NULL{student_filter}{course_filter}
                """
                logger.debug(f"GRADE ANALYTICS: Overall stats query: {overall_stats_query}")
                logger.debug(f"GRADE ANALYTICS: Query params: [{start_date}, {end_date}] + {len(filter_params)} filter IDs + {len(course_params)} course IDs")
                cursor.execute(overall_stats_query, [start_date, end_date] + filter_params + course_params)
                overall_stats = cursor.fetchone()
                logger.debug(f"GRADE ANALYTICS: Overall stats result: {overall_stats}")

                # Let's also check what courses actually have grades (with student filtering)
                courses_with_grades_query = f"""
                    SELECT DISTINCT course_id, course_name, COUNT(*) as grade_count
                    FROM course_student_scores
                    WHERE created_at >= %s
                    AND created_at <= %s
                    AND quiz IS NOT NULL{student_filter}{course_filter}
                    GROUP BY course_id, course_name
                    ORDER BY grade_count DESC
                """
                logger.debug(f"GRADE ANALYTICS: Courses with grades query: {courses_with_grades_query}")
                cursor.execute(courses_with_grades_query, [start_date, end_date] + filter_params + course_params)
                courses_with_grades = cursor.fetchall()
                logger.debug(f"GRADE ANALYTICS: Found {len(courses_with_grades)} courses with grades (student-filtered using {filter_type})")
                for i, course in enumerate(courses_with_grades[:10]):  # Log first 10
                    logger.debug(f"GRADE ANALYTICS: Course {i+1}: ID={course[0]}, Name={course[1]}, Grades={course[2]}")

                # Simplified median calculation - just use average as approximation for now
                # MySQL median calculation is complex and not critical for analytics
                median_grade = overall_stats[3] if overall_stats and overall_stats[3] else 0

                # Grade distribution by ranges - MySQL compatible with student filtering
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
                    WHERE created_at >= %s
                    AND created_at <= %s
                    AND quiz IS NOT NULL{student_filter}{course_filter}
                    GROUP BY grade_range
                    ORDER BY grade_range
                """
                logger.debug(f"GRADE ANALYTICS: Grade distribution query: {grade_distribution_query}")
                cursor.execute(grade_distribution_query, [start_date, end_date] + filter_params + course_params)
                grade_distribution = cursor.fetchall()
                logger.debug(f"GRADE ANALYTICS: Grade distribution result: {grade_distribution}")

                # Course-level grade statistics - MySQL compatible with student filtering
                course_stats_query = f"""
                    SELECT
                        course_id,
                        course_name,
                        COUNT(DISTINCT student_id) as student_count,
                        COUNT(*) as grade_count,
                        AVG(quiz) as avg_grade,
                        MIN(quiz) as min_grade,
                        MAX(quiz) as max_grade
                    FROM course_student_scores
                    WHERE created_at >= %s
                    AND created_at <= %s
                    AND quiz IS NOT NULL{student_filter}{course_filter}
                    GROUP BY course_id, course_name
                    ORDER BY student_count DESC
                """
                logger.debug(f"GRADE ANALYTICS: Course stats query: {course_stats_query}")
                cursor.execute(course_stats_query, [start_date, end_date] + filter_params + course_params)
                course_stats = cursor.fetchall()
                logger.debug(f"GRADE ANALYTICS: Found {len(course_stats)} courses with detailed stats (student-filtered using {filter_type})")

                # Monthly grade trends - MySQL compatible with student filtering
                monthly_trends_query = f"""
                    SELECT
                        DATE_FORMAT(created_at, '%%Y%%m') as month,
                        COUNT(DISTINCT student_id) as active_students,
                        COUNT(*) as total_grades,
                        AVG(quiz) as avg_grade
                    FROM course_student_scores
                    WHERE created_at >= %s
                    AND created_at <= %s
                    AND quiz IS NOT NULL{student_filter}{course_filter}
                    GROUP BY month
                    ORDER BY month
                """
                logger.debug(f"GRADE ANALYTICS: Monthly trends query: {monthly_trends_query}")
                cursor.execute(monthly_trends_query, [start_date, end_date] + filter_params + course_params)
                monthly_trends = cursor.fetchall()
                logger.debug(f"GRADE ANALYTICS: Monthly trends result: {len(monthly_trends)} months (student-filtered using {filter_type})")

                # Simplified course stats without complex median calculation
                course_stats_with_median = []
                for course_stat in course_stats:
                    course_stats_with_median.append({
                        'course_id': course_stat[0],
                        'course_name': course_stat[1],
                        'student_count': course_stat[2],
                        'grade_count': course_stat[3],
                        'avg_grade': round(float(course_stat[4]), 2) if course_stat[4] else 0,
                        'min_grade': float(course_stat[5]) if course_stat[5] else 0,
                        'max_grade': float(course_stat[6]) if course_stat[6] else 0,
                        'median_grade': round(float(course_stat[4]), 2) if course_stat[4] else 0,  # Use avg as approximation
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
                    'filter_info': filter_config  # Include filter info for debugging
                }

                logger.debug(f"GRADE ANALYTICS: Final result summary (student-filtered using {filter_type}) - Students: {result['overall_stats']['total_students']}, Courses: {result['overall_stats']['total_courses']}, Grades: {result['overall_stats']['total_grades']}")
                return result

        except Exception as e:
            logger.error(f"Error fetching grade analytics: {str(e)}")
            return {}

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

                # STEP 1: First get all activity types and their counts to find the top 5
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
                    LIMIT 10
                """
                logger.debug(f"ACCESS ANALYTICS: Getting top activity types: {top_activity_types_query}")
                cursor.execute(top_activity_types_query, [start_date, end_date])
                top_activity_types_raw = cursor.fetchall()

                # Build dynamic top activity types list
                top_activity_types = []
                dynamic_activity_fields = []

                for i, row in enumerate(top_activity_types_raw):
                    operation_name = row[0]
                    activity_count = row[1]

                    # Create dynamic field name (use operation_name as-is per user request)
                    field_name = operation_name.lower().replace(' ', '_').replace('-', '_')

                    top_activity_types.append({
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

                logger.info(f"ACCESS ANALYTICS: Found top 10 activity types: {[at['name'] for at in top_activity_types]}")

                # If no activity types found, return empty result
                if not top_activity_types:
                    logger.warning(f"ACCESS ANALYTICS: No activity types found for academic year {academic_year}")
                    return {
                        'overall_stats': {},
                        'student_access': [],
                        'course_access': [],
                        'activity_types': [],
                        'top_activity_types': [],
                        'student_id_mapping_debug': {}
                    }

                # STEP 2: Build dynamic SQL query with the top activity types
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
