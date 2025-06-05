import math
import datetime
import memcache
import logging
from django.db import models
from django.db import connections
from django.utils import timezone
from django.core.cache import cache
from typing import List, Dict, Any
from django.conf import settings

logger = logging.getLogger(__name__)

class TeacherExclusion(models.Model):
    """
    Model to store teacher accounts that should be excluded from activity charts.
    Used to filter out testing accounts and other non-production teacher accounts.
    """
    name = models.CharField(max_length=255, help_text="Teacher's full name for reference")
    lms_id = models.IntegerField(unique=True, help_text="Teacher's LMS user ID from Moodle")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    reason = models.CharField(max_length=255, blank=True, null=True, help_text="Reason for exclusion (e.g., 'Testing account')")
    is_active = models.BooleanField(default=True, help_text="Whether this exclusion is currently active")

    class Meta:
        db_table = 'teacher_exclusions'
        verbose_name = 'Teacher Exclusion'
        verbose_name_plural = 'Teacher Exclusions'
        ordering = ['name']

    def __str__(self):
        return f"{self.name} (ID: {self.lms_id})"

    @classmethod
    def get_excluded_teacher_ids(cls):
        """
        Get list of teacher IDs that should be excluded from activity charts.
        Uses Redis caching to improve performance.

        Returns:
            list: List of teacher IDs (as strings) to exclude
        """
        cache_key = 'excluded_teacher_ids'
        cached_result = cache.get(cache_key)

        if cached_result is not None:
            return cached_result

        # Get excluded teacher IDs from database
        excluded_ids = list(cls.objects.filter(is_active=True).values_list('lms_id', flat=True))

        # Cache for 1 hour (3600 seconds)
        cache.set(cache_key, excluded_ids, timeout=3600)

        return excluded_ids

    @classmethod
    def clear_exclusion_cache(cls):
        """
        Clear the excluded teacher IDs cache.
        Should be called when TeacherExclusion records are modified.
        """
        cache.delete('excluded_teacher_ids')
        return True

# Create your models here.
class Teacher(models.Model):
    username = models.CharField(max_length=100, primary_key=True)
    user_id = models.IntegerField()
    email = models.EmailField()
    firstname = models.CharField(max_length=100)
    lastname = models.CharField(max_length=100)
    course_id = models.IntegerField()
    course_name = models.CharField(max_length=100)
    role = models.CharField(max_length=100)

    class Meta:
        managed = False
        app_label = 'moodle_app'

    @staticmethod
    def get_teacher_data(sort_by=None, sort_order='asc', use_cache=True):
        """
        Retrieve teacher data from Moodle via raw SQL query with caching and sorting support.
        Now filters out excluded teachers (testing accounts).

        Args:
            sort_by (str): Field to sort by ('active_courses', 'archived_courses', 'total_courses', 'name')
            sort_order (str): Sort order ('asc' or 'desc')
            use_cache (bool): Whether to use cached data

        Returns:
            list: List of teacher dictionaries (excluding blacklisted teachers)
        """
        # Create cache key based on sort parameters
        cache_key = f'teacher_data_{sort_by}_{sort_order}_filtered'

        if use_cache:
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                return cached_result

        query = """
            SELECT
                u.id AS user_id,
                u.username,
                u.email,
                u.firstname,
                u.lastname,
                COUNT(*) AS total_courses,
                SUM(CASE
                    WHEN c.visible = 0 THEN 1
                    WHEN c.enddate > 0 AND c.enddate < UNIX_TIMESTAMP() THEN 1
                    ELSE 0
                END) AS archived_courses,
                SUM(CASE
                    WHEN c.visible = 1 AND (c.enddate = 0 OR c.enddate >= UNIX_TIMESTAMP()) THEN 1
                    ELSE 0
                END) AS active_courses
            FROM
                mdl_user u
            JOIN
                mdl_role_assignments ra ON u.id = ra.userid
            JOIN
                mdl_role r ON ra.roleid = r.id
            JOIN
                mdl_context ctx ON ra.contextid = ctx.id
            JOIN
                mdl_course c ON ctx.instanceid = c.id
            WHERE
                r.shortname IN ('editingteacher', 'teacher')
                AND ctx.contextlevel = 50
                AND u.deleted = 0
            GROUP BY
                u.id, u.username, u.email, u.firstname, u.lastname
        """

        # Add ORDER BY clause based on sort parameters
        if sort_by == 'active_courses':
            query += f" ORDER BY active_courses {sort_order.upper()}, u.firstname, u.lastname"
        elif sort_by == 'archived_courses':
            query += f" ORDER BY archived_courses {sort_order.upper()}, u.firstname, u.lastname"
        elif sort_by == 'total_courses':
            query += f" ORDER BY total_courses {sort_order.upper()}, u.firstname, u.lastname"
        elif sort_by == 'name':
            if sort_order == 'desc':
                query += " ORDER BY u.firstname DESC, u.lastname DESC"
            else:
                query += " ORDER BY u.firstname ASC, u.lastname ASC"
        else:
            # Default sorting
            query += " ORDER BY total_courses, u.firstname, u.lastname"

        with connections['moodle_db'].cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        teachers = [
            {
                "user_id": row[0],
                "username": row[1],
                "email": row[2],
                "firstname": row[3],
                "lastname": row[4],
                "total_courses": row[5],
                "archived_courses": row[6],
                "active_courses": row[7],
            }
            for row in rows
        ]

        # Filter out excluded teachers
        try:
            excluded_teacher_ids = TeacherExclusion.get_excluded_teacher_ids()
            excluded_teacher_ids_str = [str(tid) for tid in excluded_teacher_ids]

            # Filter out excluded teachers
            filtered_teachers = [
                teacher for teacher in teachers
                if str(teacher['user_id']) not in excluded_teacher_ids_str
            ]

            teachers = filtered_teachers

        except Exception as e:
            # If there's an error with exclusions, log it but don't break the functionality
            logger.warning(f"Error filtering excluded teachers: {str(e)}")
            # Continue with unfiltered teachers

        # Cache the results for 1 hour (3600 seconds)
        if use_cache:
            cache.set(cache_key, teachers, timeout=3600)

        return teachers

    @staticmethod
    def clear_teacher_cache():
        """
        Clear all teacher-related cache entries including filtered data.
        """
        # Get all possible cache keys and clear them
        sort_fields = ['active_courses', 'archived_courses', 'total_courses', 'name', None]
        sort_orders = ['asc', 'desc']

        for sort_by in sort_fields:
            for sort_order in sort_orders:
                # Clear old cache keys
                cache_key = f'teacher_data_{sort_by}_{sort_order}'
                cache.delete(cache_key)

                # Clear new filtered cache keys
                filtered_cache_key = f'teacher_data_{sort_by}_{sort_order}_filtered'
                cache.delete(filtered_cache_key)

        # Also clear the default cache key
        cache.delete('teacher_data_None_asc')
        cache.delete('teacher_data_None_asc_filtered')

        # Clear exclusion cache as well
        TeacherExclusion.clear_exclusion_cache()

        return True

    @staticmethod
    def get_teacher_activity_data(time_filter='academic_year'):
        """
        Retrieve teacher activity data from ClickHouse for stacked chart visualization.
        Groups activities by operation name for more detailed insights.

        Args:
            time_filter (str): Time period filter - 'academic_year', 'this_month', 'this_week', 'today'

        Returns:
            dict: Dictionary with teacher data and operation breakdown for stacked charts
        """
        import datetime
        from django.utils import timezone

        # Get current date
        today = timezone.now().date()

        # Calculate time filter based on selection
        if time_filter == 'academic_year':
            # Academic year: April 1 to March 31 (next year)
            current_year = today.year
            if today.month >= 4:
                # We're in the academic year starting this calendar year
                start_date = datetime.date(current_year, 4, 1)
                end_date = datetime.date(current_year + 1, 3, 31)
            else:
                # We're in the academic year that started last calendar year
                start_date = datetime.date(current_year - 1, 4, 1)
                end_date = datetime.date(current_year, 3, 31)
        elif time_filter == 'this_month':
            # First day of current month to today
            start_date = today.replace(day=1)
            end_date = today
        elif time_filter == 'this_week':
            # Start of current week (Monday) to today
            days_since_monday = today.weekday()
            start_date = today - datetime.timedelta(days=days_since_monday)
            end_date = today
        elif time_filter == 'today':
            # Just today
            start_date = today
            end_date = today
        else:
            # Default to academic year
            current_year = today.year
            if today.month >= 4:
                start_date = datetime.date(current_year, 4, 1)
                end_date = datetime.date(current_year + 1, 3, 31)
            else:
                start_date = datetime.date(current_year - 1, 4, 1)
                end_date = datetime.date(current_year, 3, 31)

        try:
            # First get the teacher list from Moodle
            teachers = Teacher.get_teacher_data(use_cache=True)

            # Get excluded teacher IDs
            excluded_teacher_ids = TeacherExclusion.get_excluded_teacher_ids()
            excluded_teacher_ids_str = [str(tid) for tid in excluded_teacher_ids]

            # Filter out excluded teachers
            filtered_teachers = [
                teacher for teacher in teachers
                if str(teacher['user_id']) not in excluded_teacher_ids_str
            ]

            teacher_ids = [str(teacher['user_id']) for teacher in filtered_teachers]

            if not teacher_ids:
                return {'teachers': [], 'series': [], 'categories': [], 'top_operations': []}

            # Create teacher lookup dictionary
            teacher_lookup = {str(teacher['user_id']): f"{teacher['firstname']} {teacher['lastname']}"
                            for teacher in filtered_teachers}

            # Query ClickHouse for teacher activities grouped by operation name
            with connections['clickhouse_db'].cursor() as cursor:
                # Create parameterized query for teacher IDs
                teacher_ids_str = "', '".join(teacher_ids)

                # First, get all operation names to identify top operations
                operations_query = f"""
                    SELECT
                        operation_name,
                        uniqExact(_id) AS total_count
                    FROM statements_mv
                    WHERE actor_account_name IN ('{teacher_ids_str}')
                        AND timestamp >= toDate('{start_date}')
                        AND timestamp <= toDate('{end_date}')
                        AND actor_account_name != ''
                        AND operation_name != ''
                        AND (actor_name_role = 'teacher' OR actor_name_role = 'editingteacher' OR actor_name_role = '')
                    GROUP BY operation_name
                    ORDER BY total_count DESC
                """

                cursor.execute(operations_query)
                all_operations = cursor.fetchall()

                # Get top 10 operations for legend and group others as "Other"
                top_operations = [op[0] for op in all_operations[:10]]
                top_operations_str = "', '".join(top_operations)

                # Main query for detailed activity data
                detailed_query = f"""
                    SELECT
                        actor_account_name,
                        CASE
                            WHEN operation_name IN ('{top_operations_str}') THEN operation_name
                            ELSE 'Other'
                        END AS grouped_operation,
                        uniqExact(_id) AS activity_count
                    FROM statements_mv
                    WHERE actor_account_name IN ('{teacher_ids_str}')
                        AND timestamp >= toDate('{start_date}')
                        AND timestamp <= toDate('{end_date}')
                        AND actor_account_name != ''
                        AND operation_name != ''
                        AND (actor_name_role = 'teacher' OR actor_name_role = 'editingteacher' OR actor_name_role = '')
                    GROUP BY actor_account_name, grouped_operation
                    ORDER BY actor_account_name, activity_count DESC
                """

                cursor.execute(detailed_query)
                activity_rows = cursor.fetchall()

                # Organize data by teacher and operation
                teacher_activities = {}
                all_operations_set = set()

                for row in activity_rows:
                    teacher_id = row[0]
                    operation = row[1]
                    count = row[2]

                    if teacher_id not in teacher_activities:
                        teacher_activities[teacher_id] = {}

                    teacher_activities[teacher_id][operation] = count
                    all_operations_set.add(operation)

                # Calculate total activities per teacher for sorting
                teacher_totals = {}
                for teacher_id, operations in teacher_activities.items():
                    teacher_totals[teacher_id] = sum(operations.values())

                # Sort teachers by total activity count and get top 20
                sorted_teachers = sorted(teacher_totals.items(), key=lambda x: x[1], reverse=True)[:20]
                top_teacher_ids = [teacher_id for teacher_id, _ in sorted_teachers]

                # Prepare data for stacked chart
                # Ensure consistent operation order (top operations first, then "Other")
                operation_order = top_operations + ['Other'] if 'Other' in all_operations_set else top_operations
                operation_order = [op for op in operation_order if op in all_operations_set]

                # Create series data for each operation
                series_data = []
                for operation in operation_order:
                    operation_data = []
                    for teacher_id in top_teacher_ids:
                        count = teacher_activities.get(teacher_id, {}).get(operation, 0)
                        operation_data.append(count)

                    series_data.append({
                        'name': operation,
                        'data': operation_data
                    })

                # Create teacher names array
                teacher_names = [teacher_lookup.get(teacher_id, f"Teacher {teacher_id}") for teacher_id in top_teacher_ids]

                return {
                    'teachers': [
                        {
                            'teacher_id': teacher_id,
                            'teacher_name': teacher_lookup.get(teacher_id, f"Teacher {teacher_id}"),
                            'total_activities': teacher_totals.get(teacher_id, 0)
                        }
                        for teacher_id in top_teacher_ids
                    ],
                    'series': series_data,
                    'categories': teacher_names,
                    'top_operations': operation_order
                }

        except Exception as e:
            logger.error(f"Error fetching teacher activity data: {str(e)}")
            return {'teachers': [], 'series': [], 'categories': [], 'top_operations': []}


class TeacherDetails(models.Model):
    user_id = models.IntegerField(primary_key=True)
    username = models.CharField(max_length=100)
    email = models.EmailField()
    firstname = models.CharField(max_length=100)
    lastname = models.CharField(max_length=100)
    total_courses = models.IntegerField()
    archived_courses = models.IntegerField()
    active_courses = models.IntegerField()

    class Meta:
        managed = False
        app_label = 'moodle_app'

    @staticmethod
    def get_teacher_details(user_id):
        """
        Retrieve teacher details from Moodle via raw SQL query.
        """
        query = """
            SELECT
                u.id AS user_id,
                u.username,
                u.email,
                u.firstname,
                u.lastname,
                u.city,
                u.country,
                u.lastlogin,
                u.timecreated,
                COUNT(*) AS total_courses,
                SUM(CASE
                    WHEN c.visible = 0 THEN 1
                    WHEN c.enddate > 0 AND c.enddate < UNIX_TIMESTAMP() THEN 1
                    ELSE 0
                END) AS archived_courses,
                SUM(CASE
                    WHEN c.visible = 1 AND (c.enddate = 0 OR c.enddate >= UNIX_TIMESTAMP()) THEN 1
                    ELSE 0
                END) AS active_courses
            FROM
                mdl_user u
            JOIN
                mdl_role_assignments ra ON u.id = ra.userid
            JOIN
                mdl_role r ON ra.roleid = r.id
            JOIN
                mdl_context ctx ON ra.contextid = ctx.id
            JOIN
                mdl_course c ON ctx.instanceid = c.id
            WHERE
                r.shortname IN ('editingteacher', 'teacher')
                AND ctx.contextlevel = 50
                AND u.deleted = 0
                AND u.id = %s
            GROUP BY
                u.id, u.username, u.email, u.firstname, u.lastname, u.city, u.country, u.lastlogin, u.timecreated
            ORDER BY
                u.firstname, u.lastname
        """
        with connections['moodle_db'].cursor() as cursor:
            cursor.execute(query, [user_id])
            row = cursor.fetchone()
        if row:
            return {
                "user_id": row[0],
                "username": row[1],
                "email": row[2],
                "firstname": row[3],
                "lastname": row[4],
                "city": row[5],
                "country": row[6],
                "lastlogin": row[7],
                "timecreated": row[8],
                "total_courses": row[9],
                "archived_courses": row[10],
                "active_courses": row[11],
            }
        return None

    @staticmethod
    def get_teacher_course_enrollments(user_id: int) -> List[Dict[str, Any]]:
        """
        Retrieve teacher course enrollments with category details from Moodle with caching.

        Args:
            user_id (int): The ID of the teacher whose enrollments to fetch

        Returns:
            List[Dict[str, Any]]: List of dictionaries containing course enrollment details
        """
        # Try to get cached result first
        cache_key = f'teacher_enrollments_{user_id}'
        cached_result = cache.get(cache_key)

        if cached_result is not None:
            return cached_result

        # If no cached result, execute the query
        query = """
            WITH RECURSIVE category_hierarchy AS (
                -- Base case: root categories
                SELECT
                    id AS category_id,
                    name AS category_name,
                    parent AS parent_id,
                    name AS full_category_path
                FROM
                    mdl_course_categories
                WHERE
                    parent = 0

                UNION ALL
                SELECT
                    child.id,
                    child.name,
                    child.parent,
                    CONCAT(parent_hierarchy.full_category_path, ' / ', child.name)
                FROM
                    mdl_course_categories child
                    INNER JOIN category_hierarchy parent_hierarchy
                        ON child.parent = parent_hierarchy.category_id
            )
            SELECT DISTINCT
                c.id,
                c.fullname,
                c.shortname,
                c.visible,
                c.startdate,
                c.enddate,
                ch.category_id,
                ch.full_category_path,
                r.shortname as role_name
            FROM
                mdl_role_assignments ra
                INNER JOIN mdl_context ctx
                    ON ra.contextid = ctx.id
                INNER JOIN mdl_course c
                    ON ctx.instanceid = c.id
                LEFT JOIN category_hierarchy ch
                    ON c.category = ch.category_id
                INNER JOIN mdl_role r
                    ON ra.roleid = r.id
            WHERE
                ra.userid = %s
                AND ra.contextid IN (
                    SELECT id
                    FROM mdl_context
                    WHERE contextlevel = 50  -- Course context level
                )
                AND ra.roleid IN (
                    SELECT id
                    FROM mdl_role
                    WHERE shortname IN ('teacher', 'editingteacher')
                )
            ORDER BY
                ch.full_category_path,
                c.fullname
        """

        # Process results in batches to handle large datasets efficiently
        BATCH_SIZE = 1000
        results = []

        try:
            with connections['moodle_db'].cursor() as cursor:
                cursor.execute(query, [user_id])

                while True:
                    rows = cursor.fetchmany(BATCH_SIZE)
                    if not rows:
                        break

                    batch_results = [
                        {
                            "course_id": row[0],
                            "course_name": row[1],
                            "course_shortname": row[2],
                            "visible": row[3],
                            "startdate": row[4],
                            "enddate": row[5],
                            "category_id": row[6],
                            "category_path": row[7],
                            "role_name": row[8],
                        }
                        for row in rows
                    ]
                    results.extend(batch_results)

            # Cache the results for 24 hours (86400 seconds)
            cache.set(cache_key, results, timeout=86400)

            return results

        except Exception as e:
            # Log the error if you have logging configured
            logger.error(f"Error fetching teacher enrollments for user {user_id}: {str(e)}")
            return []

    @staticmethod
    def get_teacher_last_access_course_list(user_id):
        """
        Retrieve the last access course list for a teacher from Moodle.
        """
        my_sql_query = """
        SELECT u.firstname, u.lastname, c.fullname AS course_name, ula.timeaccess, c.id AS course_id, cc.name AS category_name
        FROM mdl_user_lastaccess ula
        JOIN mdl_user u ON u.id = ula.userid
        JOIN mdl_course c ON c.id = ula.courseid
        LEFT JOIN mdl_course_categories cc ON c.category = cc.id
        WHERE u.id = %s
        AND ula.timeaccess >= UNIX_TIMESTAMP(NOW() - INTERVAL 30 DAY)
        ORDER BY ula.timeaccess DESC;
        """
        with connections['moodle_db'].cursor() as cursor:
            cursor.execute(my_sql_query, [user_id])
            rows = cursor.fetchall()

        # Convert rows of tuples to list of dictionaries with named keys
        result = []
        for row in rows:
            # Format the Unix timestamp to a readable date
            access_time = datetime.datetime.fromtimestamp(row[3])
            formatted_time = access_time.strftime('%Y-%m-%d %H:%M:%S')

            result.append({
                'firstname': row[0],
                'lastname': row[1],
                'course_name': row[2],
                'timeaccess': row[3],
                'timeaccess_formatted': formatted_time,
                'course_id': row[4],
                'category_name': row[5] if row[5] else 'Uncategorized',
                'category_path': row[5] if row[5] else 'Uncategorized'  # For backwards compatibility
            })

        return result

    @staticmethod
    def get_full_teacher_details(user_id):
        """
        Combine Moodle data for a teacher.
        """
        teacher_details = TeacherDetails.get_teacher_details(user_id)
        if not teacher_details:
            return None

        teacher_details["enrollments"] = TeacherDetails.get_teacher_course_enrollments(user_id)
        teacher_details["last_access_courses"] = TeacherDetails.get_teacher_last_access_course_list(user_id)

        return teacher_details


class Student(models.Model):
    """
    Read-only model representing a Moodle student.
    """
    user_id = models.IntegerField(primary_key=True)
    username = models.CharField(max_length=100)
    email = models.EmailField()
    firstname = models.CharField(max_length=100)
    lastname = models.CharField(max_length=100)
    total_coruses = models.IntegerField()
    archived_courses = models.IntegerField()
    active_courses = models.IntegerField()

    class Meta:
        managed = False
        app_label = 'moodle_app'

    @staticmethod
    def get_student_data(search_term=None, page=1, page_size=25):
        """
        Retrieve paginated student data from Moodle via raw SQL query, with optional search.
        Returns a dictionary containing 'results', 'total_pages', 'current_page', 'page_size', 'total_records'.
        """
        # --- Base query ---
        base_query = """
            SELECT
                u.id AS user_id,
                u.username,
                u.email,
                u.firstname,
                u.lastname,
                COUNT(*) AS total_courses,
                SUM(CASE
                    WHEN c.visible = 0 THEN 1
                    WHEN c.enddate > 0 AND c.enddate < UNIX_TIMESTAMP() THEN 1
                    ELSE 0
                END) AS archived_courses,
                SUM(CASE
                    WHEN c.visible = 1 AND (c.enddate = 0 OR c.enddate >= UNIX_TIMESTAMP()) THEN 1
                    ELSE 0
                END) AS active_courses
            FROM
                mdl_user u
            JOIN
                mdl_role_assignments ra ON u.id = ra.userid
            JOIN
                mdl_role r ON ra.roleid = r.id
            JOIN
                mdl_context ctx ON ra.contextid = ctx.id
            JOIN
                mdl_course c ON ctx.instanceid = c.id
        """

        # --- WHERE conditions ---
        conditions = [
            "r.shortname = 'student'",  # only students
            "ctx.contextlevel = 50",    # 50 = course context in Moodle
            "u.deleted = 0"            # not deleted
        ]
        params = []  # list of parameters for secure binding

        if search_term:
            conditions.append(
                "(u.username LIKE %s OR u.firstname LIKE %s OR u.lastname LIKE %s OR u.email LIKE %s)"
            )
            for _ in range(4):
                params.append(f"%{search_term}%")

        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)

        # --- ORDER BY ---
        base_query += """
            GROUP BY
                u.id, u.username, u.email, u.firstname, u.lastname
            ORDER BY
                u.firstname, u.lastname
            """

        # --- 1) Count total records for pagination ---
        count_query = f"SELECT COUNT(*) FROM ({base_query}) AS sub"
        with connections['moodle_db'].cursor() as cursor:
            cursor.execute(count_query, params)
            total_records = cursor.fetchone()[0]

        # --- 2) Apply LIMIT & OFFSET ---
        offset = (page - 1) * page_size
        paginated_query = f"{base_query} LIMIT %s OFFSET %s"
        params_for_page = params + [page_size, offset]

        # Execute paginated query
        with connections['moodle_db'].cursor() as cursor:
            cursor.execute(paginated_query, params_for_page)
            rows = cursor.fetchall()

        # Build 'results' list
        results = []
        user_ids_on_page = []
        for row in rows:
            user_id = row[0]
            user_ids_on_page.append(user_id)
            results.append({
                "user_id": row[0],
                "username": row[1],
                "email": row[2],
                "firstname": row[3],
                "lastname": row[4],
                "total_courses": int(row[5]),
                "archived_courses": int(row[6]),
                "active_coruses": int(row[7]),
            })

        # --- 3) Check "online" status from ClickHouse ---
        # We'll get the last xAPI event_time for each user in user_ids_on_page
        # and compare to now() - 2 minutes
        online_status_map = {}
        if user_ids_on_page:
            threshold_time = timezone.now() - datetime.timedelta(minutes=2)

            # For safety, we can parameterize the user_ids via a simple numeric check
            # But typically, user IDs are guaranteed to be integers, so below is usually safe.
            # Adjust query to match your actual ClickHouse schema.
            user_ids_str = ", ".join(f"'{uid}'" for uid in user_ids_on_page)

            # If you have a large number of user_ids, consider chunking.
            # For demonstration, we assume user_ids_on_page is not huge.
            clickhouse_query = f"""
                SELECT
                actor_account_name,
                MAX(`timestamp`) AS last_event
                FROM statements_mv
                WHERE actor_account_name IN ({user_ids_str})
                GROUP BY actor_account_name
                ORDER BY last_event DESC
            """

            with connections['clickhouse_db'].cursor() as ch_cursor:
                ch_cursor.execute(clickhouse_query)
                for ch_row in ch_cursor.fetchall():
                    cid = ch_row[0]
                    last_event_time = ch_row[1]  # e.g., '2024-12-04 07:39:58' (naive)

                    if last_event_time is not None:
                        # Convert naive datetime to aware
                        if last_event_time.tzinfo is None:
                            last_event_time = timezone.make_aware(
                                last_event_time,
                                timezone.get_default_timezone()
                            )
                        # print("last_event_time------",last_event_time)
                        # print("threshold_time------",threshold_time)
                        # Now both are aware datetimes, so this comparison will work
                        online_status_map[cid] = (last_event_time > threshold_time)
                        print("online_status_map------",online_status_map)
        # Attach 'is_online' field to each record
        for item in results:
            item['is_online'] = online_status_map.get(str(item['user_id']), False)

        # --- 4) Calculate total_pages ---
        total_pages = math.ceil(total_records / page_size)
        # print("results------",results)
        return {
            'students': results,
            'total_pages': total_pages,
            'current_page': page,
            'page_size': page_size,
            'total_records': total_records
        }

class StudentDetails(models.Model):
    user_id = models.IntegerField(primary_key=True)
    username = models.CharField(max_length=100)
    email = models.EmailField()
    firstname = models.CharField(max_length=100)
    lastname = models.CharField(max_length=100)
    total_courses = models.IntegerField()
    archived_courses = models.IntegerField()
    active_courses = models.IntegerField()

    class Meta:
        managed = False
        app_label = 'moodle_app'

    @staticmethod
    def get_student_details(user_id):
        """
        Retrieve student details from Moodle via raw SQL query.
        """
        query = """
            SELECT
                u.id AS user_id,
                u.username,
                u.email,
                u.firstname,
                u.lastname,
                u.city,
                u.country,
                COUNT(*) AS total_courses,
                SUM(CASE
                    WHEN c.visible = 0 THEN 1
                    WHEN c.enddate > 0 AND c.enddate < UNIX_TIMESTAMP() THEN 1
                    ELSE 0
                END) AS archived_courses,
                SUM(CASE
                    WHEN c.visible = 1 AND (c.enddate = 0 OR c.enddate >= UNIX_TIMESTAMP()) THEN 1
                    ELSE 0
                END) AS active_courses
            FROM
                mdl_user u
            JOIN
                mdl_role_assignments ra ON u.id = ra.userid
            JOIN
                mdl_role r ON ra.roleid = r.id
            JOIN
                mdl_context ctx ON ra.contextid = ctx.id
            JOIN
                mdl_course c ON ctx.instanceid = c.id
            WHERE
                r.shortname = 'student'
                AND ctx.contextlevel = 50
                AND u.deleted = 0
                AND u.id = %s
            GROUP BY
                u.id, u.username, u.email, u.firstname, u.lastname
            ORDER BY
                u.firstname, u.lastname
        """
        with connections['moodle_db'].cursor() as cursor:
            cursor.execute(query, [user_id])
            row = cursor.fetchone()
        if row:
            return {
                "user_id": row[0],
                "username": row[1],
                "email": row[2],
                "firstname": row[3],
                "lastname": row[4],
                "city": row[5],
                "country": row[6],
                "total_courses": row[7],
                "archived_courses": row[8],
            }
        return None

    @staticmethod
    def get_student_basic_info(user_id):
        """
        Retrieve student details from Moodle via raw SQL query.
        """
        query = """
            SELECT
                u.id AS user_id,
                u.username,
                u.email,
                u.firstname,
                u.lastname
            FROM
                mdl_user u
            WHERE
                u.id = %s
        """
        with connections['moodle_db'].cursor() as cursor:
            cursor.execute(query, [user_id])
            row = cursor.fetchone()
        if row:
            return {
                "user_id": row[0],
                "username": row[1],
                "email": row[2],
                "firstname": row[3],
                "lastname": row[4]
            }
        return None

    @staticmethod
    def get_students_course_enrollments(user_id: int) -> List[Dict[str, Any]]:
        """
        Retrieve student course enrollments with category details from Moodle with Memcached caching.

        Args:
            user_id (int): The ID of the user whose enrollments to fetch

        Returns:
            List[Dict[str, Any]]: List of dictionaries containing course enrollment details
        """
        # Try to get cached result first
        cache_key = f'student_enrollments_{user_id}'
        cached_result = cache.get(cache_key)

        if cached_result is not None:
            return cached_result

        # If no cached result, execute the query
        query = """
            WITH RECURSIVE category_hierarchy AS (
                -- Base case: root categories
                SELECT
                    id AS category_id,
                    name AS category_name,
                    parent AS parent_id,
                    name AS full_category_path
                FROM
                    mdl_course_categories
                WHERE
                    parent = 0

                UNION ALL
                SELECT
                    child.id,
                    child.name,
                    child.parent,
                    CONCAT(parent_hierarchy.full_category_path, ' / ', child.name)
                FROM
                    mdl_course_categories child
                    INNER JOIN category_hierarchy parent_hierarchy
                        ON child.parent = parent_hierarchy.category_id
            )
            SELECT DISTINCT
                c.id,
                c.fullname,
                ch.category_id,
                ch.full_category_path
            FROM
                mdl_role_assignments ra
                INNER JOIN mdl_context ctx
                    ON ra.contextid = ctx.id
                INNER JOIN mdl_course c
                    ON ctx.instanceid = c.id
                LEFT JOIN category_hierarchy ch
                    ON c.category = ch.category_id
            WHERE
                ra.userid = %s
                AND ra.contextid IN (
                    SELECT id
                    FROM mdl_context
                    WHERE contextlevel = 50  -- Course context level
                )
                AND ra.roleid IN (
                    SELECT id
                    FROM mdl_role
                    WHERE shortname IN ('student', 'learner')
                )
            ORDER BY
                ch.full_category_path,
                c.fullname
        """

        # Process results in batches to handle large datasets efficiently
        BATCH_SIZE = 1000
        results = []

        try:
            with connections['moodle_db'].cursor() as cursor:
                cursor.execute(query, [user_id])

                while True:
                    rows = cursor.fetchmany(BATCH_SIZE)
                    if not rows:
                        break

                    batch_results = [
                        {
                            "course_id": row[0],
                            "course_name": row[1],
                            "category_id": row[2],
                            "category_path": row[3],
                        }
                        for row in rows
                    ]
                    results.extend(batch_results)
            # print("course results----------",results)
            # Cache the results for 24 hours (86400 seconds)
            cache.set(cache_key, results, timeout=86400)

            return results

        except Exception as e:
            # Log the error if you have logging configured
            logger.error(f"Error fetching enrollments for user {user_id}: {str(e)}")
            return []


    @staticmethod
    def get_student_contents_from_clickhouse(user_id):
        """
        Retrieve student content details from ClickHouse via raw SQL query.
        """
        clickhouse_query = """
            SELECT contents_id
            FROM statements_mv
            WHERE actor_account_name = %s
            GROUP BY contents_id
        """
        with connections['clickhouse_db'].cursor() as ch_cursor:
            ch_cursor.execute(clickhouse_query, [str(user_id)])
            rows = ch_cursor.fetchall()
        return [row[0] for row in rows] if rows else []

    @staticmethod
    def get_student_questions_answers(user_id):
        """
        Retrieve student  from ClickHouse via raw SQL query.
        """
        clickhouse_query = """
            SELECT DISTINCT
                actor_account_name,
                object_id,
                object_definition_name_en,
                operation_name,
                contents_id
            FROM statements_mv
            WHERE actor_account_name = %s
                AND operation_name = 'ANSWER_QUIZ'
                AND actor_account_name != ''
                AND contents_id != '';
        """
        with connections['clickhouse_db'].cursor() as ch_cursor:
            ch_cursor.execute(clickhouse_query, [str(user_id)])
            rows = ch_cursor.fetchall()
        return [row[0] for row in rows] if rows else []


    @staticmethod
    def get_student_last_action_time(user_id):
        """
        Retrieve the last action time for a student from ClickHouse.
        """
        clickhouse_query = """
            SELECT MAX(`timestamp`) AS last_action_time
            FROM statements_mv
            WHERE actor_account_name = %s
        """
        with connections['clickhouse_db'].cursor() as ch_cursor:
            ch_cursor.execute(clickhouse_query, [str(user_id)])
            row = ch_cursor.fetchone()
        last_event_time = row[0] if row else None
        threshold_time = timezone.now() - datetime.timedelta(minutes=2)
        if last_event_time is not None:
        # Convert naive datetime to aware
            if last_event_time.tzinfo is None:
                last_event_time = timezone.make_aware(
                    last_event_time,
                    timezone.get_default_timezone()
                    )
        if last_event_time > threshold_time:
            return { "is_online": True, "last_action_time": last_event_time }
        else:
            return { "is_online": False, "last_action_time": last_event_time }

    @staticmethod
    def get_student_activity_by_day(user_id):
        """
        Retrieve student activity details from ClickHouse via raw SQL query.
        """
        clickhouse_query = """
            SELECT
                toDate(timestamp) AS day,
                operation_name,
                uniqExact(_id) AS daily_distinct_count
            FROM statements_mv
            WHERE actor_account_name = %s
                AND timestamp >= today() - INTERVAL 1 YEAR
            GROUP BY
                day,
                operation_name
            ORDER BY
                day ASC,
                operation_name;

        """
        with connections['clickhouse_db'].cursor() as ch_cursor:
            ch_cursor.execute(clickhouse_query, [str(user_id)])
            rows = ch_cursor.fetchall()

        results = []
        user_ids_on_page = []
        for row in rows:
            user_id = row[0]
            user_ids_on_page.append(user_id)
            results.append({
                "date": row[0],
                "operation_name": row[1],
                "daily_count": row[2],
            })
        return results


    @staticmethod
    def get_full_student_details(user_id):
        """
        Combine Moodle and ClickHouse data for a student.
        """
        moodle_details = StudentDetails.get_student_details(user_id)
        if not moodle_details:
            return None

        clickhouse_contents = StudentDetails.get_student_contents_from_clickhouse(user_id)
        moodle_details["contents_ids"] = clickhouse_contents
        clickhouse_quiz_answers = StudentDetails.get_student_questions_answers(user_id)
        moodle_details["quiz_answers"] = clickhouse_quiz_answers
        last_action_time = StudentDetails.get_student_last_action_time(user_id)
        moodle_details["last_action_time"] = last_action_time
        moodle_details["enrollments"] = StudentDetails.get_students_course_enrollments(user_id)
        moodle_details["activity_by_day"] = StudentDetails.get_student_activity_by_day(user_id)

        return moodle_details

    @staticmethod
    def get_student_last_access_course_list(user_id):
        """
        Retrieve the last access course list for a student from ClickHouse.
        """
        my_sql_query = """
        SELECT u.firstname, u.lastname, c.fullname AS course_name, ula.timeaccess, c.id AS course_id, cc.name AS category_name
        FROM mdl_user_lastaccess ula
        JOIN mdl_user u ON u.id = ula.userid
        JOIN mdl_course c ON c.id = ula.courseid
        LEFT JOIN mdl_course_categories cc ON c.category = cc.id
        WHERE u.id = %s
        AND ula.timeaccess >= UNIX_TIMESTAMP(NOW() - INTERVAL 30 DAY)
        ORDER BY ula.timeaccess DESC;
        """
        with connections['moodle_db'].cursor() as cursor:
            cursor.execute(my_sql_query, [user_id])
            rows = cursor.fetchall()

        # Convert rows of tuples to list of dictionaries with named keys
        result = []
        for row in rows:
            # Format the Unix timestamp to a readable date
            access_time = datetime.datetime.fromtimestamp(row[3])
            formatted_time = access_time.strftime('%Y-%m-%d %H:%M:%S')

            result.append({
                'firstname': row[0],
                'lastname': row[1],
                'course_name': row[2],
                'timeaccess': row[3],
                'timeaccess_formatted': formatted_time,
                'course_id': row[4],
                'category_name': row[5] if row[5] else 'Uncategorized',
                'category_path': row[5] if row[5] else 'Uncategorized'  # For backwards compatibility
            })

        return result


class StudentActivityLive(models.Model):
    """
    Optional model if you need to store/cache any activity data locally
    """
    user_id = models.CharField(max_length=255)
    timestamp = models.DateTimeField()
    operation_name = models.CharField(max_length=255)
    details = models.JSONField(null=True, blank=True)

    class Meta:
        ordering = ['-timestamp']