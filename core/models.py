import logging
import json
import datetime
from django.db import models
from django.db import connections
from clickhouse_backend.models import ClickhouseModel
from django.conf import settings

from django.http import JsonResponse
logger = logging.getLogger(__name__)


class MoodleUser(models.Model):
    id = models.AutoField(primary_key=True)
    username = models.CharField(max_length=255)
    email = models.CharField(max_length=255)
    firstname = models.CharField(max_length=255)
    lastname = models.CharField(max_length=255)

    class Meta:
        db_table = 'mdl_user'
        managed = False
        app_label = 'moodle_app'

class StudentCount(models.Model):
    student_count = models.IntegerField()

    class Meta:
        managed = False  # This is not tied to an actual table
        app_label = 'moodle_app'

    @staticmethod
    def get_student_count():
        query = """
        SELECT COUNT(DISTINCT ra.userid) AS student_count
        FROM mdl_role_assignments ra
        JOIN mdl_role r    ON ra.roleid = r.id
        JOIN mdl_context ctx ON ra.contextid = ctx.id
        JOIN mdl_course c  ON ctx.instanceid = c.id
        JOIN mdl_user u    ON u.id = ra.userid
        WHERE r.shortname = 'student'
        AND ctx.contextlevel = 50
        AND u.deleted = 0
        AND u.suspended = 0
        AND c.visible = 1
        AND c.id != 1
        """
        with connections['moodle_db'].cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result else 0

    @staticmethod
    def get_student_count_by_day():
        query = """
        SELECT FROM_UNIXTIME(timecreated) as day, COUNT(*) as total FROM mdl_user
        WHERE timecreated >= UNIX_TIMESTAMP(CURDATE() - INTERVAL 6 DAY)
        GROUP BY day ORDER BY day ASC;
        """

        with connections['moodle_db'].cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            data = [[row[0].isoformat(), row[1]] for row in rows]
            return json.dumps(data)



class ActiveUsers(models.Model):
    user_id = models.IntegerField(primary_key=True)
    last_login = models.DateTimeField()

    class Meta:
        managed = False
        app_label = 'clickhouse_db'

class TotalCourses(models.Model):
    course_count = models.IntegerField()

    class Meta:
        managed = False
        app_label = 'moodle_app'

    @staticmethod
    def get_course_count():
        query = """
        SELECT id, fullname, shortname, category
        FROM mdl_course
        WHERE visible = 1
        ORDER BY fullname;
        """
        with connections['moodle_db'].cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return len(rows)

    @staticmethod
    def get_course_count_by_day():
        query = """
        SELECT FROM_UNIXTIME(timecreated) as day, COUNT(*) as total FROM mdl_course
        WHERE timecreated >= UNIX_TIMESTAMP(CURDATE() - INTERVAL 6 DAY)
        GROUP BY day ORDER BY day ASC;
        """
        with connections['moodle_db'].cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            data = [[row[0].isoformat(sep=' '), row[1]] for row in rows]
            return json.dumps(data)


class TotalContents(models.Model):
    content_count = models.IntegerField()

    class Meta:
        managed = False
        app_label = 'bookroll_app'

    @staticmethod
    def get_content_count():
        query = """
        SELECT COUNT(DISTINCT contents_id) AS content_count
        FROM br_contents
        """
        with connections['bookroll_db'].cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result else 0

    @staticmethod
    def get_content_count_by_day():
        query = """
        SELECT DATE(created) as day, COUNT(*) as total FROM br_contents
        WHERE created >= CURDATE() - INTERVAL 6 DAY
        GROUP BY day ORDER BY day ASC;
        """
        with connections['bookroll_db'].cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            data = [[row[0].isoformat(), row[1]] for row in rows]
            return json.dumps(data)


class ActiveStudents(models.Model):
    """Model to track active students from ClickHouse"""
    total_active_students = models.IntegerField()

    @classmethod
    def get_active_students(cls):
        query = """
        SELECT COUNT(DISTINCT actor_account_name) AS total_active_students
        FROM statements_mv
        WHERE actor_name_role == 'student'
        """
        print("Connecting to ClickHouse..")
        try:
            with connections['clickhouse_db'].cursor() as cursor:
                print("Executing query.....")
                cursor.execute(query)
                result = cursor.fetchone()
                print(f"Query result: {result}")
                return result[0] if result else 0
        except Exception as e:
            logger.error(f"Error fetching active students: {str(e)}")
            print(f"Error details: {str(e)}")
            return 0

    @staticmethod
    def get_active_students_by_day():
        query = """
        SELECT toDate(`timestamp`) as date, COUNT(DISTINCT actor_account_name) AS total_active_students
        FROM saikyo_new.statements_mv
        WHERE actor_name_role == 'student'
        AND `timestamp` >= today() - INTERVAL 6 DAY
        GROUP BY date ORDER BY date ASC;
        """
        with connections['clickhouse_db'].cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            data = [[day.isoformat(), total] for day, total in rows]
            return json.dumps(data, ensure_ascii=False)

    class Meta:
        managed = False
        db_table = 'statements_mv'
        app_label = 'clickhouse_app'



class MostActiveContents(models.Model):
    """Model to track most active contents from ClickHouse"""
    content_id = models.IntegerField(primary_key=True)
    content_title = models.CharField(max_length=255)
    total_activities = models.IntegerField()

    @classmethod
    def get_activity_types(cls):
        """Get available activity types for filtering from database"""
        query = """
            SELECT DISTINCT operation_name
            FROM statements_mv
            WHERE operation_name != ''
                AND actor_name_role = 'student'
            ORDER BY operation_name
        """

        with connections['clickhouse_db'].cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

            # Start with "All Activities" option
            activity_types = [('', 'All Activities')]

            # Add each operation name from database
            for row in rows:
                operation_name = row[0]
                activity_types.append((operation_name, operation_name))

            return activity_types

    @classmethod
    def get_most_active_contents(cls, limit=10, offset=0, search=None, activity_type=None):
        query = """
            SELECT
                contents_id,
                contents_name,
                uniqExact(_id) AS total_activities,
                object_id
            FROM statements_mv
            WHERE contents_id != ''
                AND actor_name_role = 'student'
        """

        if search:
            query += f" AND contents_name ILIKE '%{search}%'"

        if activity_type:
            query += f" AND operation_name = '{activity_type}'"

        query += """
            GROUP BY
                contents_id,
                contents_name,
                object_id
            ORDER BY
                total_activities DESC
        """

        if limit is not None:
            query += f" LIMIT {offset}, {limit}"

        with connections['clickhouse_db'].cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            return [
                {"id": row[0], "contents_name": row[1], "total_activities": row[2], "object_id": row[3]}
                for row in rows
            ]

    @classmethod
    def get_most_active_contents_count(cls, search=None, activity_type=None):
        base_query = """
            SELECT
                contents_id
            FROM statements_mv
            WHERE contents_id != ''
                AND actor_name_role = 'student'
        """

        if search:
            base_query += f" AND contents_name ILIKE '%{search}%'"

        if activity_type:
            base_query += f" AND operation_name = '{activity_type}'"

        query = f"""
            SELECT
                count() as total_count
            FROM (
                {base_query}
                GROUP BY
                    contents_id
            )
        """

        with connections['clickhouse_db'].cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0] if result else 0

    @classmethod
    def get_most_active_contents_with_breakdown(cls, limit=10, offset=0, search=None, activity_type=None):
        """Get most active contents with activity breakdown for inline charts"""
        # First get the basic content data
        contents = cls.get_most_active_contents(limit, offset, search, activity_type)

        if not contents:
            return []

        # Get content IDs for breakdown query
        content_ids = [content['id'] for content in contents]
        content_ids_str = "', '".join(content_ids)

        # Query to get activity breakdown for these contents
        breakdown_query = f"""
            SELECT
                contents_id,
                operation_name,
                uniqExact(_id) AS activity_count
            FROM statements_mv
            WHERE contents_id IN ('{content_ids_str}')
                AND operation_name != ''
                AND actor_name_role = 'student'
        """

        if search:
            breakdown_query += f" AND contents_name ILIKE '%{search}%'"

        breakdown_query += """
            GROUP BY
                contents_id,
                operation_name
            ORDER BY
                contents_id,
                activity_count DESC
        """

        with connections['clickhouse_db'].cursor() as cursor:
                cursor.execute(breakdown_query)
                breakdown_rows = cursor.fetchall()

                # Organize breakdown data by content_id
        breakdown_data = {}
        for row in breakdown_rows:
            content_id = row[0]
            operation_name = row[1]
            activity_count = row[2]

            if content_id not in breakdown_data:
                breakdown_data[content_id] = []
            breakdown_data[content_id].append((operation_name, activity_count))

        # Add breakdown data to contents (limit to top 10 activities per content)
        for content in contents:
            content_id = content['id']
            activities = breakdown_data.get(content_id, [])

            # Sort by activity count (descending) and limit to top 10
            activities.sort(key=lambda x: x[1], reverse=True)
            top_activities = activities[:10]

            # Convert to dictionary for template
            content['activity_breakdown'] = dict(top_activities)

            # Calculate total for verification
            content['breakdown_total'] = sum(content['activity_breakdown'].values())

        return contents

    class Meta:
        managed = False
        db_table = 'statements_mv'
        app_label = 'clickhouse_app'


class DailyActiveUsers(models.Model):
    """Model to track daily active users from ClickHouse"""
    date = models.DateField(primary_key=True)
    total_active_users = models.IntegerField()

    @classmethod
    def get_daily_active_users(cls):
        query = """
        SELECT
            toDate(timestamp) AS date,
            COUNT(DISTINCT actor_account_name) AS total_active_users
        FROM statements_mv
        WHERE timestamp >= today() - 30
            AND actor_account_name != ''
            AND actor_name_role = 'student'
        GROUP BY date
        ORDER BY date
        """
        with connections['clickhouse_db'].cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return [
                    {"date": row[0].isoformat(), "total_active_users": row[1]}
                    for row in rows
                ]

    class Meta:
        managed = False
        db_table = 'statements_mv'
        app_label = 'clickhouse_app'

class DailyActivities(models.Model):
    """Model to track daily activities from ClickHouse"""
    date = models.DateField(primary_key=True)
    total_activities = models.IntegerField()

    @classmethod
    def get_daily_activities(cls):
        query = """
            SELECT
                toDate(timestamp) AS date,
                uniqExact(_id) AS total_activities
            FROM statements_mv
            WHERE timestamp >= today() - 30
                AND actor_name_role = 'student'
            GROUP BY date
            ORDER BY date
        """
        with connections['clickhouse_db'].cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return [
                    {"date": row[0].isoformat(), "total_activities": row[1]}
                    for row in rows
                ]

    class Meta:
        managed = False
        db_table = 'statements_mv'
        app_label = 'clickhouse_app'

class MostActiveStudents(models.Model):
    """Model to track most active students from ClickHouse"""
    user_id = models.IntegerField(primary_key=True)
    user_name = models.CharField(max_length=255)
    total_activities = models.IntegerField()

    @staticmethod
    def _get_time_filter(time_frame):
        """
        Helper method to generate time filter SQL based on time frame.
        """
        if time_frame == 'this_week':
            return "AND timestamp >= toStartOfWeek(today())"
        elif time_frame == 'this_month':
            return "AND timestamp >= toStartOfMonth(today())"
        elif time_frame == 'this_year':
            return "AND timestamp >= toStartOfYear(today())"
        elif time_frame == 'last_3_months':
            return "AND timestamp >= today() - INTERVAL 3 MONTH"
        elif time_frame == 'academic_year':
            # Academic year: April 1 to March 31 (next year)
            return """AND timestamp >=
                CASE
                    WHEN toMonth(today()) >= 4
                    THEN toDate(concat(toString(toYear(today())), '-04-01'))
                    ELSE toDate(concat(toString(toYear(today()) - 1), '-04-01'))
                END
            AND timestamp <=
                CASE
                    WHEN toMonth(today()) >= 4
                    THEN toDate(concat(toString(toYear(today()) + 1), '-03-31'))
                    ELSE toDate(concat(toString(toYear(today())), '-03-31'))
                END"""
        else:
            # Default to last 3 months
            return "AND timestamp >= today() - INTERVAL 3 MONTH"

    @staticmethod
    def _get_daily_trends_days(time_frame):
        """
        Helper method to get the number of days for daily trends based on time frame.
        """
        if time_frame == 'this_week':
            return 7
        elif time_frame == 'this_month':
            return 31
        elif time_frame == 'this_year':
            return 365
        elif time_frame == 'last_3_months':
            return 90
        elif time_frame == 'academic_year':
            return 365
        else:
            return 90

    @classmethod
    def get_most_active_students(cls, limit=10, offset=0, search=None):
        query = """
        SELECT
            actor_account_name,
            uniqExact(_id) AS total_activities
        FROM statements_mv
        WHERE actor_name_role == 'student'
            AND actor_account_name != ''
        """

        # For student search, we'll need to handle this differently
        # as we need to search by the Moodle user names which aren't directly in ClickHouse

        query += """
        GROUP BY actor_account_name
        ORDER BY total_activities DESC
        """

        if limit is not None:
            query += f" LIMIT {offset}, {limit}"

        with connections['clickhouse_db'].cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
        return [
            {
                "actor_account_name": row[0],
                "total_activities": row[1]
            }
            for row in rows
        ]

    @classmethod
    def get_most_active_students_count(cls, search=None):
        base_query = """
            SELECT
                actor_account_name
            FROM statements_mv
            WHERE actor_name_role == 'student'
                AND actor_account_name != ''
        """

        # Note: For student search, we handle filtering in Python after getting data
        # from Moodle, so we don't add search condition here

        query = f"""
            SELECT
                count() as total_count
            FROM (
                {base_query}
                GROUP BY
                    actor_account_name
            )
        """

        with connections['clickhouse_db'].cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0] if result else 0

    @classmethod
    def get_most_active_students_with_details(cls, limit=10, offset=0, search=None):
        clickhouse_rows = cls.get_most_active_students(limit=limit, offset=offset)
        actor_account_names = [row['actor_account_name'] for row in clickhouse_rows]

        # For search, we'll filter after getting Moodle data
        if search:
            moodle_users = MoodleUser.objects.using('moodle_db').filter(
                id__in=actor_account_names
            ).filter(
                models.Q(firstname__icontains=search) |
                models.Q(lastname__icontains=search) |
                models.Q(username__icontains=search)
            )
        else:
            moodle_users = MoodleUser.objects.using('moodle_db').filter(
                id__in=actor_account_names
            )

        moodle_user_dict = {u.id: u for u in moodle_users}

        results = []
        for row in clickhouse_rows:
            username = row['actor_account_name']
            total_activities = row['total_activities']
            moodle_user = moodle_user_dict.get(int(username))

            # Skip if search is provided and this user doesn't match
            if search and not moodle_user:
                continue

            if moodle_user:
                name = moodle_user.firstname + ' ' + moodle_user.lastname
                if search and search.lower() not in name.lower() and search.lower() not in moodle_user.username.lower():
                    continue

                results.append({
                    "moodle_id": moodle_user.id,
                    "username": moodle_user.username,
                    "name": name,
                    "total_activities": total_activities,
                })
            else:
                # If no search or the search term is in username
                if not search or search.lower() in username.lower():
                    results.append({
                        "moodle_id": username,
                        "username": username,
                        "name": username,
                        "total_activities": total_activities,
                    })

        return results

    @classmethod
    def get_student_activity_analytics(cls, time_frame='last_3_months'):
        """
        Get comprehensive analytics about student activities without exposing personal information.
        Returns aggregated statistics, distributions, and insights.
        """
        try:
            # Get time filter for the selected time frame
            time_filter = cls._get_time_filter(time_frame)

            with connections['clickhouse_db'].cursor() as cursor:
                # First, let's check if we have any data at all
                cursor.execute(f"""
                    SELECT COUNT(*) as total_records
                    FROM statements_mv
                    WHERE actor_name_role == 'student'
                        AND actor_account_name != ''
                        {time_filter}
                """)

                total_records = cursor.fetchone()
                logger.info(f"Total student records found for {time_frame}: {total_records[0] if total_records else 0}")

                if not total_records or total_records[0] == 0:
                    logger.warning(f"No student data found in ClickHouse for {time_frame}")
                    return {
                        'overall_stats': {
                            'total_students': 0,
                            'total_activities': 0,
                            'avg_activities': 0,
                            'median_activities': 0,
                            'std_dev_activities': 0,
                            'min_activities': 0,
                            'max_activities': 0
                        },
                        'activity_distribution': [],
                        'top_operations': [],
                        'daily_trends': []
                    }

                # Get overall activity statistics
                # First, get the individual student activity counts
                cursor.execute(f"""
                    SELECT
                        actor_account_name,
                        uniqExact(_id) AS total_activities
                    FROM statements_mv
                    WHERE actor_name_role == 'student'
                        AND actor_account_name != ''
                        {time_filter}
                    GROUP BY actor_account_name
                """)
                student_activities = cursor.fetchall()

                # Calculate statistics in Python to avoid nested aggregates
                if student_activities:
                    activity_counts = [row[1] for row in student_activities]
                    total_students = len(activity_counts)
                    total_activities = sum(activity_counts)
                    avg_activities = total_activities / total_students if total_students > 0 else 0

                    # Calculate median
                    sorted_activities = sorted(activity_counts)
                    n = len(sorted_activities)
                    if n % 2 == 0:
                        median_activities = (sorted_activities[n//2-1] + sorted_activities[n//2]) / 2
                    else:
                        median_activities = sorted_activities[n//2]

                    # Calculate standard deviation
                    if total_students > 1:
                        variance = sum((x - avg_activities) ** 2 for x in activity_counts) / total_students
                        std_dev_activities = variance ** 0.5
                    else:
                        std_dev_activities = 0

                    min_activities = min(activity_counts)
                    max_activities = max(activity_counts)

                    stats_row = (total_students, total_activities, avg_activities, median_activities,
                               std_dev_activities, min_activities, max_activities)
                else:
                    stats_row = (0, 0, 0, 0, 0, 0, 0)

                logger.info(f"Stats row: {stats_row}")

                # Calculate dynamic activity distribution ranges based on actual data
                distribution_data = {}
                distribution_rows = []

                if student_activities and len(activity_counts) > 0:
                    # Calculate percentiles for dynamic ranges using pure Python
                    def calculate_percentile(data, percentile):
                        """Calculate percentile using pure Python"""
                        sorted_data = sorted(data)
                        n = len(sorted_data)
                        if n == 0:
                            return 0
                        index = (percentile / 100.0) * (n - 1)
                        if index.is_integer():
                            return sorted_data[int(index)]
                        else:
                            lower = sorted_data[int(index)]
                            upper = sorted_data[int(index) + 1]
                            return lower + (upper - lower) * (index - int(index))

                    # Calculate percentiles (10th, 25th, 50th, 75th, 90th, 95th)
                    percentiles = [10, 25, 50, 75, 90, 95]
                    percentile_values = [calculate_percentile(activity_counts, p) for p in percentiles]

                    # Create dynamic ranges based on data distribution
                    ranges = []

                    # Handle edge case where all students have the same activity count
                    if min_activities == max_activities:
                        ranges = [
                            {'min': min_activities, 'max': min_activities, 'label': f'{min_activities}'}
                        ]
                    else:
                        # Create ranges based on data characteristics
                        if max_activities <= 100:
                            # For smaller datasets, use smaller increments
                            ranges = [
                                {'min': 1, 'max': 10, 'label': '1-10'},
                                {'min': 11, 'max': 25, 'label': '11-25'},
                                {'min': 26, 'max': 50, 'label': '26-50'},
                                {'min': 51, 'max': 75, 'label': '51-75'},
                                {'min': 76, 'max': 100, 'label': '76-100'},
                                {'min': 101, 'max': float('inf'), 'label': '100+'}
                            ]
                        elif max_activities <= 1000:
                            # For medium datasets
                            ranges = [
                                {'min': 1, 'max': 50, 'label': '1-50'},
                                {'min': 51, 'max': 100, 'label': '51-100'},
                                {'min': 101, 'max': 250, 'label': '101-250'},
                                {'min': 251, 'max': 500, 'label': '251-500'},
                                {'min': 501, 'max': 1000, 'label': '501-1K'},
                                {'min': 1001, 'max': float('inf'), 'label': '1K+'}
                            ]
                        elif max_activities <= 10000:
                            # For large datasets
                            ranges = [
                                {'min': 1, 'max': 100, 'label': '1-100'},
                                {'min': 101, 'max': 500, 'label': '101-500'},
                                {'min': 501, 'max': 1000, 'label': '501-1K'},
                                {'min': 1001, 'max': 2500, 'label': '1K-2.5K'},
                                {'min': 2501, 'max': 5000, 'label': '2.5K-5K'},
                                {'min': 5001, 'max': 10000, 'label': '5K-10K'},
                                {'min': 10001, 'max': float('inf'), 'label': '10K+'}
                            ]
                        elif max_activities <= 100000:
                            # For very large datasets
                            ranges = [
                                {'min': 1, 'max': 1000, 'label': '1-1K'},
                                {'min': 1001, 'max': 5000, 'label': '1K-5K'},
                                {'min': 5001, 'max': 10000, 'label': '5K-10K'},
                                {'min': 10001, 'max': 25000, 'label': '10K-25K'},
                                {'min': 25001, 'max': 50000, 'label': '25K-50K'},
                                {'min': 50001, 'max': 100000, 'label': '50K-100K'},
                                {'min': 100001, 'max': float('inf'), 'label': '100K+'}
                            ]
                        else:
                            # For extremely large datasets (millions)
                            ranges = [
                                {'min': 1, 'max': 10000, 'label': '1-10K'},
                                {'min': 10001, 'max': 50000, 'label': '10K-50K'},
                                {'min': 50001, 'max': 100000, 'label': '50K-100K'},
                                {'min': 100001, 'max': 500000, 'label': '100K-500K'},
                                {'min': 500001, 'max': 1000000, 'label': '500K-1M'},
                                {'min': 1000001, 'max': 5000000, 'label': '1M-5M'},
                                {'min': 5000001, 'max': float('inf'), 'label': '5M+'}
                            ]

                    # Count students in each range
                    for range_def in ranges:
                        count = 0
                        for activity_count in activity_counts:
                            if range_def['min'] <= activity_count <= range_def['max']:
                                count += 1

                        # Only include ranges that have students
                        if count > 0:
                            distribution_rows.append((range_def['label'], count))

                    logger.info(f"Dynamic ranges created based on max_activities={max_activities}: {[r['label'] for r in ranges]}")
                    logger.info(f"Distribution: {distribution_rows}")

                else:
                    # Fallback to default ranges if no data
                    distribution_rows = [
                        ('1-10', 0),
                        ('11-50', 0),
                        ('51-100', 0),
                        ('101-500', 0),
                        ('501-1000', 0),
                        ('1000+', 0)
                    ]

                # Get top operation types across all students
                cursor.execute(f"""
                    SELECT
                        operation_name,
                        uniqExact(_id) as total_count,
                        COUNT(DISTINCT actor_account_name) as student_count
                    FROM statements_mv
                    WHERE actor_name_role == 'student'
                        AND actor_account_name != ''
                        AND operation_name != ''
                        {time_filter}
                    GROUP BY operation_name
                    ORDER BY total_count DESC
                    LIMIT 15
                """)
                operation_rows = cursor.fetchall()

                # Get daily activity trends based on time frame
                cursor.execute(f"""
                    SELECT
                        toDate(timestamp) as date,
                        COUNT(DISTINCT actor_account_name) as active_students,
                        uniqExact(_id) as total_activities
                    FROM statements_mv
                    WHERE actor_name_role == 'student'
                        AND actor_account_name != ''
                        {time_filter}
                    GROUP BY date
                    ORDER BY date
                """)
                daily_trends = cursor.fetchall()

                logger.info(f"Distribution rows: {len(distribution_rows)}")
                logger.info(f"Operation rows: {len(operation_rows)}")
                logger.info(f"Daily trends: {len(daily_trends)}")

                return {
                    'overall_stats': {
                        'total_students': stats_row[0] if stats_row else 0,
                        'total_activities': stats_row[1] if stats_row else 0,
                        'avg_activities': round(stats_row[2], 2) if stats_row and stats_row[2] else 0,
                        'median_activities': stats_row[3] if stats_row else 0,
                        'std_dev_activities': round(stats_row[4], 2) if stats_row and stats_row[4] else 0,
                        'min_activities': stats_row[5] if stats_row else 0,
                        'max_activities': stats_row[6] if stats_row else 0
                    },
                    'activity_distribution': [
                        {'range': row[0], 'count': row[1]}
                        for row in distribution_rows
                    ],
                    'top_operations': [
                        {
                            'operation': row[0],
                            'total_count': row[1],
                            'student_count': row[2]
                        }
                        for row in operation_rows
                    ],
                    'daily_trends': [
                        {
                            'date': row[0].isoformat(),
                            'active_students': row[1],
                            'total_activities': row[2]
                        }
                        for row in daily_trends
                    ]
                }

        except Exception as e:
            logger.error(f"Error fetching student activity analytics: {str(e)}")
            return {
                'overall_stats': {
                    'total_students': 0,
                    'total_activities': 0,
                    'avg_activities': 0,
                    'median_activities': 0,
                    'std_dev_activities': 0,
                    'min_activities': 0,
                    'max_activities': 0
                },
                'activity_distribution': [],
                'top_operations': [],
                'daily_trends': []
            }

    @classmethod
    def get_operation_engagement_patterns(cls, time_frame='last_3_months'):
        """
        Analyze engagement patterns by operation type to understand learning behaviors.
        """
        try:
            # Get time filter for the selected time frame
            time_filter = cls._get_time_filter(time_frame)

            with connections['clickhouse_db'].cursor() as cursor:
                # Get operation patterns by hour of day
                cursor.execute(f"""
                    SELECT
                        operation_name,
                        toHour(timestamp) as hour,
                        uniqExact(_id) as activity_count
                    FROM statements_mv
                    WHERE actor_name_role == 'student'
                        AND actor_account_name != ''
                        AND operation_name != ''
                        {time_filter}
                    GROUP BY operation_name, hour
                    ORDER BY operation_name, hour
                """)
                hourly_patterns = cursor.fetchall()

                # Get operation patterns by day of week
                cursor.execute(f"""
                    SELECT
                        operation_name,
                        toDayOfWeek(timestamp) as day_of_week,
                        uniqExact(_id) as activity_count
                    FROM statements_mv
                    WHERE actor_name_role == 'student'
                        AND actor_account_name != ''
                        AND operation_name != ''
                        {time_filter}
                    GROUP BY operation_name, day_of_week
                    ORDER BY operation_name, day_of_week
                """)
                daily_patterns = cursor.fetchall()

                # Organize data by operation
                operations_data = {}

                # Process hourly patterns
                for row in hourly_patterns:
                    operation = row[0]
                    hour = row[1]
                    count = row[2]

                    if operation not in operations_data:
                        operations_data[operation] = {
                            'hourly': [0] * 24,
                            'daily': [0] * 7
                        }

                    operations_data[operation]['hourly'][hour] = count

                    # Process daily patterns
                    for row in daily_patterns:
                        operation = row[0]
                        day = row[1] - 1  # Convert to 0-based index (Monday=0)
                        count = row[2]

                        if operation not in operations_data:
                            operations_data[operation] = {
                                'hourly': [0] * 24,
                                'daily': [0] * 7
                            }

                        operations_data[operation]['daily'][day] = count

                    return operations_data

        except Exception as e:
            logger.error(f"Error fetching operation engagement patterns: {str(e)}")
            return {}

    @classmethod
    def get_learning_insights(cls, time_frame='last_3_months'):
        """
        Generate educational insights from student activity data.
        """
        try:
            # Get time filter for the selected time frame
            time_filter = cls._get_time_filter(time_frame)

            with connections['clickhouse_db'].cursor() as cursor:
                # Get content interaction patterns
                cursor.execute(f"""
                    SELECT
                        operation_name,
                        COUNT(DISTINCT contents_id) as unique_contents,
                        COUNT(DISTINCT actor_account_name) as unique_students,
                        uniqExact(_id) as total_interactions
                    FROM statements_mv
                    WHERE actor_name_role == 'student'
                        AND actor_account_name != ''
                        AND operation_name != ''
                        AND contents_id != ''
                        {time_filter}
                    GROUP BY operation_name
                    ORDER BY total_interactions DESC
                """)
                content_interactions = cursor.fetchall()

                # Get student engagement levels
                # Get individual student activity counts first
                cursor.execute(f"""
                    SELECT
                        actor_account_name,
                        uniqExact(_id) AS total_activities
                    FROM statements_mv
                    WHERE actor_name_role == 'student'
                        AND actor_account_name != ''
                        {time_filter}
                    GROUP BY actor_account_name
                """)
                student_engagement_data = cursor.fetchall()

                # Calculate engagement levels in Python
                engagement_stats = {
                    'High Engagement': {'count': 0, 'total_activities': 0},
                    'Medium Engagement': {'count': 0, 'total_activities': 0},
                    'Low Engagement': {'count': 0, 'total_activities': 0},
                    'Minimal Engagement': {'count': 0, 'total_activities': 0}
                }

                for _, activity_count in student_engagement_data:
                    if activity_count >= 1000:
                        level = 'High Engagement'
                    elif activity_count >= 100:
                        level = 'Medium Engagement'
                    elif activity_count >= 10:
                        level = 'Low Engagement'
                    else:
                        level = 'Minimal Engagement'

                    engagement_stats[level]['count'] += 1
                    engagement_stats[level]['total_activities'] += activity_count

                # Convert to the expected format
                engagement_levels = []
                for level in ['High Engagement', 'Medium Engagement', 'Low Engagement', 'Minimal Engagement']:
                    count = engagement_stats[level]['count']
                    avg_activities = engagement_stats[level]['total_activities'] / count if count > 0 else 0
                    engagement_levels.append((level, count, avg_activities))

                logger.info(f"Content interactions found: {len(content_interactions)}")
                logger.info(f"Engagement levels found: {len(engagement_levels)}")

                return {
                    'content_interactions': [
                        {
                            'operation': row[0],
                            'unique_contents': row[1],
                            'unique_students': row[2],
                            'total_interactions': row[3]
                        }
                        for row in content_interactions
                    ],
                    'engagement_levels': [
                        {
                            'level': row[0],
                            'student_count': row[1],
                            'avg_activities': round(row[2], 2)
                        }
                        for row in engagement_levels
                    ]
                }

        except Exception as e:
            logger.error(f"Error fetching learning insights: {str(e)}")
            return {
                'content_interactions': [],
                'engagement_levels': []
            }

    @classmethod
    def get_hourly_activity_heatmap(cls, time_frame='last_3_months'):
        """
        Generate hourly activity heatmap data showing student activity patterns by actual dates and hours.
        Combines School Time and Non-School Time activities in a single heatmap with different color coding.

        School Time: Weekdays during school hours (excluding holidays) - GREEN colors
        Non-School Time: Weekends, holidays, outside school hours - ORANGE colors

        Returns data suitable for ApexCharts heatmap visualization in GitHub contribution style.
        X-axis: Days (actual calendar dates)
        Y-axis: Hours (00:00 to 23:00)
        """
        logger.info(f"Hourly activity heatmap hitting >>>----------------: {time_frame}")
        try:
            # Get time filter for the selected time frame
            time_filter = cls._get_time_filter(time_frame)

            with connections['clickhouse_db'].cursor() as cursor:
                # Get activity counts grouped by actual date and hour
                # Convert timestamp to JST (UTC+9) for proper local time analysis
                cursor.execute(f"""
                    SELECT
                        toDate(addHours(timestamp, 9)) as activity_date,
                        toHour(addHours(timestamp, 9)) as hour_of_day,
                        toDayOfWeek(addHours(timestamp, 9)) as day_of_week,
                        uniqExact(_id) as activity_count,
                        COUNT(DISTINCT actor_account_name) as student_count
                    FROM statements_mv
                    WHERE actor_name_role == 'student'
                        AND actor_account_name != ''
                        {time_filter}
                    GROUP BY activity_date, hour_of_day, day_of_week
                    ORDER BY activity_date, hour_of_day
                """)

                hourly_data = cursor.fetchall()

                if not hourly_data:
                    logger.warning("No hourly activity data found")
                    return {
                        'combined_series': [],
                        'stats': {
                            'max_school_activity': 0,
                            'max_non_school_activity': 0,
                            'total_school_activity': 0,
                            'total_non_school_activity': 0
                        },
                        'date_range': [],
                        'week_boundaries': []
                    }

                # Get date range from the data
                dates = sorted(set([row[0] for row in hourly_data]))
                start_date = min(dates)
                end_date = max(dates)

                # Create a complete date range (fill gaps)
                current_date = start_date
                complete_dates = []
                while current_date <= end_date:
                    complete_dates.append(current_date)
                    current_date += datetime.timedelta(days=1)

                # Get Japanese holidays for the date range
                from holiday.models import JapaneseHoliday
                holiday_dates = set()
                holiday_info = {}  # Store holiday names
                holidays = JapaneseHoliday.objects.filter(
                    date__gte=start_date,
                    date__lte=end_date
                ).values_list('date', 'name', flat=False)

                for holiday_date, holiday_name in holidays:
                    holiday_dates.add(holiday_date)
                    holiday_info[holiday_date.isoformat()] = holiday_name

                # Get school time settings
                school_start_time = getattr(settings, 'SCHOOL_START_TIME', '09:00')
                school_end_time = getattr(settings, 'SCHOOL_END_TIME', '16:00')

                # Parse school hours
                school_start_hour, school_start_minute = map(int, school_start_time.split(':'))
                school_end_hour, school_end_minute = map(int, school_end_time.split(':'))

                # Create combined activity matrix: [hour][date] = {activity_count, is_school_time}
                activity_matrix = {}
                school_values = []
                non_school_values = []

                for hour in range(24):
                    activity_matrix[hour] = {}
                    for date in complete_dates:
                        activity_matrix[hour][date] = {'activity_count': 0, 'is_school_time': False, 'student_count': 0}

                # Fill the matrix with actual data, categorizing as school vs non-school time
                for row in hourly_data:
                    activity_date = row[0]
                    hour_of_day = row[1]
                    day_of_week = row[2]  # 1=Monday, 7=Sunday
                    activity_count = row[3]
                    student_count = row[4]

                    if 0 <= hour_of_day < 24:
                        # Determine if this is school time or non-school time
                        is_school_time = False

                        # Check if it's a weekday (Monday=1 to Friday=5)
                        if 1 <= day_of_week <= 5:
                            # Check if it's not a holiday
                            if activity_date not in holiday_dates:
                                # Check if it's within school hours
                                activity_time_minutes = hour_of_day * 60
                                school_start_minutes = school_start_hour * 60 + school_start_minute
                                school_end_minutes = school_end_hour * 60 + school_end_minute

                                if school_start_minutes <= activity_time_minutes < school_end_minutes:
                                    is_school_time = True

                        # Store in matrix
                        activity_matrix[hour_of_day][activity_date] = {
                            'activity_count': activity_count,
                            'is_school_time': is_school_time,
                            'student_count': student_count
                        }

                        # Collect values for statistics
                        if is_school_time:
                            school_values.append(activity_count)
                        else:
                            non_school_values.append(activity_count)

                # Convert to ApexCharts heatmap format with combined data
                combined_series = []
                for hour in range(24):
                    hour_data = []
                    for date in complete_dates:
                        data_point = activity_matrix[hour][date]
                        hour_data.append({
                            'x': date.isoformat(),
                            'y': data_point['activity_count'],
                            'school_time': data_point['is_school_time'],
                            'student_count': data_point['student_count']
                        })

                    combined_series.append({
                        'name': f"{hour:02d}:00",
                        'data': hour_data
                    })

                # Calculate statistics for both types
                max_school_activity = max(school_values) if school_values else 0
                max_non_school_activity = max(non_school_values) if non_school_values else 0
                total_school_activity = sum(school_values) if school_values else 0
                total_non_school_activity = sum(non_school_values) if non_school_values else 0

                # Generate week boundaries for visual separation
                week_boundaries = []
                for date in complete_dates:
                    if date.weekday() == 0:  # Monday = start of week
                        week_boundaries.append(date.isoformat())

                logger.info(f"Combined heatmap data: {len(combined_series)} hours, {len(complete_dates)} days")
                logger.info(f"School time max: {max_school_activity}, Non-school time max: {max_non_school_activity}")

                return {
                    'combined_series': combined_series,
                    'stats': {
                        'max_school_activity': max_school_activity,
                        'max_non_school_activity': max_non_school_activity,
                        'total_school_activity': total_school_activity,
                        'total_non_school_activity': total_non_school_activity
                    },
                    'date_range': [start_date.isoformat(), end_date.isoformat()],
                    'week_boundaries': week_boundaries,
                    'holiday_info': holiday_info
                }

        except Exception as e:
            logger.error(f"Error fetching hourly activity heatmap: {str(e)}")
            return {
                'combined_series': [],
                'stats': {
                    'max_school_activity': 0,
                    'max_non_school_activity': 0,
                    'total_school_activity': 0,
                    'total_non_school_activity': 0
                },
                'date_range': [],
                'week_boundaries': [],
                'holiday_info': {}
            }

    @classmethod
    def get_time_spent_distribution(cls, time_frame='last_3_months'):
        """
        Get time spent distribution data for normal distribution analysis.

        Uses optimized ClickHouse query to calculate daily hours spent per student,
        then generates distribution statistics and bins for normal distribution chart.

        Returns data suitable for normal distribution visualization with curve overlay.
        """
        logger.info(f"Getting time spent distribution for time frame: {time_frame}")
        try:
            # Get time filter for the selected time frame
            time_filter = cls._get_time_filter(time_frame)

            # Get maximum session duration from Django settings (in seconds)
            max_session_duration = getattr(settings, 'MAX_SESSION_DURATION', 5400)  # Default 1.5 hours
            max_activity_duration = 1800  # 30 minutes cap per individual activity session
            logger.info(f"Using MAX_SESSION_DURATION: {max_session_duration} seconds ({max_session_duration/3600:.1f} hours)")
            logger.info(f"Using MAX_ACTIVITY_DURATION: {max_activity_duration} seconds ({max_activity_duration/60:.0f} minutes)")

            with connections['clickhouse_db'].cursor() as cursor:
                # Improved three-tier query for accurate session boundary detection
                # Tier 1: Calculate time differences between consecutive activities
                # Tier 2: Apply session boundary logic (discard if > MAX_SESSION_DURATION, cap individual activities)
                # Tier 3: Aggregate by student and day
                cursor.execute(f"""
                    SELECT
                        student_id,
                        day,
                        round(sum(read_seconds) / 3600, 2) AS hours_spent
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
                                {time_filter}
                        )
                    )
                    GROUP BY
                        student_id,
                        day
                    HAVING hours_spent > 0
                    ORDER BY
                        day,
                        student_id
                """)

                time_spent_data = cursor.fetchall()

                if not time_spent_data:
                    logger.warning("No time spent data found")
                    return {
                        'distribution_data': [],
                        'statistics': {
                            'mean': 0,
                            'std_dev': 0,
                            'median': 0,
                            'mode': 0,
                            'min': 0,
                            'max': 0,
                            'count': 0,
                            'max_session_hours': round(max_session_duration / 3600, 1),
                            'max_activity_minutes': round(max_activity_duration / 60, 0)
                        },
                        'normal_curve': [],
                        'bins': []
                    }

                # Extract hours spent values for statistical analysis
                hours_values = [row[2] for row in time_spent_data if row[2] > 0]

                if not hours_values:
                    logger.warning("No valid hours spent values found")
                    return {
                        'distribution_data': [],
                        'statistics': {
                            'mean': 0,
                            'std_dev': 0,
                            'median': 0,
                            'mode': 0,
                            'min': 0,
                            'max': 0,
                            'count': 0,
                            'max_session_hours': round(max_session_duration / 3600, 1),
                            'max_activity_minutes': round(max_activity_duration / 60, 0)
                        },
                        'normal_curve': [],
                        'bins': []
                    }

                # Calculate basic statistics
                import statistics
                import math

                mean_hours = statistics.mean(hours_values)
                std_dev_hours = statistics.stdev(hours_values) if len(hours_values) > 1 else 0
                median_hours = statistics.median(hours_values)
                min_hours = min(hours_values)
                max_hours = max(hours_values)
                count = len(hours_values)

                # Calculate mode (most frequent value rounded to nearest 0.1)
                rounded_values = [round(val, 1) for val in hours_values]
                try:
                    mode_hours = statistics.mode(rounded_values)
                except statistics.StatisticsError:
                    # If no unique mode, use the first value of the most common bin
                    from collections import Counter
                    counter = Counter(rounded_values)
                    mode_hours = counter.most_common(1)[0][0] if counter else mean_hours

                # Create bins for histogram (using Sturges' rule for bin count)
                bin_count = min(max(int(math.ceil(math.log2(count) + 1)), 10), 50)  # Between 10-50 bins
                bin_width = (max_hours - min_hours) / bin_count

                bins = []
                bin_edges = []
                for i in range(bin_count):
                    bin_start = min_hours + i * bin_width
                    bin_end = min_hours + (i + 1) * bin_width
                    bin_center = (bin_start + bin_end) / 2

                    # Count values in this bin
                    bin_count_val = sum(1 for val in hours_values if bin_start <= val < bin_end)
                    if i == bin_count - 1:  # Include max value in last bin
                        bin_count_val = sum(1 for val in hours_values if bin_start <= val <= bin_end)

                    bins.append({
                        'bin_center': round(bin_center, 2),
                        'bin_start': round(bin_start, 2),
                        'bin_end': round(bin_end, 2),
                        'frequency': bin_count_val,
                        'density': bin_count_val / (count * bin_width) if count > 0 and bin_width > 0 else 0
                    })
                    bin_edges.append(bin_center)

                # Generate normal curve points for overlay
                normal_curve = []
                if std_dev_hours > 0:
                    x_min = max(0, mean_hours - 4 * std_dev_hours)  # Don't go below 0 hours
                    x_max = mean_hours + 4 * std_dev_hours
                    x_step = (x_max - x_min) / 100

                    for i in range(101):
                        x = x_min + i * x_step
                        # Normal distribution probability density function
                        y = (1 / (std_dev_hours * math.sqrt(2 * math.pi))) * \
                            math.exp(-0.5 * ((x - mean_hours) / std_dev_hours) ** 2)
                        normal_curve.append({
                            'x': round(x, 2),
                            'y': round(y, 6)
                        })

                # Create student daily data for detailed view
                student_daily_data = []
                for row in time_spent_data:
                    student_daily_data.append({
                        'student_id': row[0],
                        'date': row[1].isoformat(),
                        'hours_spent': row[2]
                    })

                logger.info(f"Time spent distribution: {count} data points, mean={mean_hours:.2f}h, std={std_dev_hours:.2f}h, session_cap={max_session_duration/3600:.1f}h, activity_cap={max_activity_duration/60:.0f}min")

                return {
                    'distribution_data': student_daily_data,
                    'statistics': {
                        'mean': round(mean_hours, 2),
                        'std_dev': round(std_dev_hours, 2),
                        'median': round(median_hours, 2),
                        'mode': round(mode_hours, 2),
                        'min': round(min_hours, 2),
                        'max': round(max_hours, 2),
                        'count': count,
                        'max_session_hours': round(max_session_duration / 3600, 1),
                        'max_activity_minutes': round(max_activity_duration / 60, 0)
                    },
                    'normal_curve': normal_curve,
                    'bins': bins
                }

        except Exception as e:
            logger.error(f"Error fetching time spent distribution: {str(e)}")
            return {
                'distribution_data': [],
                'statistics': {
                    'mean': 0,
                    'std_dev': 0,
                    'median': 0,
                    'mode': 0,
                    'min': 0,
                    'max': 0,
                    'count': 0,
                    'max_session_hours': round(getattr(settings, 'MAX_SESSION_DURATION', 5400) / 3600, 1),
                    'max_activity_minutes': 30
                },
                'normal_curve': [],
                'bins': []
            }

    class Meta:
        managed = False
        db_table = 'statements_mv'
        app_label = 'clickhouse_app'

class MostMemoContents(models.Model):
    """Model to track most memo contents from ClickHouse"""
    content_id = models.IntegerField(primary_key=True)
    content_title = models.CharField(max_length=255)
    total_memos = models.IntegerField()

    @classmethod
    def get_most_memo_contents(cls, limit=10, offset=0, search=None):
        query = """
            SELECT
                contents_id,
                contents_name,
                uniqExact(_id) AS total_memos,
                object_id
            FROM statements_mv
            WHERE operation_name = 'ADD_HW_MEMO'
                AND actor_name_role == 'student'
                AND contents_id != ''
        """

        if search:
            query += f" AND contents_name ILIKE '%{search}%'"

        query += """
            GROUP BY
                contents_id,
                contents_name,
                object_id
            ORDER BY
                total_memos DESC
        """

        if limit is not None:
            query += f" LIMIT {offset}, {limit}"

        with connections['clickhouse_db'].cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return [
                    {"id": row[0], "contents_name": row[1], "total_memos": row[2], 'object_id': row[3]}
                    for row in rows
                ]

    @classmethod
    def get_most_memo_contents_count(cls, search=None):
        base_query = """
            SELECT
                contents_id
            FROM statements_mv
            WHERE operation_name = 'ADD_HW_MEMO'
                AND actor_name_role == 'student'
                AND contents_id != ''
        """

        if search:
            base_query += f" AND contents_name ILIKE '%{search}%'"

        query = f"""
            SELECT
                count() as total_count
            FROM (
                {base_query}
                GROUP BY
                    contents_id
            )
        """

        with connections['clickhouse_db'].cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0] if result else 0

    class Meta:
        managed = False
        db_table = 'statements_mv'
        app_label = 'clickhouse_app'

# ADD_MARKER
class MostMarkedContents(models.Model):
    """Model to track most marked contents from ClickHouse"""
    content_id = models.IntegerField(primary_key=True)
    content_title = models.CharField(max_length=255)
    total_marks = models.IntegerField()

    @classmethod
    def get_most_marked_contents(cls, limit=10, offset=0, search=None):
        query = """
        SELECT
            contents_id,
            contents_name,
            uniqExact(_id) AS total_marks,
            object_id
        FROM statements_mv
        WHERE operation_name = 'ADD_MARKER'
            AND actor_name_role == 'student'
            AND contents_id != ''
        """

        if search:
            query += f" AND contents_name ILIKE '%{search}%'"

        query += """
        GROUP BY
            contents_id,
            contents_name,
            object_id
        ORDER BY total_marks DESC
        """

        if limit is not None:
            query += f" LIMIT {offset}, {limit}"

        with connections['clickhouse_db'].cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return [
                    {"id": row[0], "contents_name": row[1], "total_marks": row[2], "object_id": row[3]}
                    for row in rows
                ]

    @classmethod
    def get_most_marked_contents_count(cls, search=None):
        base_query = """
            SELECT
                contents_id
            FROM statements_mv
            WHERE operation_name = 'ADD_MARKER'
                AND actor_name_role == 'student'
                AND contents_id != ''
        """

        if search:
            base_query += f" AND contents_name ILIKE '%{search}%'"

        query = f"""
            SELECT
                count() as total_count
            FROM (
                {base_query}
                GROUP BY
                    contents_id
            )
        """

        with connections['clickhouse_db'].cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0] if result else 0

    class Meta:
        managed = False
        db_table = 'statements_mv'
        app_label = 'clickhouse_app'

class CourseCategory(models.Model):
    """Model to access course categories hierarchy from Moodle"""
    id = models.IntegerField(primary_key=True)
    name = models.CharField(max_length=255)

    class Meta:
        managed = False
        app_label = 'moodle_app'

    @classmethod
    def get_categories_with_courses(cls):
        """
        Retrieve the course categories hierarchy with courses from Moodle.
        Returns a nested structure of parent categories, child categories, and courses.
        """
        # Use cache to minimize database load
        from django.core.cache import cache
        import datetime

        cache_key = 'course_categories_hierarchy'
        cached_data = cache.get(cache_key)

        if cached_data:
            return cached_data

        query = """
        SELECT
            parent_cat.id AS parent_category_id,
            parent_cat.name AS parent_category_name,
            child_cat.id AS child_category_id,
            child_cat.name AS child_category_name,
            course.id AS course_id,
            course.fullname AS course_name,
            course.sortorder AS course_sortorder,
            course.visible AS course_visible,
            course.startdate AS course_startdate,
            course.enddate AS course_enddate,
            course.timecreated AS course_created
        FROM mdl_course_categories parent_cat
        JOIN mdl_course_categories child_cat ON child_cat.parent = parent_cat.id
        LEFT JOIN mdl_course course ON course.category = child_cat.id
        WHERE parent_cat.parent = 0
        ORDER BY parent_cat.sortorder, child_cat.sortorder, course.sortorder
        """

        with connections['moodle_db'].cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        # Organize data in a hierarchical structure
        hierarchy = {}

        for row in rows:
            parent_id = row[0]
            parent_name = row[1]
            child_id = row[2]
            child_name = row[3]
            course_id = row[4]
            course_name = row[5]
            course_sortorder = row[6]
            course_visible = row[7]
            course_startdate = row[8]
            course_enddate = row[9]
            course_created = row[10]

            # Convert Unix timestamps to datetime objects if they exist
            if course_startdate:
                course_startdate = datetime.datetime.fromtimestamp(course_startdate)
            if course_enddate:
                course_enddate = datetime.datetime.fromtimestamp(course_enddate)
            if course_created:
                course_created = datetime.datetime.fromtimestamp(course_created)

            # Add parent category if not exists
            if parent_id not in hierarchy:
                hierarchy[parent_id] = {
                    'id': parent_id,
                    'name': parent_name,
                    'children': {}
                }

            # Add child category if not exists
            if child_id not in hierarchy[parent_id]['children']:
                hierarchy[parent_id]['children'][child_id] = {
                    'id': child_id,
                    'name': child_name,
                    'courses': []
                }

            # Add course if exists
            if course_id is not None:
                hierarchy[parent_id]['children'][child_id]['courses'].append({
                    'id': course_id,
                    'name': course_name,
                    'sortorder': course_sortorder,
                    'visible': course_visible,
                    'startdate': course_startdate,
                    'enddate': course_enddate,
                    'created': course_created
                })

        # Cache the result for 24 hours to reduce database load
        cache.set(cache_key, hierarchy, 86400)  # 24 hours = 86400 seconds

        return hierarchy

class CourseDetail(models.Model):
    """Model to retrieve and handle detailed course information from Moodle and ClickHouse"""
    id = models.IntegerField(primary_key=True)
    fullname = models.CharField(max_length=255)

    class Meta:
        managed = False
        app_label = 'moodle_app'

    @classmethod
    def get_course_details(cls, course_id):
        """Get basic course information"""
        import datetime

        with connections['moodle_db'].cursor() as cursor:
            # Get basic course info
            cursor.execute("""
                SELECT c.id, c.fullname, c.shortname, c.summary, c.startdate,
                       c.enddate, c.timecreated, c.timemodified, cat.name as category_name
                FROM mdl_course c
                JOIN mdl_course_categories cat ON c.category = cat.id
                WHERE c.id = %s
            """, [course_id])
            course_data = cursor.fetchone()

            if not course_data:
                return None

            # Convert Unix timestamps to datetime objects
            startdate = course_data[4]
            enddate = course_data[5]
            created = course_data[6]
            modified = course_data[7]

            if startdate:
                startdate = datetime.datetime.fromtimestamp(startdate)
            if enddate:
                enddate = datetime.datetime.fromtimestamp(enddate)
            if created:
                created = datetime.datetime.fromtimestamp(created)
            if modified:
                modified = datetime.datetime.fromtimestamp(modified)

            return {
                'id': course_data[0],
                'fullname': course_data[1],
                'shortname': course_data[2],
                'summary': course_data[3],
                'startdate': startdate,
                'enddate': enddate,
                'created': created,
                'modified': modified,
                'category_name': course_data[8]
            }

    @classmethod
    def get_course_modules(cls, course_id):
        """Get course modules/activities"""
        import datetime

        with connections['moodle_db'].cursor() as cursor:
            cursor.execute("""
                SELECT cm.id, m.name as module_type, cm.instance, cm.added, cm.completion
                FROM mdl_course_modules cm
                JOIN mdl_modules m ON cm.module = m.id
                WHERE cm.course = %s AND cm.visible = 1
                ORDER BY cm.section, cm.added
            """, [course_id])
            modules = cursor.fetchall()

            result = []
            for module in modules:
                # Convert Unix timestamp to datetime
                added_time = module[3]
                if added_time:
                    added_time = datetime.datetime.fromtimestamp(added_time)

                module_info = {
                    'id': module[0],
                    'type': module[1],
                    'instance': module[2],
                    'added': added_time,
                    'completion': module[4]
                }

                # Get module name based on type
                if module[1] in ('resource', 'url', 'page', 'book', 'forum', 'quiz', 'assign'):
                    table = f"mdl_{module[1]}"
                    cursor.execute(f"""
                        SELECT name FROM {table} WHERE id = %s
                    """, [module[2]])
                    name_result = cursor.fetchone()
                    if name_result:
                        module_info['name'] = name_result[0]
                    else:
                        module_info['name'] = f"{module[1]} activity"

                result.append(module_info)

            return result

    @classmethod
    def get_enrolled_students_count(cls, course_id):
        """Get count of enrolled students in the course"""
        with connections['moodle_db'].cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(DISTINCT ra.userid) as enrolled_students
                FROM mdl_role_assignments ra
                JOIN mdl_role r ON ra.roleid = r.id
                JOIN mdl_context ctx ON ra.contextid = ctx.id
                WHERE r.shortname = 'student'
                AND ctx.contextlevel = 50
                AND ctx.instanceid = %s
            """, [course_id])
            student_count = cursor.fetchone()
            return student_count[0] if student_count else 0

    @classmethod
    def get_course_teachers(cls, course_id):
        """Get teachers assigned to the course"""
        with connections['moodle_db'].cursor() as cursor:
            cursor.execute("""
                SELECT u.id, u.firstname, u.lastname, u.email
                FROM mdl_role_assignments ra
                JOIN mdl_role r ON ra.roleid = r.id
                JOIN mdl_context ctx ON ra.contextid = ctx.id
                JOIN mdl_user u ON ra.userid = u.id
                WHERE r.shortname IN ('teacher', 'editingteacher')
                AND ctx.contextlevel = 50
                AND ctx.instanceid = %s
                ORDER BY u.lastname, u.firstname
            """, [course_id])
            teachers = cursor.fetchall()
            return [
                {
                    'id': teacher[0],
                    'name': f"{teacher[1]} {teacher[2]}",
                    'email': teacher[3]
                }
                for teacher in teachers
            ]

    @classmethod
    def get_course_activity_stats(cls, course_id, start_date=None, end_date=None):
        """Get activity statistics from ClickHouse"""
        try:
            stats = {}

            # If no dates provided, default to last 30 days
            if not start_date:
                start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')

            if not end_date:
                end_date = datetime.datetime.now().strftime('%Y-%m-%d')

            with connections['clickhouse_db'].cursor() as cursor:

                # Get daily engagement data by activity type
                cursor.execute("""
                    SELECT
                        toDate(timestamp) as date,
                        countIf(operation_name = 'OPEN') as content_open,
                        countIf(operation_name = 'ADD_MARKER') as marker,
                        countIf(operation_name = 'ADD_MEMO') as memo,
                        countIf(operation_name = 'ADD_HW_MEMO') as hand_writing_memo,
                        countIf(operation_name = 'ADD_BOOKMARK') as bookmark,
                        countIf(operation_name = 'ANSWER_QUIZ') as quiz_attempts,
                        uniqExact(actor_account_name) as active_students
                    FROM statements_mv
                    WHERE context_id = %s
                    AND timestamp >= toDate(%s)
                    AND timestamp <= toDate(%s)
                    GROUP BY date
                    ORDER BY date
                """, [str(course_id), start_date, end_date])  # Convert course_id to string to match context_id type

                daily_data = cursor.fetchall()
                daily_activity = [
                    {
                        'date': row[0].isoformat(),
                        'content_open': row[1],
                        'marker': row[2],
                        'memo': row[3],
                        'hand_writing_memo': row[4],
                        'bookmark': row[5],
                        'quiz_attempts': row[6],
                        'active_students': row[7]
                    }
                    for row in daily_data
                ]

                # Convert to JSON string for safe rendering in template
                stats['daily_activity_data'] = json.dumps(daily_activity)

            return stats, None

        except Exception as e:
            logger.error(f"Error fetching ClickHouse data for course {course_id}: {str(e)}")
            return {}, str(e)

    @classmethod
    def get_student_highlights_by_time_category(cls, course_id, start_date=None, end_date=None):
        """
        Get student highlights segregated by time categories: School Time vs Non-School Time

        School Time: SCHOOL_START_TIME to SCHOOL_END_TIME on weekdays (excluding Japanese holidays)
        Non-School Time: All other times (weekends, holidays, after hours, before hours)

        Times are converted from UTC (ClickHouse) to JST for comparison
        """
        from django.conf import settings
        from holiday.models import JapaneseHoliday
        import pytz

        try:
            # If no dates provided, default to last 30 days
            if not start_date:
                start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.datetime.now().strftime('%Y-%m-%d')

            # Get school time settings (default to 09:00-16:00 if not set)
            school_start_time = getattr(settings, 'SCHOOL_START_TIME', '09:00')
            school_end_time = getattr(settings, 'SCHOOL_END_TIME', '16:00')

            # Parse school hours
            school_start_hour, school_start_minute = map(int, school_start_time.split(':'))
            school_end_hour, school_end_minute = map(int, school_end_time.split(':'))

            # Get Japanese holidays for the date range
            holiday_dates = set()
            holiday_info = {}  # Store holiday names
            holidays = JapaneseHoliday.objects.filter(
                date__gte=start_date,
                date__lte=end_date
            ).values_list('date', 'name', flat=False)

            for holiday_date, holiday_name in holidays:
                holiday_dates.add(holiday_date)
                holiday_info[holiday_date.isoformat()] = holiday_name

            # Step 1: Get all enrolled students from Moodle
            enrolled_students = {}
            with connections['moodle_db'].cursor() as cursor:
                cursor.execute("""
                    SELECT u.id, u.firstname, u.lastname, u.username, u.email
                    FROM mdl_role_assignments ra
                    JOIN mdl_role r ON ra.roleid = r.id
                    JOIN mdl_context ctx ON ra.contextid = ctx.id
                    JOIN mdl_user u ON ra.userid = u.id
                    WHERE r.shortname = 'student'
                    AND ctx.contextlevel = 50
                    AND ctx.instanceid = %s
                    AND u.deleted = 0
                    AND u.suspended = 0
                    ORDER BY u.lastname, u.firstname
                """, [course_id])

                for row in cursor.fetchall():
                    user_id = str(row[0])
                    enrolled_students[user_id] = {
                        'user_id': user_id,
                        'name': f"{row[1]} {row[2]}",
                        'username': row[3],
                        'email': row[4],
                        'school_time_count': 0,
                        'non_school_time_count': 0,
                        'total_count': 0,
                        'status': 'absent'
                    }

            # Step 2: Get activity data from ClickHouse with time categorization
            with connections['clickhouse_db'].cursor() as cursor:
                # Query to get activities with timestamp details for time categorization
                cursor.execute("""
                    SELECT
                        actor_name_id,
                        _id,
                        timestamp,
                        toDate(timestamp) as activity_date,
                        toHour(addHours(timestamp, 9)) as jst_hour,
                        toMinute(addHours(timestamp, 9)) as jst_minute,
                        toDayOfWeek(addHours(timestamp, 9)) as jst_day_of_week
                    FROM saikyo_new.statements_mv
                    WHERE context_id = %s
                    AND actor_name_id != ''
                    AND timestamp >= toDate(%s)
                    AND timestamp <= toDate(%s)
                    ORDER BY actor_name_id, timestamp
                """, [str(course_id), start_date, end_date])

                activities = cursor.fetchall()

                # Process each activity to categorize by time
                for activity in activities:
                    user_id = activity[0]
                    activity_id = activity[1]
                    timestamp = activity[2]
                    activity_date = activity[3]
                    jst_hour = activity[4]
                    jst_minute = activity[5]
                    jst_day_of_week = activity[6]  # 1=Monday, 7=Sunday

                    # Only process activities for enrolled students
                    if user_id not in enrolled_students:
                        continue

                    # Convert activity date to string for holiday comparison
                    activity_date_str = activity_date.strftime('%Y-%m-%d')

                    # Determine if this is school time or non-school time
                    is_school_time = False

                    # Check if it's a weekday (Monday=1 to Friday=5)
                    if 1 <= jst_day_of_week <= 5:
                        # Check if it's not a holiday
                        if activity_date_str not in holiday_dates:
                            # Check if it's within school hours
                            activity_time_minutes = jst_hour * 60 + jst_minute
                            school_start_minutes = school_start_hour * 60 + school_start_minute
                            school_end_minutes = school_end_hour * 60 + school_end_minute

                            if school_start_minutes <= activity_time_minutes <= school_end_minutes:
                                is_school_time = True

                    # Update student counts
                    if is_school_time:
                        enrolled_students[user_id]['school_time_count'] += 1
                    else:
                        enrolled_students[user_id]['non_school_time_count'] += 1

                    enrolled_students[user_id]['total_count'] += 1
                    enrolled_students[user_id]['status'] = 'active'

            # Convert dictionary to list and calculate percentages
            result = []
            for student_data in enrolled_students.values():
                total = student_data['total_count']
                if total > 0:
                    student_data['school_time_percentage'] = round((student_data['school_time_count'] / total) * 100, 1)
                    student_data['non_school_time_percentage'] = round((student_data['non_school_time_count'] / total) * 100, 1)
                else:
                    student_data['school_time_percentage'] = 0
                    student_data['non_school_time_percentage'] = 0

                result.append(student_data)

            # Sort by total activity count (descending)
            result.sort(key=lambda x: x['total_count'], reverse=True)

            return result

        except Exception as e:
            logger.error(f"Error fetching time-categorized student highlights: {str(e)}")
            return []

    @classmethod
    def get_student_highlights(cls, course_id, start_date=None, end_date=None):
        """Get student highlights from ClickHouse and compare with enrolled students in Moodle"""
        try:
            # If no dates provided, default to last 30 days
            if not start_date:
                start_date = (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')

            if not end_date:
                end_date = datetime.datetime.now().strftime('%Y-%m-%d')

            # Step 1: Get all enrolled students from Moodle
            enrolled_students = {}
            with connections['moodle_db'].cursor() as cursor:
                cursor.execute("""
                    SELECT u.id, u.firstname, u.lastname, u.username, u.email
                    FROM mdl_role_assignments ra
                    JOIN mdl_role r ON ra.roleid = r.id
                    JOIN mdl_context ctx ON ra.contextid = ctx.id
                    JOIN mdl_user u ON ra.userid = u.id
                    WHERE r.shortname = 'student'
                    AND ctx.contextlevel = 50
                    AND ctx.instanceid = %s
                    AND u.deleted = 0
                    AND u.suspended = 0
                    ORDER BY u.lastname, u.firstname
                """, [course_id])

                for row in cursor.fetchall():
                    user_id = str(row[0])  # Convert to string to match ClickHouse data
                    enrolled_students[user_id] = {
                        'user_id': user_id,
                        'name': f"{row[1]} {row[2]}",  # firstname lastname
                        'username': row[3],
                        'email': row[4],
                        'unique_count': 0,  # Default to 0 interactions
                        'status': 'absent'  # Default to absent
                    }

            # Step 2: Get activity data from ClickHouse with date filtering
            with connections['clickhouse_db'].cursor() as cursor:
                cursor.execute("""
                        SELECT
                            COUNT(DISTINCT _id) AS unique_count,
                            actor_name_id
                        FROM
                            saikyo_new.statements_mv sm
                        WHERE
                            context_id = %s
                        AND actor_name_id !=''
                        AND timestamp >= toDate(%s)
                        AND timestamp <= toDate(%s)
                        GROUP BY
                            actor_name_id
                        ORDER BY
                            unique_count DESC
                        """, [str(course_id), start_date, end_date])

                student_highlights = cursor.fetchall()

                # Update enrolled students with activity data
                for highlight in student_highlights:
                    activity_count = highlight[0]
                    user_id = highlight[1]

                    # Only include activity for officially enrolled students
                    if user_id in enrolled_students:
                        enrolled_students[user_id]['unique_count'] = activity_count
                        enrolled_students[user_id]['status'] = 'active'

            # Convert dictionary to list
            result = list(enrolled_students.values())

            # Sort by activity count (descending)
            result.sort(key=lambda x: x['unique_count'], reverse=True)

            return result

        except Exception as e:
            logger.error(f"Error fetching student highlights: {str(e)}")
            return []

class TopKeywords(models.Model):
    """Model to track top keywords extracted from student highlights"""
    keyword = models.CharField(max_length=255, primary_key=True)
    frequency = models.IntegerField()
    score = models.FloatField()

    @classmethod
    def get_top_keywords(cls, context_id=None, limit=None, max_keywords_per_text=5, top_n=100):
        """
        Get top keywords from student highlights

        Args:
            context_id (str, optional): Filter by specific context (course) ID
            limit (int, optional): Limit number of highlight records to process
            max_keywords_per_text (int): Maximum keywords to extract per highlight
            top_n (int): Number of top keywords to return in final ranking

        Returns:
            list: List of dictionaries with keyword data
        """
        from leaf_school.utils.keyword_ranking import get_keyword_ranking

        try:
            # Get keyword ranking DataFrame
            keyword_df = get_keyword_ranking(
                context_id=context_id,
                limit=limit,
                max_keywords_per_text=max_keywords_per_text,
                top_n=top_n
            )

            # Convert DataFrame to list of dictionaries
            if not keyword_df.empty:
                return keyword_df.to_dict('records')
            return []

        except Exception as e:
            logger.error(f"Error getting top keywords: {str(e)}")
            return []

    class Meta:
        managed = False
        app_label = 'clickhouse_app'
