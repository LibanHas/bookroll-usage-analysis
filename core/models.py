import logging
import json
from django.db import models
from django.db import connections
from clickhouse_backend.models import ClickhouseModel
from leaf_school.utils.db_helpers import clickhouse_connection
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
    def get_most_active_contents(cls, limit=10, offset=0, search=None):
        query = """
            SELECT
                contents_id,
                contents_name,
                uniqExact(_id) AS total_activities,
                object_id
            FROM statements_mv
            WHERE contents_id != ''
        """

        if search:
            query += f" AND contents_name ILIKE '%{search}%'"

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

        with clickhouse_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return [
                    {"id": row[0], "contents_name": row[1], "total_activities": row[2], "object_id": row[3]}
                    for row in rows
                ]

    @classmethod
    def get_most_active_contents_count(cls, search=None):
        base_query = """
            SELECT
                contents_id
            FROM statements_mv
            WHERE contents_id != ''
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

        with clickhouse_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                return result[0] if result else 0

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
        GROUP BY date
        ORDER BY date
        """
        with clickhouse_connection() as connection:
            with connection.cursor() as cursor:
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
            GROUP BY date
            ORDER BY date
        """
        with clickhouse_connection() as connection:
            with connection.cursor() as cursor:
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

    @classmethod
    def get_most_active_students(cls, limit=10, offset=0, search=None):
        query = """
        SELECT
            actor_account_name,
            uniqExact(_id) AS total_activities
        FROM statements_mv
        WHERE actor_name_role = 'student'
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

        with clickhouse_connection() as connection:
            with connection.cursor() as cursor:
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
            WHERE actor_name_role = 'student'
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

        with clickhouse_connection() as connection:
            with connection.cursor() as cursor:
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
                AND actor_name_role = 'student'
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

        with clickhouse_connection() as connection:
            with connection.cursor() as cursor:
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
                AND actor_name_role = 'student'
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

        with clickhouse_connection() as connection:
            with connection.cursor() as cursor:
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
            AND actor_name_role = 'student'
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

        with clickhouse_connection() as connection:
            with connection.cursor() as cursor:
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
                AND actor_name_role = 'student'
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

        with clickhouse_connection() as connection:
            with connection.cursor() as cursor:
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

        # Cache the result for 30 minutes to reduce database load
        cache.set(cache_key, hierarchy, 1800)  # 30 minutes = 1800 seconds

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
    def get_course_activity_stats(cls, course_id):
        """Get activity statistics from ClickHouse"""
        try:
            stats = {}

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
                    AND timestamp >= today() - INTERVAL 30 DAY
                    GROUP BY date
                    ORDER BY date
                """, [str(course_id)])  # Convert course_id to string to match context_id type

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
