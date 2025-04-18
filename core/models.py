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
    def get_most_active_contents(cls):
        query = """
            SELECT
                contents_id,
                contents_name,
                uniqExact(_id) AS total_activities,
                object_id
            FROM statements_mv
            WHERE contents_id != ''
            GROUP BY
                contents_id,
                contents_name,
                object_id
            ORDER BY
                total_activities DESC
            LIMIT 10
        """
        with clickhouse_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return [
                    {"id": row[0], "contents_name": row[1], "total_activities": row[2], "object_id": row[3]}
                    for row in rows
                ]

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
    def get_most_active_students(cls):
        query = """
        SELECT
            actor_account_name,
            uniqExact(_id) AS total_activities
        FROM statements_mv
        WHERE actor_name_role = 'student'
            AND actor_account_name != ''
        GROUP BY actor_account_name
        ORDER BY total_activities DESC
        LIMIT 10
        """
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
    def get_most_active_students_with_details(cls):
        clickhouse_rows = cls.get_most_active_students()
        actor_account_names = [row['actor_account_name'] for row in clickhouse_rows]
        print(f"Actor account names: {actor_account_names}")
        moodle_users = MoodleUser.objects.using('moodle_db').filter(
            id__in=actor_account_names
        )
        moodle_user_dict = {u.id: u for u in moodle_users}
        print(f"Moodle users: {moodle_user_dict}")

        results = []
        for row in clickhouse_rows:
            username = row['actor_account_name']
            print(f"Username: {username}")
            total_activities = row['total_activities']
            moodle_user = moodle_user_dict.get(int(username))
            print(f"Moodle user: {moodle_user}")
            if moodle_user:
                results.append({

                    "moodle_id": moodle_user.id,
                    "username": moodle_user.username,
                    "name": moodle_user.firstname + ' ' + moodle_user.lastname,
                    "total_activities": total_activities,
                })
            else:
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
    def get_most_memo_contents(cls):
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
            GROUP BY
                contents_id,
                contents_name,
                object_id
            ORDER BY
                total_memos DESC
            LIMIT 10
        """
        with clickhouse_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return [
                    {"id": row[0], "contents_name": row[1], "total_memos": row[2], 'object_id': row[3]}
                    for row in rows
                ]

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
    def get_most_marked_contents(cls):
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
        GROUP BY
            contents_id,
            contents_name,
            object_id
        ORDER BY total_marks DESC
        LIMIT 10
        """
        with clickhouse_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                return [
                    {"id": row[0], "contents_name": row[1], "total_marks": row[2], "object_id": row[3]}
                    for row in rows
                ]

    class Meta:
        managed = False
        db_table = 'statements_mv'
        app_label = 'clickhouse_app'