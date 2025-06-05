#!/usr/bin/env python3
"""
Debug script to investigate the engagement vs grade issue where average grades show as 0.0.
This script will check database connections, sample data, and the data matching process.
"""

import os
import sys
import django
from django.conf import settings

# Add the project directory to Python path
sys.path.append('/home/alma/workbench/leaf_school/leaf_school')

# Configure Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leaf_school.settings')
django.setup()

from django.db import connections
from past_years.models import extract_student_id_from_actor_account_name
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_database_connections():
    """Check if database connections are working."""
    print("=" * 50)
    print("CHECKING DATABASE CONNECTIONS")
    print("=" * 50)

    # Check ClickHouse connections
    for db_alias in ['clickhouse_db', 'clickhouse_db_pre_2025']:
        try:
            with connections[db_alias].cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                print(f"✅ {db_alias}: Connected (result: {result})")
        except Exception as e:
            print(f"❌ {db_alias}: Failed - {str(e)}")

    # Check analysis_db connection
    try:
        with connections['analysis_db'].cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            print(f"✅ analysis_db: Connected (result: {result})")
    except Exception as e:
        print(f"❌ analysis_db: Failed - {str(e)}")

def check_engagement_data_sample():
    """Check sample engagement data from ClickHouse."""
    print("\n" + "=" * 50)
    print("CHECKING ENGAGEMENT DATA SAMPLE (2022)")
    print("=" * 50)

    academic_year = 2022
    start_date = f"{academic_year}-04-01"
    end_date = f"{academic_year + 1}-03-31"

    try:
        # Try 2025+ database first
        db_alias = 'clickhouse_db'
        with connections[db_alias].cursor() as cursor:
            query = f"""
            SELECT
                extractAll(actor_account_name, '[0-9]+')[1] as student_id,
                actor_account_name,
                count(*) as total_activities,
                round(sum(activity_duration) / 3600, 2) as total_hours
            FROM (
                SELECT
                    actor_account_name,
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
                )
                WHERE activity_duration > 0
            )
            WHERE extractAll(actor_account_name, '[0-9]+')[1] != ''
            GROUP BY student_id, actor_account_name
            HAVING total_activities >= 5
            ORDER BY total_activities DESC
            LIMIT 10
            """

            print(f"Running query on {db_alias}...")
            cursor.execute(query)
            results = cursor.fetchall()

            print(f"Found {len(results)} engagement records:")
            for row in results:
                student_id, actor_account, activities, hours = row
                engagement_score = activities * hours
                print(f"  Student ID: {student_id}, Actor: {actor_account}, Activities: {activities}, Hours: {hours}, Score: {engagement_score:.1f}")

            return results

    except Exception as e:
        print(f"❌ Error getting engagement data: {str(e)}")
        return []

def check_grade_data_sample():
    """Check sample grade data from analysis_db."""
    print("\n" + "=" * 50)
    print("CHECKING GRADE DATA SAMPLE (2022)")
    print("=" * 50)

    academic_year = 2022

    try:
        with connections['analysis_db'].cursor() as cursor:
            # First, check what courses exist for this academic year
            course_query = f"""
            SELECT DISTINCT course_name, count(DISTINCT student_id) as student_count
            FROM course_student_scores
            WHERE course_name LIKE '%{academic_year}年度%'
            GROUP BY course_name
            ORDER BY student_count DESC
            LIMIT 10
            """

            print("Courses for 2022年度:")
            cursor.execute(course_query)
            course_results = cursor.fetchall()

            if not course_results:
                print("❌ No courses found with 2022年度 pattern")

                # Try broader search
                print("\nTrying broader search for 2022:")
                broader_query = """
                SELECT DISTINCT course_name, count(DISTINCT student_id) as student_count
                FROM course_student_scores
                WHERE course_name LIKE '%2022%'
                GROUP BY course_name
                ORDER BY student_count DESC
                LIMIT 10
                """
                cursor.execute(broader_query)
                broader_results = cursor.fetchall()

                print(f"Found {len(broader_results)} courses with '2022' pattern:")
                for course_name, student_count in broader_results:
                    print(f"  {course_name}: {student_count} students")

                return []

            for course_name, student_count in course_results:
                print(f"  {course_name}: {student_count} students")

            # Now check sample grade data
            grade_query = f"""
            SELECT
                student_id,
                count(*) as total_grades,
                round(avg(quiz), 2) as average_grade,
                string_agg(DISTINCT course_name, ', ') as courses
            FROM course_student_scores
            WHERE course_name LIKE '%{academic_year}年度%'
                AND (name LIKE '%Benesse%' OR name LIKE '%ベネッセ%')
                AND quiz IS NOT NULL
                AND quiz >= 0 AND quiz <= 100
            GROUP BY student_id
            HAVING count(*) >= 3
            ORDER BY average_grade DESC
            LIMIT 10
            """

            print(f"\nSample grade data:")
            cursor.execute(grade_query)
            grade_results = cursor.fetchall()

            print(f"Found {len(grade_results)} students with grades:")
            for row in grade_results:
                student_id, total_grades, avg_grade, courses = row
                print(f"  Student ID: {student_id}, Grades: {total_grades}, Avg: {avg_grade}, Courses: {courses[:100]}...")

            return grade_results

    except Exception as e:
        print(f"❌ Error getting grade data: {str(e)}")
        return []

def check_student_id_matching():
    """Check if student IDs from ClickHouse match those in analysis_db."""
    print("\n" + "=" * 50)
    print("CHECKING STUDENT ID MATCHING")
    print("=" * 50)

    # Get some engagement data
    engagement_results = check_engagement_data_sample()
    grade_results = check_grade_data_sample()

    if not engagement_results or not grade_results:
        print("❌ Cannot check matching - missing data from one or both sources")
        return

    # Extract student IDs
    engagement_student_ids = {row[0] for row in engagement_results}
    grade_student_ids = {row[0] for row in grade_results}

    print(f"\nEngagement student IDs (sample): {sorted(list(engagement_student_ids)[:10])}")
    print(f"Grade student IDs (sample): {sorted(list(grade_student_ids)[:10])}")

    # Check overlap
    common_students = engagement_student_ids & grade_student_ids
    print(f"\nCommon student IDs: {len(common_students)}")

    if common_students:
        print(f"Common students: {sorted(list(common_students)[:10])}")
    else:
        print("❌ NO COMMON STUDENT IDs FOUND!")
        print("This explains why average grades are 0.0")

        # Show some examples
        print(f"\nExample engagement student IDs: {sorted(list(engagement_student_ids)[:5])}")
        print(f"Example grade student IDs: {sorted(list(grade_student_ids)[:5])}")

def main():
    """Main debug function."""
    print("DEBUG: Engagement vs Grade Issue Investigation")
    print("=" * 60)

    check_database_connections()

    # Check data samples
    engagement_data = check_engagement_data_sample()
    grade_data = check_grade_data_sample()

    # Check student ID matching
    check_student_id_matching()

    print("\n" + "=" * 60)
    print("DEBUG COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()