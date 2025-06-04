#!/usr/bin/env python
import os
import django
import sys

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leaf_school.settings')
django.setup()

from past_years.models import PastYearCourseCategory, PastYearGradeAnalytics, PastYearStudentGrades
from django.db import connections
import json

print('=== DEBUGGING CORRELATION DATA ===')

# Check available years
try:
    available_years = PastYearCourseCategory.get_available_academic_years()
    print(f'Available years: {available_years[:5]}')
except Exception as e:
    print(f'Error getting available years: {e}')
    sys.exit(1)

print('\n=== CHECKING RAW GRADE DATA ===')
try:
    with connections['analysis_db'].cursor() as cursor:
        # Check total grades
        cursor.execute("SELECT COUNT(*) FROM course_student_scores WHERE quiz IS NOT NULL")
        total_grades = cursor.fetchone()[0]
        print(f'Total grades in analysis_db: {total_grades}')

        # Check Benesse grades (fix SQL formatting by escaping % signs)
        cursor.execute("SELECT COUNT(*) FROM course_student_scores WHERE quiz IS NOT NULL AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')")
        benesse_grades = cursor.fetchone()[0]
        print(f'Benesse grades: {benesse_grades}')

        # Get sample data (fix SQL formatting)
        cursor.execute("SELECT student_id, course_id, quiz, name FROM course_student_scores WHERE quiz IS NOT NULL AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%') LIMIT 5")
        samples = cursor.fetchall()
        print(f'Sample grades:')
        for i, (student_id, course_id, quiz, name) in enumerate(samples):
            print(f'  {i+1}: student={student_id}, course={course_id}, grade={quiz}, name={name}')

        if samples:
            # Check how many unique students have grades
            cursor.execute("SELECT COUNT(DISTINCT student_id) FROM course_student_scores WHERE quiz IS NOT NULL AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')")
            unique_students = cursor.fetchone()[0]
            print(f'Unique students with grades: {unique_students}')

            # Check how many unique courses have grades
            cursor.execute("SELECT COUNT(DISTINCT course_id) FROM course_student_scores WHERE quiz IS NOT NULL AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%')")
            unique_courses = cursor.fetchone()[0]
            print(f'Unique courses with grades: {unique_courses}')

            # Get sample student IDs to check format
            cursor.execute("SELECT DISTINCT student_id FROM course_student_scores WHERE quiz IS NOT NULL AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%') LIMIT 10")
            grade_student_ids = [row[0] for row in cursor.fetchall()]
            print(f'Sample grade student IDs: {grade_student_ids}')

except Exception as e:
    print(f'Error checking raw grade data: {e}')
    import traceback
    traceback.print_exc()

print('\n=== CHECKING CLICKHOUSE TIME DATA STUDENT IDS ===')
try:
    with connections['clickhouse_db_pre_2025'].cursor() as cursor:
        # Get sample actor_account_names from ClickHouse
        cursor.execute("""
            SELECT DISTINCT actor_account_name
            FROM statements_mv
            WHERE actor_account_name != ''
            LIMIT 10
        """)
        clickhouse_student_ids = [row[0] for row in cursor.fetchall()]
        print(f'Sample ClickHouse student IDs: {clickhouse_student_ids}')

except Exception as e:
    print(f'Error checking ClickHouse student IDs: {e}')
    import traceback
    traceback.print_exc()

# Test correlation data with updated method for one year
test_year = 2024
print(f'\n--- Testing UPDATED correlation method for year {test_year} ---')
try:
    # Clear cache first to get fresh data
    from django.core.cache import cache
    cache_key = f'past_years_time_spent_grade_correlation_{test_year}'
    cache.delete(cache_key)

    correlation_data = PastYearGradeAnalytics.get_time_spent_vs_grade_correlation(test_year)
    print(f'  Error: {correlation_data.get("error")}')
    print(f'  Correlation data length: {len(correlation_data.get("correlation_data", []))}')
    print(f'  Students with grades: {correlation_data.get("metadata", {}).get("students_with_grades_only")}')
    print(f'  Students with time data: {correlation_data.get("metadata", {}).get("students_with_time_data")}')
    print(f'  Method: {correlation_data.get("metadata", {}).get("method")}')
    print(f'  Statistics: {correlation_data.get("statistics", {})}')

    # Show sample data if available
    sample_data = correlation_data.get("correlation_data", [])[:5]
    if sample_data:
        print(f'  Sample correlation data:')
        for i, data in enumerate(sample_data):
            print(f'    {i+1}: student={data["student_id"]}, grade={data["average_grade"]:.2f}, minutes={data["total_time_spent_minutes"]:.2f}')

except Exception as e:
    print(f'  Exception: {e}')
    import traceback
    traceback.print_exc()

print('\n=== TESTING MANUAL FALLBACK APPROACH (FIXED) ===')
# Let's test the fallback approach manually for a specific year
year = 2022  # Try an older year that might have more data
try:
    print(f'Testing manual fallback for year {year}...')

    # Get student filter
    filter_config = PastYearCourseCategory.get_optimal_student_filter_for_academic_year(year)
    filter_type = filter_config['filter_type']
    filter_ids = filter_config['filter_ids']

    print(f'Filter type: {filter_type}, Filter IDs: {len(filter_ids)}')

    if filter_ids:
        with connections['analysis_db'].cursor() as cursor:
            # Build student filter clause
            filter_placeholders = ",".join(["%s"] * len(filter_ids))
            if filter_type == 'NOT_IN':
                student_filter = f" AND student_id NOT IN ({filter_placeholders}) AND student_id IS NOT NULL"
            else:
                student_filter = f" AND student_id IN ({filter_placeholders})"

            # Find students with grades (any courses) - FIXED SQL with escaped %
            query = f"""
                SELECT
                    student_id,
                    AVG(quiz) as average_grade,
                    COUNT(*) as grade_count
                FROM course_student_scores
                WHERE quiz IS NOT NULL
                AND (name LIKE '%%Benesse%%' OR name LIKE '%%ベネッセ%%') {student_filter}
                AND quiz >= 0 AND quiz <= 100
                GROUP BY student_id
                HAVING COUNT(*) > 0
                LIMIT 10
            """

            cursor.execute(query, filter_ids)
            results = cursor.fetchall()

            print(f'Manual fallback found {len(results)} students with grades for year {year}:')
            for i, (student_id, avg_grade, grade_count) in enumerate(results):
                print(f'  {i+1}: student={student_id}, avg_grade={avg_grade:.2f}, count={grade_count}')

except Exception as e:
    print(f'Error in manual fallback test: {e}')
    import traceback
    traceback.print_exc()

print('\n=== STUDENT ID FORMAT ANALYSIS ===')
print('Issue identified: Student IDs in grade database vs ClickHouse have different formats')
print('Grade DB: 1261, 1262, 1263... (simple integers)')
print('ClickHouse: 2143@domain, Learner:2143, 2143... (complex patterns)')
print('')
print('This explains why we find students with grades but no time data.')
print('The correlation method needs to handle this ID format mismatch.')

print('\n=== SOLUTION ===')
print('We need to either:')
print('1. Create a mapping between grade student IDs and ClickHouse student IDs')
print('2. Use a different approach to find time data')
print('3. Check if there is a user mapping table that connects these IDs')

print('\n=== DEBUGGING STUDENT ID MAPPING ===')

# Check if there are user mapping tables
print('\n=== CHECKING MOODLE DATABASE FOR USER MAPPING ===')
try:
    with connections['moodle_db'].cursor() as cursor:
        # Check for user tables
        cursor.execute("SHOW TABLES LIKE '%user%'")
        user_tables = cursor.fetchall()
        print(f'User-related tables in moodle_db: {[table[0] for table in user_tables]}')

        # Check mdl_user table structure if it exists
        try:
            cursor.execute("DESCRIBE mdl_user")
            user_columns = cursor.fetchall()
            print(f'mdl_user columns: {[col[0] for col in user_columns]}')

            # Get sample user data
            cursor.execute("SELECT id, username, idnumber FROM mdl_user WHERE idnumber IS NOT NULL AND idnumber != '' LIMIT 10")
            sample_users = cursor.fetchall()
            print(f'Sample users with idnumber:')
            for user_id, username, idnumber in sample_users:
                print(f'  id={user_id}, username={username}, idnumber={idnumber}')

            # Check if any idnumber matches our grade student IDs
            cursor.execute("SELECT id, username, idnumber FROM mdl_user WHERE idnumber IN ('1261', '1262', '1263', '1264', '1265') LIMIT 5")
            matching_users = cursor.fetchall()
            print(f'Users matching grade student IDs:')
            for user_id, username, idnumber in matching_users:
                print(f'  id={user_id}, username={username}, idnumber={idnumber}')

        except Exception as e:
            print(f'Error checking mdl_user: {e}')

except Exception as e:
    print(f'Error checking moodle_db: {e}')

print('\n=== CHECKING ANALYSIS DATABASE FOR USER MAPPING ===')
try:
    with connections['analysis_db'].cursor() as cursor:
        # Check for user-related tables
        cursor.execute("SHOW TABLES LIKE '%user%'")
        user_tables = cursor.fetchall()
        print(f'User-related tables in analysis_db: {[table[0] for table in user_tables]}')

        # Check for student mapping tables
        cursor.execute("SHOW TABLES")
        all_tables = cursor.fetchall()
        print(f'All tables in analysis_db: {[table[0] for table in all_tables]}')

except Exception as e:
    print(f'Error checking analysis_db: {e}')

print('\n=== TESTING REVERSE MAPPING APPROACH ===')
# Try to see if we can extract user IDs from ClickHouse that might map to grade IDs
try:
    with connections['clickhouse_db_pre_2025'].cursor() as cursor:
        # Get all unique student IDs with patterns that might be simple numbers
        cursor.execute("""
            SELECT DISTINCT actor_account_name
            FROM statements_mv
            WHERE actor_name_role = 'student'
            AND actor_account_name != ''
            AND (
                match(actor_account_name, '^[0-9]+$') OR
                match(actor_account_name, '^[0-9]+@.*') OR
                match(actor_account_name, '^Learner:[0-9]+$')
            )
            ORDER BY actor_account_name
            LIMIT 20
        """)
        clickhouse_patterns = cursor.fetchall()
        print(f'ClickHouse student ID patterns (first 20):')
        for pattern in clickhouse_patterns:
            print(f'  {pattern[0]}')

        # Check if any simple numeric IDs exist that match our grade range
        cursor.execute("""
            SELECT DISTINCT actor_account_name
            FROM statements_mv
            WHERE actor_name_role = 'student'
            AND match(actor_account_name, '^[0-9]+$')
            AND toInt32(actor_account_name) BETWEEN 1260 AND 1270
            ORDER BY actor_account_name
        """)
        direct_matches = cursor.fetchall()
        print(f'Direct numeric matches in ClickHouse for grade range 1260-1270:')
        for match in direct_matches:
            print(f'  {match[0]}')

except Exception as e:
    print(f'Error checking ClickHouse patterns: {e}')
    import traceback
    traceback.print_exc()

print('\n=== ALTERNATIVE APPROACH: SIMPLE NUMERIC IDS ===')
print('If ClickHouse has some simple numeric IDs, we can modify the correlation method')
print('to also search for simple numeric patterns in addition to the complex patterns.')

print('\n=== RECOMMENDATION ===')
print('1. Check if mdl_user.idnumber maps to grade student_id')
print('2. Check if mdl_user.username maps to ClickHouse patterns')
print('3. Add simple numeric ID patterns to ClickHouse search')
print('4. Create a mapping table if none exists')

print('\n=== CHECKING ANALYSIS_DB USERS TABLE ===')
try:
    with connections['analysis_db'].cursor() as cursor:
        # Check users table structure
        cursor.execute("DESCRIBE users")
        user_columns = cursor.fetchall()
        print(f'users table columns: {[col[0] for col in user_columns]}')

        # Get sample users data
        cursor.execute("SELECT * FROM users LIMIT 5")
        sample_users = cursor.fetchall()
        print(f'Sample users:')
        for i, user in enumerate(sample_users):
            print(f'  {i+1}: {user}')

        # Check if any users have IDs that match our grade student IDs
        cursor.execute("SELECT * FROM users WHERE id IN ('1261', '1262', '1263', '1264', '1265') OR name IN ('1261', '1262', '1263', '1264', '1265')")
        matching_users = cursor.fetchall()
        print(f'Users matching grade student IDs:')
        for user in matching_users:
            print(f'  {user}')

except Exception as e:
    print(f'Error checking users table: {e}')

print('\n=== CONCLUSION AND NEXT STEPS ===')
print('Since no direct mapping exists between grade student IDs and ClickHouse student IDs:')
print('')
print('OPTION 1: Create demo with synthetic data')
print('- Generate sample correlation data for demonstration')
print('- Show how the chart would work with real data')
print('')
print('OPTION 2: Use different data source')
print('- Find alternative time tracking data')
print('- Use Moodle activity logs instead of ClickHouse')
print('')
print('OPTION 3: Create manual mapping')
print('- Survey actual student ID mappings')
print('- Create a mapping table between systems')
print('')
print('RECOMMENDED: Start with OPTION 1 (demo) to show the feature working,')
print('then implement proper mapping once ID relationships are clarified.')