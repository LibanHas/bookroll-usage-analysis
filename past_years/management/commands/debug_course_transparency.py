from django.core.management.base import BaseCommand
from django.utils import timezone
from past_years.models import PastYearCourseCategory, PastYearGradeAnalytics
from django.db import connections
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Debug course transparency to see why wrong courses appear for each academic year'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('🔍 DEBUGGING COURSE TRANSPARENCY ISSUE'))
        self.stdout.write('=' * 80)

        try:
            # Step 1: Check available academic years
            self.stdout.write('\n📅 STEP 1: Available academic years...')
            available_years = PastYearCourseCategory.get_available_academic_years()
            self.stdout.write(f"Available years: {available_years}")

            # Step 2: Check what courses are in the grade data
            self.stdout.write('\n📊 STEP 2: All courses in grade data...')
            with connections['analysis_db'].cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT course_id, course_name, COUNT(*) as grade_count,
                           MIN(created_at) as earliest_grade, MAX(created_at) as latest_grade
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND quiz >= 0 AND quiz <= 100
                    AND name LIKE %s
                    AND course_id IS NOT NULL
                    GROUP BY course_id, course_name
                    ORDER BY grade_count DESC
                    LIMIT 20
                """, ['%Benesse%'])

                all_courses = cursor.fetchall()
                self.stdout.write(f"Found {len(all_courses)} distinct courses with grades")
                self.stdout.write("Top 10 courses by grade count:")
                for i, course in enumerate(all_courses[:10]):
                    self.stdout.write(f"  {i+1}. Course ID: {course[0]}, Name: {course[1][:50]}..., Grades: {course[2]}, Date range: {course[3]} to {course[4]}")

            # Step 3: For each academic year, check what the current logic is doing
            for year in available_years[:3]:  # Check first 3 years
                self.stdout.write(f'\n🔍 STEP 3.{year}: Analyzing academic year {year}...')

                # Get students for this year
                students = PastYearCourseCategory.get_student_user_ids_for_academic_year(year)
                self.stdout.write(f"  Students in year {year}: {len(students)}")

                if not students:
                    self.stdout.write(f"  ❌ No students found for year {year}")
                    continue

                # Check what courses these students have grades in
                student_placeholders = ",".join(["%s"] * len(students))

                with connections['analysis_db'].cursor() as cursor:
                    # Current logic: What courses do students from this year have grades in?
                    current_logic_query = f"""
                        SELECT
                            course_id,
                            course_name,
                            COUNT(DISTINCT student_id) as students_with_grades,
                            COUNT(*) as total_grades,
                            MIN(created_at) as earliest_grade,
                            MAX(created_at) as latest_grade,
                            AVG(quiz) as avg_grade
                        FROM course_student_scores
                        WHERE quiz IS NOT NULL
                        AND student_id IN ({student_placeholders})
                        AND quiz >= 0 AND quiz <= 100
                        AND name LIKE %s
                        GROUP BY course_id, course_name
                        HAVING students_with_grades >= 3
                        ORDER BY students_with_grades DESC
                        LIMIT 10
                    """

                    cursor.execute(current_logic_query, students + ['%Benesse%'])
                    year_courses_current = cursor.fetchall()

                    self.stdout.write(f"  📚 Current logic finds {len(year_courses_current)} courses for year {year}:")
                    for i, course in enumerate(year_courses_current):
                        grade_date_range = f"{course[4].strftime('%Y-%m') if course[4] else 'N/A'} to {course[5].strftime('%Y-%m') if course[5] else 'N/A'}"
                        self.stdout.write(f"    {i+1}. Course {course[0]}: {course[1][:40]}... | {course[2]} students | Grades: {course[4]} to {course[5]}")

                    # Alternative logic: Filter by grade creation date for academic year
                    start_date = f"{year}-04-01"
                    end_date = f"{year + 1}-03-31"

                    date_filtered_query = f"""
                        SELECT
                            course_id,
                            course_name,
                            COUNT(DISTINCT student_id) as students_with_grades,
                            COUNT(*) as total_grades,
                            MIN(created_at) as earliest_grade,
                            MAX(created_at) as latest_grade
                        FROM course_student_scores
                        WHERE quiz IS NOT NULL
                        AND created_at >= %s
                        AND created_at <= %s
                        AND quiz >= 0 AND quiz <= 100
                        AND name LIKE %s
                        GROUP BY course_id, course_name
                        HAVING students_with_grades >= 3
                        ORDER BY students_with_grades DESC
                        LIMIT 10
                    """

                    cursor.execute(date_filtered_query, [start_date, end_date, '%Benesse%'])
                    year_courses_date = cursor.fetchall()

                    self.stdout.write(f"  📅 Date-filtered logic finds {len(year_courses_date)} courses for year {year} ({start_date} to {end_date}):")
                    for i, course in enumerate(year_courses_date):
                        self.stdout.write(f"    {i+1}. Course {course[0]}: {course[1][:40]}... | {course[2]} students | {course[3]} grades")

                    # Check overlap between the two approaches
                    current_course_ids = set(str(c[0]) for c in year_courses_current)
                    date_course_ids = set(str(c[0]) for c in year_courses_date)

                    overlap = current_course_ids.intersection(date_course_ids)
                    current_only = current_course_ids - date_course_ids
                    date_only = date_course_ids - current_course_ids

                    self.stdout.write(f"  🔄 Comparison for year {year}:")
                    self.stdout.write(f"    Overlap: {len(overlap)} courses | Current only: {len(current_only)} | Date only: {len(date_only)}")
                    if current_only:
                        self.stdout.write(f"    Current logic extra courses: {list(current_only)[:5]}")
                    if date_only:
                        self.stdout.write(f"    Date logic extra courses: {list(date_only)[:5]}")

            # Step 4: Check if course IDs from Moodle match those in grades
            self.stdout.write('\n🔍 STEP 4: Checking Moodle vs Grade course ID alignment...')

            # Get a sample of course IDs from Moodle for year 2024
            if 2024 in available_years:
                moodle_courses = PastYearCourseCategory.get_courses_by_academic_year(2024)
                moodle_course_ids = set()

                if moodle_courses and moodle_courses.get('categories'):
                    for category in moodle_courses.get('categories', {}).values():
                        for child_category in category.get('children', {}).values():
                            moodle_course_ids.update(str(course['id']) for course in child_category.get('courses', []))

                self.stdout.write(f"  Moodle course IDs for 2024: {len(moodle_course_ids)} courses")
                self.stdout.write(f"  Sample Moodle IDs: {list(moodle_course_ids)[:10]}")

                # Check if any of these exist in the grade data
                if moodle_course_ids:
                    moodle_ids_list = list(moodle_course_ids)[:100]  # Check first 100
                    placeholders = ",".join(["%s"] * len(moodle_ids_list))

                    with connections['analysis_db'].cursor() as cursor:
                        overlap_query = f"""
                            SELECT DISTINCT course_id, course_name, COUNT(*) as grade_count
                            FROM course_student_scores
                            WHERE course_id IN ({placeholders})
                            AND quiz IS NOT NULL
                            GROUP BY course_id, course_name
                            ORDER BY grade_count DESC
                        """

                        cursor.execute(overlap_query, moodle_ids_list)
                        overlap_courses = cursor.fetchall()

                        self.stdout.write(f"  Moodle courses found in grade data: {len(overlap_courses)}")
                        if overlap_courses:
                            for course in overlap_courses[:5]:
                                self.stdout.write(f"    Course {course[0]}: {course[1]} ({course[2]} grades)")
                        else:
                            self.stdout.write("    ❌ NO OVERLAP between Moodle course IDs and grade data course IDs!")

            self.stdout.write('\n✅ COURSE TRANSPARENCY DEBUG COMPLETE')
            self.stdout.write('\n💡 RECOMMENDATION:')
            if len(overlap_courses) == 0:
                self.stdout.write('   The issue is that Moodle course IDs do not match grade data course IDs.')
                self.stdout.write('   We should either:')
                self.stdout.write('   1. Use grade creation dates to categorize courses by academic year')
                self.stdout.write('   2. Find a mapping between the two course ID systems')
                self.stdout.write('   3. Show course transparency differently (not by academic year)')
            else:
                self.stdout.write('   Some overlap exists. Consider hybrid approach.')

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Error: {str(e)}'))
            logger.error(f"Debug course transparency error: {str(e)}", exc_info=True)