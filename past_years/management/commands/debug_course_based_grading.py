from django.core.management.base import BaseCommand
from django.utils import timezone
from past_years.models import PastYearCourseCategory, PastYearGradeAnalytics
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Debug course-based grade categorization to see why no year-by-year data is showing'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('🔍 DEBUGGING COURSE-BASED GRADE CATEGORIZATION'))
        self.stdout.write('=' * 80)

        try:
            # Step 1: Check available academic years
            self.stdout.write('\n📅 STEP 1: Checking available academic years...')
            available_years = PastYearCourseCategory.get_available_academic_years()
            self.stdout.write(f"Available academic years: {available_years}")

            if not available_years:
                self.stdout.write(self.style.ERROR("❌ No academic years found!"))
                return

            # Step 2: Check courses for each academic year
            self.stdout.write('\n🏫 STEP 2: Checking courses for each academic year...')
            for year in available_years[:3]:  # Check first 3 years
                self.stdout.write(f"\n--- Academic Year {year} ---")
                courses_data = PastYearCourseCategory.get_courses_by_academic_year(year)

                if courses_data and courses_data.get('categories'):
                    course_ids = []
                    for category in courses_data.get('categories', {}).values():
                        for child_category in category.get('children', {}).values():
                            course_ids.extend([str(course['id']) for course in child_category.get('courses', [])])

                    self.stdout.write(f"  Courses found: {len(course_ids)}")
                    if course_ids:
                        self.stdout.write(f"  Sample course IDs: {course_ids[:5]}")
                else:
                    self.stdout.write(f"  ❌ No courses found for {year}")

            # Step 3: Check grade data directly
            self.stdout.write('\n📊 STEP 3: Checking grade data in database...')
            from django.db import connections

            with connections['analysis_db'].cursor() as cursor:
                # Check total grades
                cursor.execute("""
                    SELECT COUNT(*) as total_grades,
                           COUNT(DISTINCT student_id) as total_students,
                           COUNT(DISTINCT course_id) as total_courses
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND name LIKE '%Benesse%'
                    AND quiz >= 0 AND quiz <= 100
                """)
                total_stats = cursor.fetchone()
                self.stdout.write(f"Total grades: {total_stats[0]}, Students: {total_stats[1]}, Courses: {total_stats[2]}")

                # Check sample course IDs in grade data
                cursor.execute("""
                    SELECT DISTINCT course_id
                    FROM course_student_scores
                    WHERE quiz IS NOT NULL
                    AND name LIKE '%Benesse%'
                    AND quiz >= 0 AND quiz <= 100
                    ORDER BY course_id
                    LIMIT 10
                """)
                grade_course_ids = [str(row[0]) for row in cursor.fetchall()]
                self.stdout.write(f"Sample course IDs from grade data: {grade_course_ids}")

            # Step 4: Test course-based categorization for one year
            if available_years:
                test_year = available_years[0]
                self.stdout.write(f'\n🧪 STEP 4: Testing course-based categorization for {test_year}...')

                # Get courses for this year
                courses_data = PastYearCourseCategory.get_courses_by_academic_year(test_year)
                if courses_data and courses_data.get('categories'):
                    year_course_ids = []
                    for category in courses_data.get('categories', {}).values():
                        for child_category in category.get('children', {}).values():
                            year_course_ids.extend([str(course['id']) for course in child_category.get('courses', [])])

                    self.stdout.write(f"Course IDs for {test_year}: {len(year_course_ids)} courses")

                    # Check if any of these courses have grade data
                    if year_course_ids:
                        with connections['analysis_db'].cursor() as cursor:
                            course_placeholders = ','.join(['%s'] * len(year_course_ids[:10]))  # Test first 10
                            test_course_ids = year_course_ids[:10]

                            query = f"""
                                SELECT course_id, COUNT(*) as grade_count
                                FROM course_student_scores
                                WHERE quiz IS NOT NULL
                                AND name LIKE %s
                                AND quiz >= 0 AND quiz <= 100
                                AND course_id IN ({course_placeholders})
                                GROUP BY course_id
                                ORDER BY grade_count DESC
                            """
                            cursor.execute(query, ['%Benesse%'] + test_course_ids)
                            matching_courses = cursor.fetchall()

                            self.stdout.write(f"Courses with grade data (from first 10): {len(matching_courses)}")
                            for course_id, count in matching_courses:
                                self.stdout.write(f"  Course {course_id}: {count} grades")

                            if not matching_courses:
                                self.stdout.write(self.style.ERROR("❌ NO GRADE DATA found for courses from this academic year!"))
                                self.stdout.write("This suggests a mismatch between course IDs in Moodle and grade data.")
                else:
                    self.stdout.write(self.style.ERROR(f"❌ No course data found for {test_year}"))

            # Step 5: Try the actual grade performance method
            self.stdout.write('\n⚡ STEP 5: Testing actual grade performance method...')
            try:
                performance_data = PastYearGradeAnalytics.get_grade_performance_by_period()
                top_25_data = performance_data.get('top_25_data', [])
                bottom_25_data = performance_data.get('bottom_25_data', [])

                self.stdout.write(f"Top 25% data points: {len(top_25_data)}")
                self.stdout.write(f"Bottom 25% data points: {len(bottom_25_data)}")

                if top_25_data:
                    self.stdout.write(f"Sample top 25% data: {top_25_data[0]}")
                else:
                    self.stdout.write(self.style.ERROR("❌ No top 25% data returned"))

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Error in grade performance method: {str(e)}"))

            self.stdout.write('\n' + '=' * 80)
            self.stdout.write(self.style.SUCCESS('🔍 DEBUG COMPLETE'))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Debug failed: {str(e)}'))
            import traceback
            self.stdout.write(traceback.format_exc())