from django.core.management.base import BaseCommand
from django.conf import settings
import json
from past_years.models import PastYearStudentGrades, PastYearCourseCategory


class Command(BaseCommand):
    help = 'Debug scatter plot correlation data for past years analytics'

    def add_arguments(self, parser):
        parser.add_argument(
            '--year',
            type=int,
            default=2024,
            help='Academic year to debug (default: 2024)'
        )

    def handle(self, *args, **options):
        academic_year = options['year']

        self.stdout.write(
            self.style.SUCCESS(f'Debugging scatter plot data for academic year {academic_year}')
        )

        try:
            # Get the full analytics
            analytics = PastYearStudentGrades.get_student_analytics_for_year(academic_year)

            if 'error' in analytics:
                self.stdout.write(
                    self.style.ERROR(f'Error getting analytics: {analytics["error"]}')
                )
                return

            # Check each component
            grade_analytics = analytics.get('grade_analytics', {})
            access_analytics = analytics.get('access_analytics', {})
            combined_analytics = analytics.get('combined_analytics', {})

            self.stdout.write(f'\n--- GRADE ANALYTICS ---')
            grade_stats = grade_analytics.get('overall_stats', {})
            self.stdout.write(f'Students with grades: {grade_stats.get("total_students", 0)}')
            self.stdout.write(f'Courses with grades: {grade_stats.get("total_courses", 0)}')
            self.stdout.write(f'Total grade records: {grade_stats.get("total_grades", 0)}')

            self.stdout.write(f'\n--- ACCESS ANALYTICS ---')
            access_stats = access_analytics.get('overall_stats', {})
            self.stdout.write(f'Students with activities: {access_stats.get("total_unique_students", 0)}')
            self.stdout.write(f'Courses with activities: {access_stats.get("total_courses_with_activity", 0)}')
            self.stdout.write(f'Total activities: {access_stats.get("total_activities", 0)}')
            self.stdout.write(f'Student access records: {len(access_analytics.get("student_access", []))}')

            self.stdout.write(f'\n--- COMBINED ANALYTICS ---')
            correlations = combined_analytics.get('student_course_correlations', [])
            self.stdout.write(f'Student-course correlations: {len(correlations)}')

            if len(correlations) == 0:
                self.stdout.write(
                    self.style.WARNING('No correlations found! Investigating...')
                )

                # Check what students we have in each dataset
                grade_students = set()
                activity_students = set()

                # Get students from grade data (if available)
                course_stats = grade_analytics.get('course_stats', [])
                self.stdout.write(f'Grade course stats available: {len(course_stats)}')

                # Get students from activity data
                for access in access_analytics.get('student_access', []):
                    activity_students.add(access['student_id'])

                self.stdout.write(f'Unique students in activity data: {len(activity_students)}')

                if activity_students:
                    sample_activity_students = list(activity_students)[:10]
                    self.stdout.write(f'Sample activity student IDs: {sample_activity_students}')

                # Check for data type mismatches
                if activity_students:
                    first_activity_student = list(activity_students)[0]
                    self.stdout.write(f'Activity student ID type: {type(first_activity_student)} - "{first_activity_student}"')

            else:
                self.stdout.write(
                    self.style.SUCCESS(f'Found {len(correlations)} correlations!')
                )

                # Show sample correlation
                sample = correlations[0]
                self.stdout.write(f'Sample correlation:')
                self.stdout.write(f'  Student: {sample.get("student_id")}')
                self.stdout.write(f'  Course: {sample.get("course_id")} - {sample.get("course_name", "N/A")}')
                self.stdout.write(f'  Grade: {sample.get("avg_grade")}')
                self.stdout.write(f'  Activities: {sample.get("total_activities")}')

                # Check template condition
                if len(correlations) > 0:
                    self.stdout.write(
                        self.style.SUCCESS('Template should show scatter plot!')
                    )
                else:
                    self.stdout.write(
                        self.style.WARNING('Template will NOT show scatter plot (empty correlations)')
                    )

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error debugging scatter plot: {str(e)}')
            )
            raise