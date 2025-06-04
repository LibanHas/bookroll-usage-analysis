from django.core.management.base import BaseCommand
from django.conf import settings
import json
from past_years.models import PastYearStudentGrades, PastYearCourseCategory


class Command(BaseCommand):
    help = 'Test student filtering effectiveness for past years analytics'

    def add_arguments(self, parser):
        parser.add_argument(
            '--year',
            type=int,
            default=2024,
            help='Academic year to test (default: 2024)'
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed output'
        )

    def handle(self, *args, **options):
        academic_year = options['year']
        verbose = options['verbose']

        self.stdout.write(
            self.style.SUCCESS(f'Testing student filtering for academic year {academic_year}')
        )

        try:
            # Test the filtering effectiveness
            test_results = PastYearStudentGrades.test_student_filtering_effectiveness(academic_year)

            if 'error' in test_results:
                self.stdout.write(
                    self.style.ERROR(f'Error during testing: {test_results["error"]}')
                )
                return

            # Test the filter optimization
            self.stdout.write(
                self.style.WARNING(f'\n=== FILTER OPTIMIZATION TEST ===')
            )

            try:
                filter_config = PastYearCourseCategory.get_optimal_student_filter_for_academic_year(academic_year)

                self.stdout.write(f'Optimal Filter Type: {filter_config["filter_type"]}')
                self.stdout.write(f'Filter Count: {filter_config["filter_count"]} IDs')
                self.stdout.write(f'Reason: {filter_config["efficiency_reason"]}')

                if verbose:
                    # Show some sample IDs
                    sample_ids = filter_config["filter_ids"][:10]
                    self.stdout.write(f'Sample Filter IDs: {sample_ids}')

            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'Error testing filter optimization: {str(e)}')
                )

            # Display results
            student_info = test_results['student_filter_info']
            grade_comparison = test_results['grade_data_comparison']
            activity_comparison = test_results['activity_data_comparison']

            self.stdout.write(
                self.style.WARNING(f'\n=== STUDENT FILTERING TEST RESULTS ===')
            )

            self.stdout.write(f'Academic Year: {academic_year}')
            self.stdout.write(f'Student IDs for filtering: {student_info["total_student_ids_for_year"]}')

            if verbose:
                self.stdout.write(f'Sample Student IDs: {student_info["sample_student_ids"]}')

            self.stdout.write(f'\n--- GRADE DATA COMPARISON ---')
            self.stdout.write(f'Unfiltered - Students: {grade_comparison["unfiltered"]["total_students"]}, '
                             f'Courses: {grade_comparison["unfiltered"]["total_courses"]}, '
                             f'Grades: {grade_comparison["unfiltered"]["total_grades"]}')
            self.stdout.write(f'Filtered   - Students: {grade_comparison["filtered"]["total_students"]}, '
                             f'Courses: {grade_comparison["filtered"]["total_courses"]}, '
                             f'Grades: {grade_comparison["filtered"]["total_grades"]}')
            self.stdout.write(f'Removed    - Students: {grade_comparison["filtering_effect"]["students_removed"]}, '
                             f'Grades: {grade_comparison["filtering_effect"]["grades_removed"]}')

            self.stdout.write(f'\n--- ACTIVITY DATA COMPARISON ---')
            self.stdout.write(f'Unfiltered - Accounts: {activity_comparison["unfiltered"]["total_accounts"]}, '
                             f'Activities: {activity_comparison["unfiltered"]["total_activities"]}')
            self.stdout.write(f'Filtered   - Accounts: {activity_comparison["filtered"]["total_accounts"]}, '
                             f'Students: {activity_comparison["filtered"]["total_students"]}, '
                             f'Activities: {activity_comparison["filtered"]["total_activities"]}')
            self.stdout.write(f'Removed    - Accounts: {activity_comparison["filtering_effect"]["accounts_removed"]}, '
                             f'Activities: {activity_comparison["filtering_effect"]["activities_removed"]}')

            # Calculate effectiveness percentages
            if grade_comparison["unfiltered"]["total_students"] > 0:
                student_retention_rate = (grade_comparison["filtered"]["total_students"] /
                                        grade_comparison["unfiltered"]["total_students"]) * 100
                self.stdout.write(f'\nStudent retention rate: {student_retention_rate:.1f}%')

            if activity_comparison["unfiltered"]["total_accounts"] > 0:
                account_retention_rate = (activity_comparison["filtered"]["total_accounts"] /
                                        activity_comparison["unfiltered"]["total_accounts"]) * 100
                self.stdout.write(f'Account retention rate: {account_retention_rate:.1f}%')

            if verbose:
                self.stdout.write(f'\n--- FULL TEST RESULTS (JSON) ---')
                self.stdout.write(json.dumps(test_results, indent=2, default=str))

            self.stdout.write(
                self.style.SUCCESS(f'\nStudent filtering test completed successfully!')
            )

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'Error running student filtering test: {str(e)}')
            )
            raise