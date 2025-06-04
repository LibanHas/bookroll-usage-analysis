from django.core.management.base import BaseCommand, CommandError
from django.core.cache import cache
from past_years.models import PastYearCourseCategory
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Clear cache for past years analytics data'

    def add_arguments(self, parser):
        parser.add_argument(
            '--year',
            type=int,
            help='Specific academic year to clear cache for (e.g., 2024)',
        )
        parser.add_argument(
            '--all-years',
            action='store_true',
            help='Clear cache for all available academic years',
        )
        parser.add_argument(
            '--list-keys',
            action='store_true',
            help='List cache keys that would be cleared (dry run)',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed output',
        )

    def handle(self, *args, **options):
        verbose = options['verbose']
        list_keys = options['list_keys']

        if options['all_years']:
            self.handle_all_years(verbose, list_keys)
        elif options['year']:
            self.handle_single_year(options['year'], verbose, list_keys)
        else:
            # Show available years and current cache status
            self.show_cache_status()

    def handle_single_year(self, year, verbose, list_keys):
        """Clear cache for a specific year"""
        if list_keys:
            self.stdout.write(f"Cache keys that would be cleared for year {year}:")
            self.list_cache_keys_for_year(year)
        else:
            if verbose:
                self.stdout.write(f"Clearing cache for academic year {year}...")

            success = PastYearCourseCategory.clear_cache_for_year(year)

            if success:
                self.stdout.write(
                    self.style.SUCCESS(f'Successfully cleared cache for academic year {year}')
                )
            else:
                raise CommandError(f'Failed to clear cache for academic year {year}')

    def handle_all_years(self, verbose, list_keys):
        """Clear cache for all available years"""
        available_years = PastYearCourseCategory.get_available_academic_years()

        if not available_years:
            self.stdout.write(self.style.WARNING('No academic years found'))
            return

        if list_keys:
            self.stdout.write("Cache keys that would be cleared for all years:")
            for year in available_years:
                self.stdout.write(f"\n--- Year {year} ---")
                self.list_cache_keys_for_year(year)
        else:
            if verbose:
                self.stdout.write(f"Clearing cache for {len(available_years)} academic years...")

            cleared_count = 0
            for year in available_years:
                try:
                    success = PastYearCourseCategory.clear_cache_for_year(year)
                    if success:
                        cleared_count += 1
                        if verbose:
                            self.stdout.write(f"  ✓ Cleared cache for year {year}")
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f"  ✗ Failed to clear cache for year {year}: {e}")
                    )

            self.stdout.write(
                self.style.SUCCESS(
                    f'Successfully cleared cache for {cleared_count}/{len(available_years)} academic years'
                )
            )

    def list_cache_keys_for_year(self, year):
        """List all cache keys for a specific year"""
        # Generate all possible cache keys for the year
        cache_key_base = f'student_analytics_{year}'

        keys = [
            # Main analytics
            f'{cache_key_base}_main_all_activities',
            f'{cache_key_base}_main_graded_only',

            # Chart data
            f'{cache_key_base}_charts_all_activities',
            f'{cache_key_base}_charts_graded_only',

            # Engagement data
            f'{cache_key_base}_engagement_all_activities',
            f'{cache_key_base}_engagement_graded_only',

            # Courses context
            f'{cache_key_base}_courses_context',

            # Legacy keys
            f'past_year_courses_{year}',
            f'student_user_ids_{year}',
            f'non_student_user_ids_{year}',

            # Registry
            f'cache_registry_{year}',
        ]

        # Check for course-specific distribution keys via registry
        registry_key = f'cache_registry_{year}'
        cached_course_keys = cache.get(registry_key, [])
        keys.extend(cached_course_keys)

        for key in sorted(keys):
            exists = cache.get(key) is not None
            status = "✓ exists" if exists else "✗ not found"
            self.stdout.write(f"  {key} - {status}")

    def show_cache_status(self):
        """Show cache status for all available years"""
        available_years = PastYearCourseCategory.get_available_academic_years()

        if not available_years:
            self.stdout.write(self.style.WARNING('No academic years found'))
            return

        self.stdout.write("Past Years Analytics - Cache Status")
        self.stdout.write("=" * 50)

        for year in available_years:
            self.stdout.write(f"\nAcademic Year {year}:")

            # Check main cache keys
            cache_key_base = f'student_analytics_{year}'
            main_keys = [
                f'{cache_key_base}_main_all_activities',
                f'{cache_key_base}_main_graded_only',
                f'{cache_key_base}_courses_context',
            ]

            cached_count = sum(1 for key in main_keys if cache.get(key) is not None)
            total_count = len(main_keys)

            # Check registry for course distributions
            registry_key = f'cache_registry_{year}'
            cached_course_keys = cache.get(registry_key, [])
            course_cache_count = len(cached_course_keys)

            self.stdout.write(f"  Main Analytics: {cached_count}/{total_count} cached")
            self.stdout.write(f"  Course Distributions: {course_cache_count} cached")

            if cached_count > 0 or course_cache_count > 0:
                self.stdout.write(f"  Status: {self.style.SUCCESS('CACHED')}")
            else:
                self.stdout.write(f"  Status: {self.style.WARNING('NO CACHE')}")

        self.stdout.write("\nUsage:")
        self.stdout.write("  python manage.py clear_past_years_cache --year 2024")
        self.stdout.write("  python manage.py clear_past_years_cache --all-years")
        self.stdout.write("  python manage.py clear_past_years_cache --list-keys --year 2024")