from django.core.management.base import BaseCommand
from django.db import transaction
from django.conf import settings
from course.models import Course
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Django management command to update level_category field based on child_category_name content.

    Usage:
        python manage.py update_level_categories
        python manage.py update_level_categories --dry-run
    """

    help = 'Update level_category field based on child_category_name content (高=high, 中=junior)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be updated without making actual changes',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']

        self.stdout.write(
            self.style.SUCCESS('Starting level category update process...')
        )

        if dry_run:
            self.stdout.write(
                self.style.WARNING('DRY RUN MODE - No changes will be made to the database')
            )

        # Get level category choices from settings
        level_categories = dict(getattr(settings, 'COURSE_LEVEL_CATEGORIES', []))
        high_key = None
        junior_key = None

        # Find the keys for 'High' and 'Junior' from settings
        for key, value in level_categories.items():
            if value == 'High':
                high_key = key
            elif value == 'Junior':
                junior_key = key

        if not high_key or not junior_key:
            self.stdout.write(
                self.style.ERROR('Could not find "High" or "Junior" in COURSE_LEVEL_CATEGORIES settings')
            )
            return

        self.stdout.write(f'Using level categories: {high_key}="High", {junior_key}="Junior"')

        # Get all courses
        courses = Course.objects.all()
        total_courses = courses.count()

        self.stdout.write(f'Found {total_courses} courses to process')

        # Counters for tracking updates
        high_updates = 0
        junior_updates = 0
        no_match_count = 0
        already_set_count = 0

        try:
            with transaction.atomic():
                for course in courses:
                    child_category_name = course.child_category_name or ''
                    current_level = course.level_category
                    new_level = None

                    # Check for Japanese characters
                    if '高' in child_category_name:
                        new_level = high_key
                        if current_level != new_level:
                            high_updates += 1
                            if not dry_run:
                                course.level_category = new_level
                                course.save(update_fields=['level_category'])
                            self.stdout.write(
                                f'  Course {course.course_id}: "{child_category_name}" -> {new_level} (High)'
                            )
                        else:
                            already_set_count += 1

                    elif '中' in child_category_name:
                        new_level = junior_key
                        if current_level != new_level:
                            junior_updates += 1
                            if not dry_run:
                                course.level_category = new_level
                                course.save(update_fields=['level_category'])
                            self.stdout.write(
                                f'  Course {course.course_id}: "{child_category_name}" -> {new_level} (Junior)'
                            )
                        else:
                            already_set_count += 1

                    else:
                        no_match_count += 1
                        if self.verbosity >= 2:  # Only show in verbose mode
                            self.stdout.write(
                                f'  Course {course.course_id}: "{child_category_name}" -> No match found'
                            )

                if dry_run:
                    # Rollback transaction in dry run mode
                    transaction.set_rollback(True)
                    self.stdout.write(
                        self.style.WARNING('DRY RUN: All changes have been rolled back')
                    )

        except Exception as e:
            logger.error(f'Error updating level categories: {str(e)}')
            self.stdout.write(
                self.style.ERROR(f'Error occurred: {str(e)}')
            )
            return

        # Print summary
        self.stdout.write('\n' + '='*60)
        self.stdout.write(self.style.SUCCESS('UPDATE SUMMARY:'))
        self.stdout.write(f'Total courses processed: {total_courses}')
        self.stdout.write(f'Updated to "High": {high_updates}')
        self.stdout.write(f'Updated to "Junior": {junior_updates}')
        self.stdout.write(f'Already correctly set: {already_set_count}')
        self.stdout.write(f'No match found: {no_match_count}')
        self.stdout.write('='*60)

        if dry_run:
            self.stdout.write(
                self.style.WARNING('This was a DRY RUN - no actual changes were made.')
            )
            self.stdout.write(
                self.style.SUCCESS('Run without --dry-run to apply changes.')
            )
        else:
            self.stdout.write(
                self.style.SUCCESS('Level category update completed successfully!')
            )