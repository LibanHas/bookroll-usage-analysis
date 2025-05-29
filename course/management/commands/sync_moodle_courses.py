from django.core.management.base import BaseCommand, CommandError
from django.db import connections, transaction
from django.utils import timezone
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

from course.models import Course


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Management command to sync course information from Moodle database.

    Usage:
        python manage.py sync_moodle_courses
        python manage.py sync_moodle_courses --dry-run
        python manage.py sync_moodle_courses --course-id 123
    """
    help = 'Sync course information from Moodle database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Run without making changes to the database',
        )
        parser.add_argument(
            '--course-id',
            type=int,
            help='Sync only specific course by ID',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Enable verbose output',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        course_id = options.get('course_id')
        verbose = options['verbose']

        if verbose:
            logger.setLevel(logging.DEBUG)

        self.stdout.write(
            self.style.SUCCESS(
                f"Starting Moodle course sync {'(DRY RUN)' if dry_run else ''}"
            )
        )

        try:
            courses_data = self.fetch_courses_from_moodle(course_id)

            if not courses_data:
                self.stdout.write(
                    self.style.WARNING("No courses found in Moodle database")
                )
                return

            self.stdout.write(f"Found {len(courses_data)} courses to process")

            if not dry_run:
                results = self.sync_courses(courses_data)
                self.display_results(results)
            else:
                self.stdout.write("DRY RUN - No changes made")
                for course_data in courses_data[:5]:  # Show first 5 for preview
                    self.stdout.write(f"  Would sync: {course_data['course_name']} (ID: {course_data['course_id']})")

        except Exception as e:
            logger.error(f"Error syncing courses: {e}")
            raise CommandError(f"Failed to sync courses: {e}")

    def fetch_courses_from_moodle(self, course_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch course information from Moodle database using the provided query.
        """
        moodle_db = connections['moodle_db']

        base_query = """
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
        """

        if course_id:
            base_query += f" AND course.id = {course_id}"

        base_query += " ORDER BY parent_cat.sortorder, child_cat.sortorder, course.sortorder"

        with moodle_db.cursor() as cursor:
            cursor.execute(base_query)
            columns = [col[0] for col in cursor.description]

            courses_data = []
            for row in cursor.fetchall():
                course_dict = dict(zip(columns, row))

                # Skip if no course (LEFT JOIN might return NULL course data)
                if not course_dict['course_id']:
                    continue

                # Convert timestamps to datetime objects
                if course_dict['course_startdate'] and course_dict['course_startdate'] != 0:
                    try:
                        course_dict['course_startdate'] = datetime.fromtimestamp(
                            course_dict['course_startdate'], tz=timezone.get_current_timezone()
                        )
                    except (ValueError, OSError, TypeError):
                        course_dict['course_startdate'] = None
                else:
                    course_dict['course_startdate'] = None

                if course_dict['course_enddate'] and course_dict['course_enddate'] != 0:
                    try:
                        course_dict['course_enddate'] = datetime.fromtimestamp(
                            course_dict['course_enddate'], tz=timezone.get_current_timezone()
                        )
                    except (ValueError, OSError, TypeError):
                        course_dict['course_enddate'] = None
                else:
                    course_dict['course_enddate'] = None

                if course_dict['course_created'] and course_dict['course_created'] != 0:
                    try:
                        course_dict['course_created'] = datetime.fromtimestamp(
                            course_dict['course_created'], tz=timezone.get_current_timezone()
                        )
                    except (ValueError, OSError, TypeError):
                        # Set to current time if creation time is invalid
                        course_dict['course_created'] = timezone.now()
                else:
                    # Set to current time if creation time is None/empty/0
                    course_dict['course_created'] = timezone.now()

                courses_data.append(course_dict)

        return courses_data

    def sync_courses(self, courses_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Sync courses with create or update logic.
        """
        results = {
            'created': 0,
            'updated': 0,
            'errors': 0,
        }

        for course_data in courses_data:
            try:
                with transaction.atomic():
                    course_id = course_data['course_id']

                    # Try to get existing course
                    try:
                        course = Course.objects.get(course_id=course_id)

                        # Check if any field has changed
                        updated = False
                        update_fields = []

                        # Check each field for changes
                        field_mappings = {
                            'course_name': 'course_name',
                            'parent_category_id': 'parent_category_id',
                            'parent_category_name': 'parent_category_name',
                            'child_category_id': 'child_category_id',
                            'child_category_name': 'child_category_name',
                            'course_sortorder': 'course_sortorder',
                            'course_visible': 'course_visible',
                            'course_startdate': 'course_startdate',
                            'course_enddate': 'course_enddate',
                            'course_created': 'course_created',
                        }

                        for db_field, moodle_field in field_mappings.items():
                            current_value = getattr(course, db_field)
                            new_value = course_data[moodle_field]

                            if current_value != new_value:
                                setattr(course, db_field, new_value)
                                update_fields.append(db_field)
                                updated = True

                        if updated:
                            update_fields.append('last_synced')
                            course.save(update_fields=update_fields)
                            results['updated'] += 1
                            logger.debug(f"Updated course {course_id}: {', '.join(update_fields)}")
                        else:
                            # Still update last_synced even if no data changed
                            course.last_synced = timezone.now()
                            course.save(update_fields=['last_synced'])
                            logger.debug(f"No changes for course {course_id}")

                    except Course.DoesNotExist:
                        # Create new course
                        course = Course.objects.create(
                            course_id=course_data['course_id'],
                            course_name=course_data['course_name'],
                            parent_category_id=course_data['parent_category_id'],
                            parent_category_name=course_data['parent_category_name'],
                            child_category_id=course_data['child_category_id'],
                            child_category_name=course_data['child_category_name'],
                            course_sortorder=course_data['course_sortorder'],
                            course_visible=course_data['course_visible'],
                            course_startdate=course_data['course_startdate'],
                            course_enddate=course_data['course_enddate'],
                            course_created=course_data['course_created'],
                            # subject_category will be null initially and can be set manually
                        )
                        results['created'] += 1
                        logger.debug(f"Created course {course_id}")

            except Exception as e:
                results['errors'] += 1
                logger.error(f"Error processing course {course_data.get('course_id', 'unknown')}: {e}")
                logger.error(f"Course data: {course_data}")
                # Continue with next course instead of stopping
                continue

        return results

    def display_results(self, results: Dict[str, int]):
        """
        Display sync results.
        """
        self.stdout.write("\n" + "="*50)
        self.stdout.write(self.style.SUCCESS("SYNC COMPLETED"))
        self.stdout.write("="*50)

        self.stdout.write(f"Created: {results['created']} courses")
        self.stdout.write(f"Updated: {results['updated']} courses")

        if results['errors'] > 0:
            self.stdout.write(
                self.style.ERROR(f"Errors: {results['errors']} courses")
            )
        else:
            self.stdout.write(self.style.SUCCESS("No errors"))

        total_processed = results['created'] + results['updated']
        self.stdout.write(f"Total processed: {total_processed} courses")