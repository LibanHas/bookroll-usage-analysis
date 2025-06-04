import requests
import json
from datetime import datetime, date
from typing import Dict, Any, List
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils.translation import gettext as _
from holiday.models import JapaneseHoliday


class Command(BaseCommand):
    """
    Management command to fetch Japanese holidays from the API.

    This command fetches holiday data from https://holidays-jp.github.io/api/v1/{year}/date.json
    for years 2018-2028 and stores them in the database.

    Usage:
        python manage.py fetch_holidays
        python manage.py fetch_holidays --year 2023
        python manage.py fetch_holidays --start-year 2020 --end-year 2025
        python manage.py fetch_holidays --force-update
    """

    help = 'Fetch Japanese holidays from API and store in database'

    # Holiday name translations (Japanese to English)
    HOLIDAY_TRANSLATIONS = {
        '元日': 'New Year\'s Day',
        '休日 元日': 'New Year\'s Day Holiday',
        '成人の日': 'Coming of Age Day',
        '建国記念の日': 'National Foundation Day',
        '天皇誕生日': 'Emperor\'s Birthday',
        '春分の日': 'Vernal Equinox Day',
        '昭和の日': 'Showa Day',
        '憲法記念日': 'Constitution Memorial Day',
        'みどりの日': 'Greenery Day',
        'こどもの日': 'Children\'s Day',
        '海の日': 'Marine Day',
        '山の日': 'Mountain Day',
        '敬老の日': 'Respect for the Aged Day',
        '秋分の日': 'Autumnal Equinox Day',
        'スポーツの日': 'Sports Day',
        '文化の日': 'Culture Day',
        '勤労感謝の日': 'Labor Thanksgiving Day',
    }

    def add_arguments(self, parser):
        """Add command line arguments."""
        parser.add_argument(
            '--year',
            type=int,
            help='Specific year to fetch holidays for'
        )
        parser.add_argument(
            '--start-year',
            type=int,
            default=2018,
            help='Start year for fetching holidays (default: 2018)'
        )
        parser.add_argument(
            '--end-year',
            type=int,
            default=2028,
            help='End year for fetching holidays (default: 2028)'
        )
        parser.add_argument(
            '--force-update',
            action='store_true',
            help='Force update existing holidays'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be done without making changes'
        )

    def handle(self, *args, **options):
        """Main command handler."""
        self.verbosity = options['verbosity']
        self.force_update = options['force_update']
        self.dry_run = options['dry_run']

        if self.dry_run:
            self.stdout.write(
                self.style.WARNING('DRY RUN MODE - No changes will be made')
            )

        # Determine years to process
        if options['year']:
            years = [options['year']]
        else:
            start_year = options['start_year']
            end_year = options['end_year']
            years = list(range(start_year, end_year + 1))

        self.stdout.write(f'Fetching holidays for years: {years}')

        total_created = 0
        total_updated = 0
        total_errors = 0

        for year in years:
            try:
                created, updated = self.fetch_holidays_for_year(year)
                total_created += created
                total_updated += updated

                if self.verbosity >= 1:
                    self.stdout.write(
                        f'Year {year}: {created} created, {updated} updated'
                    )

            except Exception as e:
                total_errors += 1
                self.stdout.write(
                    self.style.ERROR(f'Error fetching holidays for {year}: {e}')
                )

        # Summary
        self.stdout.write(
            self.style.SUCCESS(
                f'Completed! Total: {total_created} created, '
                f'{total_updated} updated, {total_errors} errors'
            )
        )

    def fetch_holidays_for_year(self, year: int) -> tuple[int, int]:
        """
        Fetch holidays for a specific year.

        Returns:
            tuple: (created_count, updated_count)
        """
        url = f'https://holidays-jp.github.io/api/v1/{year}/date.json'

        if self.verbosity >= 2:
            self.stdout.write(f'Fetching from: {url}')

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            holidays_data = response.json()

            if not holidays_data:
                self.stdout.write(
                    self.style.WARNING(f'No holidays found for year {year}')
                )
                return 0, 0

            return self.process_holidays_data(holidays_data, year)

        except requests.RequestException as e:
            raise CommandError(f'Failed to fetch data from API: {e}')
        except json.JSONDecodeError as e:
            raise CommandError(f'Failed to parse JSON response: {e}')

    def process_holidays_data(self, holidays_data: Dict[str, str], year: int) -> tuple[int, int]:
        """
        Process and save holidays data.

        Args:
            holidays_data: Dictionary with date strings as keys and holiday names as values
            year: The year being processed

        Returns:
            tuple: (created_count, updated_count)
        """
        created_count = 0
        updated_count = 0

        if self.dry_run:
            self.stdout.write(f'Would process {len(holidays_data)} holidays for {year}')
            for date_str, name in holidays_data.items():
                self.stdout.write(f'  {date_str}: {name}')
            return len(holidays_data), 0

        with transaction.atomic():
            for date_str, holiday_name in holidays_data.items():
                try:
                    # Parse date
                    holiday_date = datetime.strptime(date_str, '%Y-%m-%d').date()

                    # Get English translation
                    name_en = self.HOLIDAY_TRANSLATIONS.get(holiday_name, '')

                    # Create or update holiday
                    holiday, created = JapaneseHoliday.objects.update_or_create(
                        date=holiday_date,
                        defaults={
                            'name': holiday_name,
                            'name_en': name_en,
                            'year': year,
                        }
                    )

                    if created:
                        created_count += 1
                        if self.verbosity >= 2:
                            self.stdout.write(f'Created: {holiday}')
                    elif self.force_update:
                        updated_count += 1
                        if self.verbosity >= 2:
                            self.stdout.write(f'Updated: {holiday}')

                except ValueError as e:
                    self.stdout.write(
                        self.style.ERROR(f'Invalid date format {date_str}: {e}')
                    )
                except Exception as e:
                    self.stdout.write(
                        self.style.ERROR(f'Error processing {date_str}: {e}')
                    )

        return created_count, updated_count

    def get_existing_years(self) -> List[int]:
        """Get list of years that already have holiday data."""
        return list(
            JapaneseHoliday.objects.values_list('year', flat=True)
            .distinct()
            .order_by('year')
        )