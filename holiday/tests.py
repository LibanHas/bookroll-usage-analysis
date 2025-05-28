from django.test import TestCase, Client
from django.urls import reverse
from django.utils import timezone
from datetime import date, datetime
from .models import JapaneseHoliday


class JapaneseHolidayModelTest(TestCase):
    """Test cases for JapaneseHoliday model."""

    def setUp(self):
        """Set up test data."""
        self.holiday = JapaneseHoliday.objects.create(
            date=date(2023, 1, 1),
            name='元日',
            name_en='New Year\'s Day',
            year=2023
        )

    def test_string_representation(self):
        """Test the string representation of the model."""
        expected = "2023-01-01: 元日"
        self.assertEqual(str(self.holiday), expected)

    def test_is_current_year_property(self):
        """Test the is_current_year property."""
        current_year = datetime.now().year
        current_year_holiday = JapaneseHoliday.objects.create(
            date=date(current_year, 5, 5),
            name='こどもの日',
            name_en='Children\'s Day',
            year=current_year
        )
        self.assertTrue(current_year_holiday.is_current_year)
        self.assertFalse(self.holiday.is_current_year)

    def test_get_holidays_for_year(self):
        """Test getting holidays for a specific year."""
        # Create another holiday for the same year
        JapaneseHoliday.objects.create(
            date=date(2023, 5, 5),
            name='こどもの日',
            name_en='Children\'s Day',
            year=2023
        )

        holidays_2023 = JapaneseHoliday.get_holidays_for_year(2023)
        self.assertEqual(holidays_2023.count(), 2)

        holidays_2024 = JapaneseHoliday.get_holidays_for_year(2024)
        self.assertEqual(holidays_2024.count(), 0)

    def test_get_upcoming_holidays(self):
        """Test getting upcoming holidays."""
        # Create a future holiday
        future_date = date(2030, 1, 1)
        JapaneseHoliday.objects.create(
            date=future_date,
            name='元日',
            name_en='New Year\'s Day',
            year=2030
        )

        upcoming = JapaneseHoliday.get_upcoming_holidays(limit=5)
        self.assertTrue(upcoming.count() >= 1)


class HolidayViewsTest(TestCase):
    """Test cases for holiday views."""

    def setUp(self):
        """Set up test data and client."""
        self.client = Client()
        self.holiday = JapaneseHoliday.objects.create(
            date=date(2023, 1, 1),
            name='元日',
            name_en='New Year\'s Day',
            year=2023
        )

    def test_holiday_list_view(self):
        """Test the holiday list view."""
        url = reverse('holiday:holiday-list')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, '元日')

    def test_holiday_detail_view(self):
        """Test the holiday detail view."""
        url = reverse('holiday:holiday-detail', kwargs={'pk': self.holiday.pk})
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)

    def test_holiday_api_view(self):
        """Test the holiday API view."""
        url = reverse('holiday:holiday-api')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response['Content-Type'], 'application/json')

    def test_upcoming_holidays_view(self):
        """Test the upcoming holidays view."""
        url = reverse('holiday:upcoming-holidays')
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)