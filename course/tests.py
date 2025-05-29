from django.test import TestCase
from django.utils import timezone
from datetime import datetime, timedelta

from .models import Course


class CourseModelTest(TestCase):
    """
    Test cases for the Course model.
    """

    def setUp(self):
        """Set up test data."""
        self.course_data = {
            'course_id': 123,
            'course_name': 'Test Course',
            'parent_category_id': 1,
            'parent_category_name': 'Mathematics',
            'child_category_id': 11,
            'child_category_name': 'Algebra',
            'course_sortorder': 1,
            'course_visible': True,
            'course_created': timezone.now(),
        }

    def test_course_creation(self):
        """Test creating a course."""
        course = Course.objects.create(**self.course_data)
        self.assertEqual(course.course_id, 123)
        self.assertEqual(course.course_name, 'Test Course')
        self.assertTrue(course.course_visible)

    def test_course_str_method(self):
        """Test the string representation of a course."""
        course = Course.objects.create(**self.course_data)
        expected_str = f"Test Course (ID: 123)"
        self.assertEqual(str(course), expected_str)

    def test_is_active_property(self):
        """Test the is_active property."""
        # Test visible course without dates
        course = Course.objects.create(**self.course_data)
        self.assertTrue(course.is_active)

        # Test invisible course
        course.course_visible = False
        course.save()
        self.assertFalse(course.is_active)

        # Test course with future start date
        course.course_visible = True
        course.course_startdate = timezone.now() + timedelta(days=1)
        course.save()
        self.assertFalse(course.is_active)

        # Test course with past end date
        course.course_startdate = timezone.now() - timedelta(days=2)
        course.course_enddate = timezone.now() - timedelta(days=1)
        course.save()
        self.assertFalse(course.is_active)

    def test_full_category_path_property(self):
        """Test the full_category_path property."""
        course = Course.objects.create(**self.course_data)
        expected_path = "Mathematics > Algebra"
        self.assertEqual(course.full_category_path, expected_path)