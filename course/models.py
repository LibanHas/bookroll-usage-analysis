from django.db import models
from django.conf import settings
from typing import Optional


class Course(models.Model):
    """
    Course model storing information pulled from Moodle database.
    Based on the query from mdl_course_categories and mdl_course tables.
    """

    # Course identification
    course_id = models.IntegerField(unique=True, help_text="Moodle course ID")
    course_name = models.CharField(max_length=255, help_text="Course full name from Moodle")

    # Category information
    parent_category_id = models.IntegerField(help_text="Parent category ID from Moodle")
    parent_category_name = models.CharField(max_length=255, help_text="Parent category name")
    child_category_id = models.IntegerField(help_text="Child category ID from Moodle")
    child_category_name = models.CharField(max_length=255, help_text="Child category name")

    # Course details
    course_sortorder = models.IntegerField(default=0, help_text="Sort order in Moodle")
    course_visible = models.BooleanField(default=True, help_text="Course visibility in Moodle")
    course_startdate = models.DateTimeField(null=True, blank=True, help_text="Course start date")
    course_enddate = models.DateTimeField(null=True, blank=True, help_text="Course end date")
    course_created = models.DateTimeField(help_text="Course creation timestamp")

    # Subject category (custom field)
    subject_category = models.CharField(
        max_length=20,
        choices=getattr(settings, 'COURSE_SUBJECT_CATEGORIES', []),
        null=True,
        blank=True,
        help_text="Subject category classification"
    )

    # Level category (custom field)
    level_category = models.CharField(
        max_length=20,
        choices=getattr(settings, 'COURSE_LEVEL_CATEGORIES', []),
        null=True,
        blank=True,
        help_text="Level category classification"
    )

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_synced = models.DateTimeField(auto_now=True, help_text="Last synchronization with Moodle")

    class Meta:
        db_table = 'course_course'
        verbose_name = 'Course'
        verbose_name_plural = 'Courses'
        ordering = ['parent_category_name', 'child_category_name', 'course_sortorder']
        indexes = [
            models.Index(fields=['course_id']),
            models.Index(fields=['parent_category_id']),
            models.Index(fields=['child_category_id']),
            models.Index(fields=['subject_category']),
            models.Index(fields=['level_category']),
            models.Index(fields=['course_visible']),
        ]

    def __str__(self) -> str:
        return f"{self.course_name} (ID: {self.course_id})"

    @property
    def is_active(self) -> bool:
        """Check if course is currently active based on dates and visibility."""
        if not self.course_visible:
            return False

        from django.utils import timezone
        now = timezone.now()

        if self.course_startdate and self.course_startdate > now:
            return False

        if self.course_enddate and self.course_enddate < now:
            return False

        return True

    @property
    def full_category_path(self) -> str:
        """Get the full category path."""
        return f"{self.parent_category_name} > {self.child_category_name}"