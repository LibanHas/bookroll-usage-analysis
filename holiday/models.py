from django.db import models
from django.utils.translation import gettext_lazy as _
from typing import Optional


class JapaneseHoliday(models.Model):
    """
    Model to store Japanese national holidays.

    This model stores data fetched from the Japanese holidays API
    (https://holidays-jp.github.io/api/v1/{year}/date.json)
    """

    date = models.DateField(
        unique=True,
        verbose_name=_("Holiday Date"),
        help_text=_("The date of the holiday")
    )

    name = models.CharField(
        max_length=100,
        verbose_name=_("Holiday Name"),
        help_text=_("The name of the holiday in Japanese")
    )

    name_en = models.CharField(
        max_length=100,
        blank=True,
        null=True,
        verbose_name=_("Holiday Name (English)"),
        help_text=_("The name of the holiday in English")
    )

    year = models.PositiveIntegerField(
        verbose_name=_("Year"),
        help_text=_("The year this holiday occurs"),
        db_index=True
    )

    created_at = models.DateTimeField(
        auto_now_add=True,
        verbose_name=_("Created At")
    )

    updated_at = models.DateTimeField(
        auto_now=True,
        verbose_name=_("Updated At")
    )

    class Meta:
        db_table = 'japanese_holidays'
        db_table_comment = 'Japanese national holidays data'
        verbose_name = _("Japanese Holiday")
        verbose_name_plural = _("Japanese Holidays")
        ordering = ['date']
        indexes = [
            models.Index(fields=['year'], name='holiday_year_idx'),
            models.Index(fields=['date'], name='holiday_date_idx'),
        ]

    def __str__(self) -> str:
        return f"{self.date.strftime('%Y-%m-%d')}: {self.name}"

    @property
    def is_current_year(self) -> bool:
        """Check if this holiday is in the current year."""
        from datetime import datetime
        return self.year == datetime.now().year

    @classmethod
    def get_holidays_for_year(cls, year: int):
        """Get all holidays for a specific year."""
        return cls.objects.filter(year=year).order_by('date')

    @classmethod
    def get_upcoming_holidays(cls, limit: Optional[int] = 5):
        """Get upcoming holidays from today."""
        from datetime import date
        today = date.today()
        queryset = cls.objects.filter(date__gte=today).order_by('date')
        if limit:
            queryset = queryset[:limit]
        return queryset