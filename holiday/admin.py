from django.contrib import admin
from django.utils.translation import gettext_lazy as _
from .models import JapaneseHoliday


@admin.register(JapaneseHoliday)
class JapaneseHolidayAdmin(admin.ModelAdmin):
    """Admin interface for Japanese holidays."""

    list_display = ['date', 'name', 'name_en', 'year', 'created_at']
    list_filter = ['year', 'created_at']
    search_fields = ['name', 'name_en', 'date']
    readonly_fields = ['created_at', 'updated_at']
    ordering = ['-date']

    fieldsets = (
        (_('Holiday Information'), {
            'fields': ('date', 'name', 'name_en', 'year')
        }),
        (_('Timestamps'), {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )

    def get_queryset(self, request):
        """Optimize queryset for admin list view."""
        return super().get_queryset(request).select_related()

    def has_add_permission(self, request):
        """Allow adding holidays manually if needed."""
        return True

    def has_change_permission(self, request, obj=None):
        """Allow editing holidays."""
        return True

    def has_delete_permission(self, request, obj=None):
        """Allow deleting holidays."""
        return True