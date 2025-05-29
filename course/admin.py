from django.contrib import admin
from .models import Course


@admin.register(Course)
class CourseAdmin(admin.ModelAdmin):
    """
    Django admin configuration for Course model.
    """
    list_display = [
        'course_id',
        'course_name',
        'parent_category_name',
        'child_category_name',
        'subject_category',
        'course_visible',
        'is_active',
        'last_synced',
    ]

    list_filter = [
        'subject_category',
        'course_visible',
        'parent_category_name',
        'child_category_name',
        'course_startdate',
        'course_enddate',
        'last_synced',
    ]

    search_fields = [
        'course_name',
        'course_id',
        'parent_category_name',
        'child_category_name',
    ]

    readonly_fields = [
        'course_id',
        'created_at',
        'updated_at',
        'last_synced',
        'full_category_path',
        'is_active',
    ]

    fieldsets = (
        ('Basic Information', {
            'fields': ('course_id', 'course_name', 'subject_category')
        }),
        ('Category Information', {
            'fields': (
                'parent_category_id',
                'parent_category_name',
                'child_category_id',
                'child_category_name',
                'full_category_path',
            )
        }),
        ('Course Details', {
            'fields': (
                'course_sortorder',
                'course_visible',
                'course_startdate',
                'course_enddate',
                'course_created',
                'is_active',
            )
        }),
        ('Metadata', {
            'fields': ('created_at', 'updated_at', 'last_synced'),
            'classes': ('collapse',)
        }),
    )

    ordering = ['parent_category_name', 'child_category_name', 'course_sortorder']

    def is_active(self, obj):
        """Display if course is active."""
        return obj.is_active
    is_active.boolean = True
    is_active.short_description = 'Active'

    def get_queryset(self, request):
        """Optimize queryset for admin list view."""
        return super().get_queryset(request).select_related()