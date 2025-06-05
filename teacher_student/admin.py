from django.contrib import admin
from .models import TeacherExclusion, Teacher

@admin.register(TeacherExclusion)
class TeacherExclusionAdmin(admin.ModelAdmin):
    list_display = ('name', 'lms_id', 'reason', 'is_active', 'created_at')
    list_filter = ('is_active', 'created_at', 'updated_at')
    search_fields = ('name', 'lms_id', 'reason')
    list_editable = ('is_active',)
    readonly_fields = ('created_at', 'updated_at')

    fieldsets = (
        ('Teacher Information', {
            'fields': ('name', 'lms_id')
        }),
        ('Exclusion Details', {
            'fields': ('reason', 'is_active')
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        }),
    )

    def get_queryset(self, request):
        queryset = super().get_queryset(request)
        return queryset.order_by('-created_at')

    def save_model(self, request, obj, form, change):
        """Clear cache when a TeacherExclusion record is saved."""
        super().save_model(request, obj, form, change)
        # Clear both teacher and exclusion caches
        Teacher.clear_teacher_cache()

    def delete_model(self, request, obj):
        """Clear cache when a TeacherExclusion record is deleted."""
        super().delete_model(request, obj)
        # Clear both teacher and exclusion caches
        Teacher.clear_teacher_cache()

    def delete_queryset(self, request, queryset):
        """Clear cache when multiple TeacherExclusion records are deleted."""
        super().delete_queryset(request, queryset)
        # Clear both teacher and exclusion caches
        Teacher.clear_teacher_cache()

# Register your models here.
