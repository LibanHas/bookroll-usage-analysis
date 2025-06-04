from django.shortcuts import render
from django.views.generic import ListView, DetailView
from django.contrib.auth.mixins import LoginRequiredMixin

from .models import Course


# For future use - basic class-based views
class CourseListView(LoginRequiredMixin, ListView):
    """
    List view for courses (for future frontend implementation).
    """
    model = Course
    template_name = 'course/course_list.html'
    context_object_name = 'courses'
    paginate_by = 20

    def get_queryset(self):
        return Course.objects.filter(course_visible=True).order_by(
            'parent_category_name', 'child_category_name', 'course_sortorder'
        )


class CourseDetailView(LoginRequiredMixin, DetailView):
    """
    Detail view for a single course (for future frontend implementation).
    """
    model = Course
    template_name = 'course/course_detail.html'
    context_object_name = 'course'

    def get_queryset(self):
        return Course.objects.filter(course_visible=True)