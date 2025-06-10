import datetime
import json
from django.views.generic import ListView, DetailView, TemplateView
from .models import Teacher, Student, StudentDetails, TeacherDetails
from django.http import Http404, JsonResponse, HttpResponse
from django.db import connections
from django.utils import timezone
from django.contrib import messages
from django.shortcuts import redirect
from django.urls import reverse
import logging
import os
import requests
from django.views import View

class TeacherListView(ListView):
    model = Teacher
    template_name = 'teacher_list.html'
    context_object_name = 'teachers'
    paginate_by = 50  # Number of teachers per page

    def get_queryset(self):
        # Check if user is staff or admin
        user = self.request.user
        is_staff_or_admin = user.is_authenticated and (user.is_staff or user.is_superuser)

        # Only allow search for staff/admin users
        search_term = self.request.GET.get('search', '') if is_staff_or_admin else ''
        sort_by = self.request.GET.get('sort_by', 'active_courses')  # Default to active_courses
        sort_order = self.request.GET.get('sort_order', 'desc')      # Default to desc for active courses

        # Validate sort parameters
        valid_sort_fields = ['active_courses', 'archived_courses', 'total_courses', 'name']
        if sort_by not in valid_sort_fields:
            sort_by = 'active_courses'  # Default fallback

        if sort_order not in ['asc', 'desc']:
            sort_order = 'desc'  # Default to desc for better UX

        teachers = Teacher.get_teacher_data(
            sort_by=sort_by,
            sort_order=sort_order,
            use_cache=True
        )

        # Apply search filter if search term is provided and user is staff/admin
        if search_term and is_staff_or_admin:
            filtered_teachers = []
            search_term_lower = search_term.lower()
            for teacher in teachers:
                # Search in firstname, lastname, email, username
                if (search_term_lower in teacher['firstname'].lower() or
                    search_term_lower in teacher['lastname'].lower() or
                    search_term_lower in teacher['email'].lower() or
                    search_term_lower in str(teacher['username']).lower()):
                    filtered_teachers.append(teacher)
            return filtered_teachers
        return teachers

    def _anonymize_teacher_names(self, teachers):
        """
        Anonymize teacher names for non-staff users.
        Returns list with anonymized names like 'Teacher 1', 'Teacher 2', etc.
        """
        anonymized_teachers = []
        for index, teacher in enumerate(teachers, 1):
            teacher_copy = teacher.copy()
            teacher_copy['display_name'] = f"Teacher {index}"
            teacher_copy['original_firstname'] = teacher['firstname']
            teacher_copy['original_lastname'] = teacher['lastname']
            anonymized_teachers.append(teacher_copy)
        return anonymized_teachers

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['additional_info'] = "This is additional context data"

        # Check if user is staff or admin (do this once)
        user = self.request.user
        is_staff_or_admin = user.is_authenticated and (user.is_staff or user.is_superuser)

        # Only pass search term to template if user is staff/admin
        context['search_term'] = self.request.GET.get('search', '') if is_staff_or_admin else ''
        context['current_sort_by'] = self.request.GET.get('sort_by', 'active_courses')
        context['current_sort_order'] = self.request.GET.get('sort_order', 'desc')

        # Check if staff user wants to show real names (default to False for privacy)
        show_names = self.request.GET.get('show_names', 'false').lower() == 'true'

        # Apply name anonymization logic
        teachers = context['teachers']
        if not is_staff_or_admin or (is_staff_or_admin and not show_names):
            # Anonymize names for regular users or staff users who haven't toggled to show names
            teachers = self._anonymize_teacher_names(teachers)
            context['names_anonymized'] = True
        else:
            # Staff users with show_names=true see real names
            for teacher in teachers:
                teacher['display_name'] = f"{teacher['firstname']} {teacher['lastname']}"
            context['names_anonymized'] = False

        context['teachers'] = teachers
        context['is_staff_or_admin'] = is_staff_or_admin
        context['show_names'] = show_names

        # Add breadcrumbs
        context['breadcrumbs'] = [
            {'name': 'Dashboard', 'url': 'index'},
            {'name': 'Teacher List', 'url': None}
        ]

        # Get time filter for chart (default to academic_year)
        chart_time_filter = self.request.GET.get('chart_filter', 'academic_year')

        # Get teacher activity data for chart (now returns stacked data)
        activity_data = Teacher.get_teacher_activity_data(time_filter=chart_time_filter)

        # Apply name anonymization to chart data as well
        if not is_staff_or_admin or (is_staff_or_admin and not show_names):
            # Anonymize chart categories (teacher names)
            chart_categories = activity_data.get('categories', [])
            anonymized_categories = [f"Teacher {i+1}" for i in range(len(chart_categories))]
            activity_data['categories'] = anonymized_categories

        # Prepare chart data for stacked bars
        context['chart_time_filter'] = chart_time_filter
        context['chart_series'] = json.dumps(activity_data.get('series', []))
        context['chart_categories'] = json.dumps(activity_data.get('categories', []))
        context['top_operations'] = activity_data.get('top_operations', [])

        # Calculate max value for Y-axis
        max_values = []
        for teacher_idx in range(len(activity_data.get('categories', []))):
            teacher_total = sum(
                series.get('data', [])[teacher_idx] if teacher_idx < len(series.get('data', [])) else 0
                for series in activity_data.get('series', [])
            )
            max_values.append(teacher_total)

        context['chart_max_value'] = max(max_values) if max_values else 100

        # Time filter options for dropdown
        context['time_filter_options'] = [
            {'value': 'academic_year', 'label': 'Current Academic Year'},
            {'value': 'this_month', 'label': 'This Month'},
            {'value': 'this_week', 'label': 'This Week'},
            {'value': 'today', 'label': 'Today'}
        ]

        return context

    def paginate_queryset(self, queryset, page_size):
        """Override to handle invalid page numbers gracefully."""
        try:
            return super().paginate_queryset(queryset, page_size)
        except Exception as e:
            # Log the error
            logger = logging.getLogger(__name__)
            logger.error(f"Pagination error: {str(e)}")

            # Reset to page 1
            self.kwargs[self.page_kwarg] = 1
            return super().paginate_queryset(queryset, page_size)


class TeacherCacheClearView(View):
    """
    View to handle cache clearing for teacher data and exclusions.
    """
    def post(self, request, *args, **kwargs):
        try:
            Teacher.clear_teacher_cache()
            messages.success(request, 'Teacher cache and exclusion filters cleared successfully!')
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Error clearing teacher cache: {str(e)}")
            messages.error(request, 'Failed to clear teacher cache.')

        return redirect('teacher_list')

class StudentListView(TemplateView):
    template_name = 'student_list.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        # Get search term and page
        search_term = self.request.GET.get('search', '')
        page = int(self.request.GET.get('page', 1))
        page_size = 50  # Number of students per page

        # Get student data with pagination
        student_data = Student.get_student_data(
            search_term=search_term,
            page=page,
            page_size=page_size
        )

        # Basic pagination context
        context['results'] = student_data
        context['search_term'] = search_term
        context['current_page'] = student_data.get('current_page', 1)
        context['total_pages'] = student_data.get('total_pages', 1)
        context['is_paginated'] = student_data.get('total_pages', 1) > 1

        # Create a paginator-like object for the template
        class CustomPaginator:
            def __init__(self, num_pages):
                self.num_pages = num_pages
                self.page_range = range(1, num_pages + 1)

        class CustomPage:
            def __init__(self, number, paginator, has_next, has_previous,
                         next_page_number, previous_page_number):
                self.number = number
                self.paginator = paginator
                self.has_next = has_next
                self.has_previous = has_previous
                self.next_page_number = next_page_number
                self.previous_page_number = previous_page_number

        current_page = student_data.get('current_page', 1)
        total_pages = student_data.get('total_pages', 1)

        paginator = CustomPaginator(total_pages)
        page_obj = CustomPage(
            number=current_page,
            paginator=paginator,
            has_next=current_page < total_pages,
            has_previous=current_page > 1,
            next_page_number=current_page + 1 if current_page < total_pages else current_page,
            previous_page_number=current_page - 1 if current_page > 1 else current_page
        )

        context['page_obj'] = page_obj

        return context

class StudentDetailsView(DetailView):
    model = StudentDetails
    template_name = 'student_detail.html'
    context_object_name = 'student'

    def get_object(self, queryset=None):
        """
        Fetch a single student's details using the user_id from the URL.
        """
        user_id = self.kwargs.get('user_id')
        student_details = StudentDetails.get_full_student_details(user_id)
        if not student_details:
            raise Http404("Student not found")
        return student_details

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        user_id = self.kwargs.get('user_id')
        context["activity_by_day"] = json.dumps(StudentDetails.get_student_activity_by_day(user_id), default=str)
        context["last_access_course_list"] = StudentDetails.get_student_last_access_course_list(user_id)
        context["LMS_URL"] = os.getenv('LMS_URL')
        return context


class TeacherDetailView(DetailView):
    model = TeacherDetails
    template_name = 'teacher_detail.html'
    context_object_name = 'teacher'

    def get_object(self, queryset=None):
        """
        Fetch a single teacher's details using the user_id from the URL.
        """
        user_id = self.kwargs.get('user_id')
        teacher_details = TeacherDetails.get_full_teacher_details(user_id)
        if not teacher_details:
            raise Http404("Teacher not found")
        return teacher_details

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        user_id = self.kwargs.get('user_id')
        context["last_access_course_list"] = TeacherDetails.get_teacher_last_access_course_list(user_id)
        context["LMS_URL"] = os.getenv('LMS_URL')
        return context


class StudentActivityLiveView(TemplateView):
    template_name = 'student_activity_live.html'

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        user_id = self.kwargs.get('user_id')
        context['user_id'] = user_id
        context['initial_activities'] = self.get_initial_activities(str(user_id))
        context['basic_info'] = StudentDetails.get_student_basic_info(user_id)
        context['last_action_time'] = StudentDetails.get_student_last_action_time(user_id)
        return context

    @staticmethod
    def get_initial_activities(user_id):
        """
        Retrieve the initial set of student activities for the dashboard.
        """
        clickhouse_query = """
            SELECT DISTINCT
                _id,
                operation_name as type,
                timestamp,
                platform,
                object_id,
                description,
                marker_color,
                marker_position,
                marker_text,
                title,
                memo_text,
                contents_id,
                contents_name,
                page_no,
                context_label
            FROM statements_mv
            WHERE actor_account_name = %(user_id)s
                AND timestamp >= now() - INTERVAL 1 HOUR
                AND actor_account_name != ''
                AND contents_id != ''
            ORDER BY timestamp DESC
            LIMIT 100
        """

        with connections['clickhouse_db'].cursor() as ch_cursor:
            ch_cursor.execute(clickhouse_query, {'user_id': user_id})
            rows = ch_cursor.fetchall()

        activities = []
        for row in rows:
            id = row[0]
            type_ = row[1]
            timestamp = row[2]
            platform = row[3]
            object_id = row[4]
            description = row[5]
            marker_color = row[6]
            marker_position = row[7]
            marker_text = row[8]
            title = row[9]
            memo_text = row[10]
            contents_id = row[11]
            contents_name = row[12]
            page_no = row[13]
            context_label = row[14]

            # Ensure timestamp is timezone-aware
            if timestamp is not None and timestamp.tzinfo is None:
                timestamp = timezone.make_aware(
                    timestamp,
                    timezone.get_default_timezone()
                )

            # Generate a human-readable label
            label = StudentActivityLiveView.get_activity_label(type_, contents_name, page_no)

            activity = {
                "id": id,
                "type": type_,
                "timestamp": timestamp.isoformat() if timestamp else None,
                "platform": platform,
                "object_id": object_id,
                "description": description,
                "marker_color": marker_color,
                "marker_position": marker_position,
                "marker_text": marker_text,
                "title": title,
                "memo_text": memo_text,
                "contents_id": contents_id,
                "contents_name": contents_name,
                "page_no": page_no,
                "context_label": context_label,
                "label": label
            }
            # print(activity)
            activities.append(activity)

        return json.dumps(activities)

    @staticmethod
    def get_activity_label(operation_type, contents_name, page_no):
        """
        Generate a human-readable label based on operation type, contents name, and page number.
        """
        if operation_type == "page_open":
            return f"{contents_name} (Page {page_no})"
        elif operation_type == "quiz_answer":
            return f"Answered Quiz on {contents_name} (Page {page_no})"
        elif operation_type == "next":
            return f"Navigated to Next Page: {contents_name} (Page {page_no})"
        elif operation_type == "close":
            return f"Closed {contents_name} (Page {page_no})"
        # Add more operation types as needed
        else:
            return f"{operation_type.replace('_', ' ').title()} - {contents_name} (Page {page_no})"

class ImageProxyView(View):
    def get(self, request):
        """
        Proxy for fetching images from internal API and serving them over HTTPS.
        Expected query parameters:
        - url: The URL to fetch the image from
        - token: The authentication token
        """
        url = request.GET.get('url')
        token = request.GET.get('token')

        if not url or not token:
            return JsonResponse({'error': 'Missing required parameters'}, status=400)

        try:
            # Fetch the image from the internal API
            response = requests.get(
                url,
                headers={
                    'Authorization': f'Bearer {token}',
                    'Accept': 'image/*'
                },
                timeout=10
            )

            # If the request was not successful, return an error
            if not response.ok:
                return JsonResponse({
                    'error': f'Failed to fetch image: {response.status_code}'
                }, status=response.status_code)

            # Get content type from response
            content_type = response.headers.get('Content-Type', 'image/jpeg')

            # Return the image with the appropriate content type
            return HttpResponse(
                content=response.content,
                content_type=content_type
            )

        except Exception as e:
            logging.error(f"Error proxying image: {str(e)}")
            return JsonResponse({'error': str(e)}, status=500)

