import json
import logging
from django.contrib.auth.views import LoginView, LogoutView
from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.urls import reverse_lazy
from django.contrib import messages
from django.http import HttpResponseRedirect
from django.utils.http import url_has_allowed_host_and_scheme
from django.conf import settings
from django.core.paginator import Paginator, PageNotAnInteger, EmptyPage
from django.db import connections
import datetime

from .models import MoodleUser, StudentCount, TotalCourses, TotalContents, ActiveStudents, MostActiveContents, DailyActiveUsers, DailyActivities, MostActiveStudents, MostMemoContents, MostMarkedContents, CourseCategory, CourseDetail

logger = logging.getLogger(__name__)


class IndexView(LoginRequiredMixin, TemplateView):
    """
    Display the main dashboard page for authenticated users.

    Requires the user to be logged in. If not logged in, the user will be
    redirected to the login page (handled by LoginRequiredMixin).
    """
    template_name = 'index.html'
    login_url = settings.LOGIN_URL  # for clarity, though LoginRequiredMixin uses LOGIN_URL by default

    def get_context_data(self, **kwargs):
        """
        Add Moodle users to the context for the template.
        """
        context = super().get_context_data(**kwargs)
        context['users'] = MoodleUser.objects.using('moodle_db').all()
        context['students_count'] = StudentCount.get_student_count()
        context['students_count_by_day'] = StudentCount.get_student_count_by_day()
        context['courses_count'] = TotalCourses.get_course_count()
        context['courses_count_by_day'] = TotalCourses.get_course_count_by_day()
        context['contents_count'] = TotalContents.get_content_count()
        context['contents_count_by_day'] = TotalContents.get_content_count_by_day()
        context['active_students'] = ActiveStudents.get_active_students()
        context['active_students_by_day'] = ActiveStudents.get_active_students_by_day()
        context['most_active_contents'] = MostActiveContents.get_most_active_contents()
        context['daily_active_users'] = json.dumps(DailyActiveUsers.get_daily_active_users())
        context['daily_activities'] = json.dumps(DailyActivities.get_daily_activities())
        context['most_active_students'] = MostActiveStudents.get_most_active_students_with_details()
        context['most_memo_contents'] = MostMemoContents.get_most_memo_contents()
        context['most_marked_contents'] = MostMarkedContents.get_most_marked_contents()


        return context

    def handle_no_permission(self):
        """
        Handle the case where a user tries to access this view without being logged in.
        """
        messages.warning(self.request, "You must be logged in to view the dashboard.")
        return super().handle_no_permission()

class CustomLoginView(LoginView):
    """
    A custom login view that:
    - Renders a login form
    - Authenticates the user
    - Handles invalid credentials gracefully
    - Redirects authenticated users to the index page or a specified 'next' URL if valid
    """
    template_name = 'login.html'
    redirect_authenticated_user = True
    # If a valid 'next' param is provided, user will be redirected there after login.
    # Otherwise, LOGIN_REDIRECT_URL will be used.

    def get_redirect_url(self):
        """
        Override get_redirect_url to ensure redirection to a safe URL.
        """
        redirect_url = self.request.POST.get('next') or self.request.GET.get('next')
        if redirect_url and url_has_allowed_host_and_scheme(
                url=redirect_url,
                allowed_hosts={self.request.get_host()},
                require_https=self.request.is_secure()):
            return redirect_url
        # Fallback to standard redirect URL if no valid 'next' is provided
        return super().get_redirect_url()

    def form_invalid(self, form):
        """
        Handle invalid form submissions, which typically means
        invalid credentials or missing fields.
        """
        # Log failed login attempts with some details, but be careful not to log sensitive info.
        username = form.cleaned_data.get('username', 'Unknown')
        logger.warning(f"Failed login attempt for username: {username}")

        # Add a friendly error message for the user
        messages.error(self.request, "Invalid username or password. Please try again.")
        return super().form_invalid(form)

    def dispatch(self, request, *args, **kwargs):
        """
        Handle any pre-processing before the form is displayed or processed.
        For example, if the user is already authenticated and tries to hit the login URL,
        redirect them away immediately to avoid confusion.
        """
        if request.user.is_authenticated:
            # User is already logged in, redirect them to the index view or 'next' if valid
            next_url = request.GET.get('next')
            if next_url and url_has_allowed_host_and_scheme(
                    url=next_url,
                    allowed_hosts={request.get_host()},
                    require_https=request.is_secure()):
                return HttpResponseRedirect(next_url)
            return HttpResponseRedirect(reverse_lazy('index'))
        return super().dispatch(request, *args, **kwargs)

    def form_valid(self, form):
        """
        If the form is valid, the user will be authenticated and logged in.
        We can add any additional checks or logging here.
        """
        username = form.cleaned_data.get('username', 'Unknown')
        logger.info(f"User '{username}' logged in successfully.")
        # Optionally add a success message
        messages.success(self.request, f"Welcome back, {username}!")
        return super().form_valid(form)

class CustomLogoutView(LogoutView):
    """
    Logs out the user and redirects them to LOGOUT_REDIRECT_URL or a safe 'next' URL if provided.
    Also provides user feedback and logs the event.

    Key Points:
    - Invalidate user's session.
    - Display a success message upon logout.
    - Log the logout event for auditing.
    - Ensure safe redirection using 'next' parameter checks.
    """

    # Use LOGOUT_REDIRECT_URL if defined; otherwise, fallback to 'login' page or another safe default.
    next_page = getattr(settings, 'LOGOUT_REDIRECT_URL', 'login')

    def dispatch(self, request, *args, **kwargs):
        """
        Overrides dispatch to log the event and add a success message after the logout.
        """
        username = request.user.get_username() if request.user.is_authenticated else 'Anonymous'

        response = super().dispatch(request, *args, **kwargs)

        # At this point, the user should be logged out.
        logger.info(f"User '{username}' logged out.")
        messages.success(request, "You have been successfully logged out. Thank you for visiting.")

        return response

    def get_next_page(self):
        """
        Determine the URL to redirect to after logout.
        If 'next' parameter is provided and valid, use it; otherwise, use next_page or a fallback.
        """
        # Check the 'next' param in GET.
        next_page = self.request.GET.get('next')
        if next_page and url_has_allowed_host_and_scheme(
            url=next_page,
            allowed_hosts={self.request.get_host()},
            require_https=self.request.is_secure()
        ):
            return next_page

        # If next_page is a named URL, try to resolve it. If it fails, assume it's a literal URL.
        if self.next_page:
            try:
                return reverse_lazy(self.next_page)
            except:
                # If next_page cannot be reversed, return it as-is (assuming it's a literal URL).
                return self.next_page

        # Fallback to the default logic of LogoutView if nothing else applies.
        return super().get_next_page()

class MostHighlightedContentView(LoginRequiredMixin, TemplateView):
    """
    Display a detail page showing all marked/highlighted content with pagination.
    """
    template_name = 'most_highlighted_content.html'
    login_url = settings.LOGIN_URL

    def get_context_data(self, **kwargs):
        """
        Add marked content data to the context with database-level pagination.
        """
        context = super().get_context_data(**kwargs)

        # Get search term
        search_term = self.request.GET.get('search', '')
        context['search_term'] = search_term

        # Get page number from request
        page = self.request.GET.get('page', 1)
        try:
            page = int(page)
        except ValueError:
            page = 1

        # Calculate offset and get paginated data directly from database
        page_size = 50
        offset = (page - 1) * page_size

        # Get total count for pagination
        total_count = MostMarkedContents.get_most_marked_contents_count(search=search_term or None)

        # Create a custom Page object
        paginator = Paginator(range(total_count), page_size)
        try:
            page_obj = paginator.page(page)
        except (PageNotAnInteger, EmptyPage):
            page_obj = paginator.page(1)

        # Get only the necessary records for this page
        contents = MostMarkedContents.get_most_marked_contents(
            limit=page_size,
            offset=offset,
            search=search_term or None
        )

        context['most_marked_contents'] = contents
        context['is_paginated'] = (total_count > page_size)
        context['page_obj'] = page_obj
        return context


class MostMemoedContentView(LoginRequiredMixin, TemplateView):
    """
    Display a detail page showing all content with memos with pagination.
    """
    template_name = 'most_memoed_content.html'
    login_url = settings.LOGIN_URL

    def get_context_data(self, **kwargs):
        """
        Add memoed content data to the context with database-level pagination.
        """
        context = super().get_context_data(**kwargs)

        # Get search term
        search_term = self.request.GET.get('search', '')
        context['search_term'] = search_term

        # Get page number from request
        page = self.request.GET.get('page', 1)
        try:
            page = int(page)
        except ValueError:
            page = 1

        # Calculate offset and get paginated data directly from database
        page_size = 50
        offset = (page - 1) * page_size

        # Get total count for pagination
        total_count = MostMemoContents.get_most_memo_contents_count(search=search_term or None)

        # Create a custom Page object
        paginator = Paginator(range(total_count), page_size)
        try:
            page_obj = paginator.page(page)
        except (PageNotAnInteger, EmptyPage):
            page_obj = paginator.page(1)

        # Get only the necessary records for this page
        contents = MostMemoContents.get_most_memo_contents(
            limit=page_size,
            offset=offset,
            search=search_term or None
        )

        context['most_memo_contents'] = contents
        context['is_paginated'] = (total_count > page_size)
        context['page_obj'] = page_obj
        return context


class MostActiveStudentsView(LoginRequiredMixin, TemplateView):
    """
    Display a detail page showing all active students with pagination.
    """
    template_name = 'most_active_students.html'
    login_url = settings.LOGIN_URL

    def get_context_data(self, **kwargs):
        """
        Add active students data to the context with database-level pagination.
        """
        context = super().get_context_data(**kwargs)

        # Get search term
        search_term = self.request.GET.get('search', '')
        context['search_term'] = search_term

        # Get page number from request
        page = self.request.GET.get('page', 1)
        try:
            page = int(page)
        except ValueError:
            page = 1

        # Calculate offset and get paginated data directly from database
        page_size = 50
        offset = (page - 1) * page_size

        # For students, we need a different approach to get total count with search
        # because search is applied after fetching from ClickHouse
        if not search_term:
            total_count = MostActiveStudents.get_most_active_students_count()
        else:
            # We need to get all students and filter them after getting Moodle data
            # This is less efficient but necessary for searching by student name
            all_students = MostActiveStudents.get_most_active_students_with_details(
                limit=None,
                offset=0,
                search=search_term
            )
            total_count = len(all_students)

            # If we have a small number of results, we can paginate in memory
            if total_count <= page_size * 2:
                # Calculate slice indices for the current page
                start_idx = (page - 1) * page_size
                end_idx = min(start_idx + page_size, total_count)

                # Get the slice for the current page
                students = all_students[start_idx:end_idx] if total_count > 0 else []

                # Create paginator and page object
                paginator = Paginator(range(total_count), page_size)
                try:
                    page_obj = paginator.page(page)
                except (PageNotAnInteger, EmptyPage):
                    page_obj = paginator.page(1)

                context['most_active_students'] = students
                context['is_paginated'] = (total_count > page_size)
                context['page_obj'] = page_obj
                return context

        # Create a custom Page object
        paginator = Paginator(range(total_count), page_size)
        try:
            page_obj = paginator.page(page)
        except (PageNotAnInteger, EmptyPage):
            page_obj = paginator.page(1)

        # Get only the necessary records for this page
        students = MostActiveStudents.get_most_active_students_with_details(
            limit=page_size if not search_term else None,  # When searching, we need all records
            offset=offset if not search_term else 0,
            search=search_term or None
        )

        # If searching, we need to manually paginate the results
        if search_term and len(students) > page_size:
            start_idx = (page - 1) * page_size
            end_idx = min(start_idx + page_size, len(students))
            students = students[start_idx:end_idx]

        context['most_active_students'] = students
        context['is_paginated'] = (total_count > page_size)
        context['page_obj'] = page_obj
        return context


class MostActiveContentsView(LoginRequiredMixin, TemplateView):
    """
    Display a detail page showing all active contents with pagination.
    """
    template_name = 'most_active_contents.html'
    login_url = settings.LOGIN_URL

    def get_context_data(self, **kwargs):
        """
        Add active contents data to the context with database-level pagination.
        """
        context = super().get_context_data(**kwargs)

        # Get search term
        search_term = self.request.GET.get('search', '')
        context['search_term'] = search_term

        # Get page number from request
        page = self.request.GET.get('page', 1)
        try:
            page = int(page)
        except ValueError:
            page = 1

        # Calculate offset and get paginated data directly from database
        page_size = 50
        offset = (page - 1) * page_size

        # Get total count for pagination
        total_count = MostActiveContents.get_most_active_contents_count(search=search_term or None)

        # Create a custom Page object
        paginator = Paginator(range(total_count), page_size)
        try:
            page_obj = paginator.page(page)
        except (PageNotAnInteger, EmptyPage):
            page_obj = paginator.page(1)

        # Get only the necessary records for this page
        contents = MostActiveContents.get_most_active_contents(
            limit=page_size,
            offset=offset,
            search=search_term or None
        )

        context['most_active_contents'] = contents
        context['is_paginated'] = (total_count > page_size)
        context['page_obj'] = page_obj
        return context

class CourseCategoriesView(LoginRequiredMixin, TemplateView):
    """
    Display a page showing all courses organized by their parent and child categories.
    """
    template_name = 'course_categories.html'
    login_url = settings.LOGIN_URL

    def get_context_data(self, **kwargs):
        """
        Add course categories hierarchy data to the context.
        """
        context = super().get_context_data(**kwargs)
        context['categories'] = CourseCategory.get_categories_with_courses()

        # Add Moodle LMS URL for course links
        context['LMS_URL'] = settings.LMS_URL if hasattr(settings, 'LMS_URL') else ''

        return context

class CourseDetailView(LoginRequiredMixin, TemplateView):
    """
    Display detailed information about a specific course.
    """
    template_name = 'course_detail.html'
    login_url = settings.LOGIN_URL

    def get_context_data(self, **kwargs):
        """
        Add course details and related data to the context.
        """
        context = super().get_context_data(**kwargs)
        course_id = self.kwargs.get('course_id')

        # Get course details using the model
        course = CourseDetail.get_course_details(course_id)
        if not course:
            context['course_exists'] = False
            return context

        context['course'] = course
        context['modules'] = CourseDetail.get_course_modules(course_id)
        context['enrolled_students'] = CourseDetail.get_enrolled_students_count(course_id)
        context['teachers'] = CourseDetail.get_course_teachers(course_id)

        # Get activity statistics
        stats, error = CourseDetail.get_course_activity_stats(course_id)
        print(stats)
        if error:
            context['clickhouse_error'] = True

        # Add stats to context
        if 'total_views' in stats:
            context['total_views'] = stats['total_views']
        if 'engagement' in stats:
            context['engagement'] = stats['engagement']
        if 'activity_timeline' in stats:
            context['activity_timeline'] = json.dumps(stats['activity_timeline'])
        if 'daily_activity_data' in stats:
            context['daily_activity_data'] = stats['daily_activity_data']

        # Add Moodle LMS URL for course link
        context['LMS_URL'] = settings.LMS_URL if hasattr(settings, 'LMS_URL') else ''
        context['course_exists'] = True

        return context


