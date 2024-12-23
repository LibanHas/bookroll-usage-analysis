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

from .models import MoodleUser, StudentCount, TotalCourses, TotalContents, ActiveStudents, MostActiveContents, DailyActiveUsers, DailyActivities, MostActiveStudents, MostMemoContents, MostMarkedContents

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
        context['courses_count'] = TotalCourses.get_course_count()
        context['contents_count'] = TotalContents.get_content_count()
        context['active_students'] = ActiveStudents.get_active_students()
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


