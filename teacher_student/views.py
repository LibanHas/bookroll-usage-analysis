import datetime
import json
from django.views.generic import ListView, DetailView, TemplateView
from .models import Teacher, Student, StudentDetails
from django.http import Http404
from django.db import connections
from django.utils import timezone

class TeacherListView(ListView):
    model = Teacher
    template_name = 'teacher_list.html'
    context_object_name = 'teachers'

    def get_queryset(self):
        return Teacher.get_teacher_data()

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['additional_info'] = "This is additional context data"
        return context


class StudentListView(ListView):
    model = Student
    template_name = 'student_list.html'
    context_object_name = 'results'

    def get_queryset(self):
        return Student.get_student_data()

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['additional_info'] = "This is additional context data"
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
            raise Http404("Student not found.")
        return student_details

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        user_id = self.kwargs.get('user_id')
        context["activity_by_day"] = json.dumps(StudentDetails.get_student_activity_by_day(user_id), default=str)
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