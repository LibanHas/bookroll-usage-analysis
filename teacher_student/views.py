from django.views.generic import ListView, DetailView
from .models import Teacher, Student, StudentDetails
from django.http import Http404

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
        context['additional_info'] = "This is additional context data"
        return context
