from django.urls import path
from . import views

urlpatterns = [
    path('teachers/', views.TeacherListView.as_view(), name='teacher_list'),
    path('students/', views.StudentListView.as_view(), name='student_list'),
    path('students/<int:user_id>/', views.StudentDetailsView.as_view(), name='student_detail'),
]
