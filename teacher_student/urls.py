from django.urls import path
from . import views

urlpatterns = [
    path('teachers/', views.TeacherListView.as_view(), name='teacher_list'),
    path('teachers/<int:user_id>/', views.TeacherDetailView.as_view(), name='teacher_detail'),
    path('students/', views.StudentListView.as_view(), name='student_list'),
    path('students/<int:user_id>/', views.StudentDetailsView.as_view(), name='student_detail'),
    path('students/live/<int:user_id>/', views.StudentActivityLiveView.as_view(), name='student_activity_live'),
    path('image-proxy/', views.ImageProxyView.as_view(), name='image_proxy'),
]
