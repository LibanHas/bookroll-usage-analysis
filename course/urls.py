from django.urls import path
from . import views

app_name = 'course'

urlpatterns = [
    # For future frontend implementation
    path('', views.CourseListView.as_view(), name='course_list'),
    path('<int:pk>/', views.CourseDetailView.as_view(), name='course_detail'),
]