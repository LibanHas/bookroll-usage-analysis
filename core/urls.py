from django.urls import path
from . import views

urlpatterns = [
    path('', views.IndexView.as_view(), name='index'),
    path('login/', views.CustomLoginView.as_view(), name='login'),
    path('logout/', views.LogoutView.as_view(), name='logout'),
    path('most-highlighted-content/', views.MostHighlightedContentView.as_view(), name='most_highlighted_content'),
    path('most-memoed-content/', views.MostMemoedContentView.as_view(), name='most_memoed_content'),
    path('most-active-students/', views.MostActiveStudentsView.as_view(), name='most_active_students'),
    path('most-active-contents/', views.MostActiveContentsView.as_view(), name='most_active_contents'),
    path('course-categories/', views.CourseCategoriesView.as_view(), name='course_categories'),
    path('course/<int:course_id>/', views.CourseDetailView.as_view(), name='course_detail'),
]
