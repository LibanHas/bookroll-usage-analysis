from django.urls import path
from datetime import datetime
from . import views

app_name = 'past_years'

# Generate dynamic URLs for years from 2021 to current year - 1
current_year = datetime.now().year
start_year = 2019
end_year = current_year - 1

# Ensure we have at least some years to work with
if end_year < start_year:
    end_year = start_year

urlpatterns = [
    # Add a general overview URL first
    path('', views.PastYearsOverviewView.as_view(), name='overview'),

    # Cache management
    path('clear-cache/', views.ClearCacheView.as_view(), name='clear_cache'),

    # Academic year specific pages (generic patterns)
    path('<int:year>/', views.YearAnalysisView.as_view(), name='year_detail'),
    path('<int:year>/courses/', views.YearCoursesView.as_view(), name='year_courses'),
    path('<int:year>/students/', views.YearStudentsView.as_view(), name='year_students'),
    path('<int:year>/teachers/', views.YearTeachersView.as_view(), name='year_teachers'),
    path('<int:year>/analytics/', views.YearAnalyticsView.as_view(), name='year_analytics'),

    # Cache clearing for specific years (legacy)
    path('<int:year>/clear-cache/', views.ClearCacheView.as_view(), name='clear_cache_year'),
]

# Add dynamic year URLs for backward compatibility
for year in range(start_year, end_year + 1):
    urlpatterns.extend([
        path(f'{year}/', views.YearAnalysisView.as_view(), {'year': year}, name=f'year_{year}'),
        path(f'{year}/courses/', views.YearCoursesView.as_view(), {'year': year}, name=f'year_{year}_courses'),
        path(f'{year}/students/', views.YearStudentsView.as_view(), {'year': year}, name=f'year_{year}_students'),
        path(f'{year}/teachers/', views.YearTeachersView.as_view(), {'year': year}, name=f'year_{year}_teachers'),
        path(f'{year}/analytics/', views.YearAnalyticsView.as_view(), {'year': year}, name=f'year_{year}_analytics'),
        path(f'{year}/courses/clear-cache/', views.ClearCacheView.as_view(), {'year': year}, name=f'year_{year}_clear_cache'),
        path(f'{year}/students/course/<str:course_id>/distribution/', views.CourseGradeDistributionView.as_view(), {'year': year}, name=f'year_{year}_course_distribution'),
    ])
