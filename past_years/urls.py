from django.urls import path
from datetime import datetime
from . import views

app_name = 'past_years'

# Generate dynamic URLs for years from 2021 to current year - 1
current_year = datetime.now().year
start_year = 2021
end_year = current_year - 1

# Ensure we have at least some years to work with
if end_year < start_year:
    end_year = start_year

urlpatterns = [
    # Add a general overview URL first
    path('', views.PastYearsOverviewView.as_view(), name='overview'),
]

# Add dynamic year URLs
for year in range(start_year, end_year + 1):
    urlpatterns.extend([
        path(f'{year}/', views.YearAnalysisView.as_view(), {'year': year}, name=f'year_{year}'),
        path(f'{year}/courses/', views.YearCoursesView.as_view(), {'year': year}, name=f'year_{year}_courses'),
        path(f'{year}/courses/clear-cache/', views.ClearCacheView.as_view(), {'year': year}, name=f'year_{year}_clear_cache'),
        path(f'{year}/students/', views.YearStudentsView.as_view(), {'year': year}, name=f'year_{year}_students'),
        path(f'{year}/students/course/<str:course_id>/distribution/', views.CourseGradeDistributionView.as_view(), {'year': year}, name=f'year_{year}_course_distribution'),
        path(f'{year}/teachers/', views.YearTeachersView.as_view(), {'year': year}, name=f'year_{year}_teachers'),
        path(f'{year}/analytics/', views.YearAnalyticsView.as_view(), {'year': year}, name=f'year_{year}_analytics'),
    ])
