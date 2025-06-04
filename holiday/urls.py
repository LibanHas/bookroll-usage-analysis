from django.urls import path
from .views import (
    HolidayListView,
    HolidayDetailView,
    HolidayAPIView,
    UpcomingHolidaysView,
)

app_name = 'holiday'

urlpatterns = [
    path('', HolidayListView.as_view(), name='holiday-list'),
    path('<int:pk>/', HolidayDetailView.as_view(), name='holiday-detail'),
    path('api/', HolidayAPIView.as_view(), name='holiday-api'),
    path('upcoming/', UpcomingHolidaysView.as_view(), name='upcoming-holidays'),
]