from django.views.generic import ListView, DetailView
from django.utils.translation import gettext_lazy as _
from django.http import JsonResponse
from django.core.paginator import Paginator
from datetime import datetime, date
from typing import Any, Dict
from .models import JapaneseHoliday


class HolidayListView(ListView):
    """List view for Japanese holidays."""

    model = JapaneseHoliday
    template_name = 'holiday/holiday_list.html'
    context_object_name = 'holidays'
    paginate_by = 50

    def get_queryset(self):
        """Get filtered queryset based on year parameter."""
        queryset = super().get_queryset()

        year = self.request.GET.get('year')
        if year:
            try:
                year = int(year)
                queryset = queryset.filter(year=year)
            except ValueError:
                pass

        return queryset.order_by('date')

    def get_context_data(self, **kwargs) -> Dict[str, Any]:
        """Add additional context data."""
        context = super().get_context_data(**kwargs)

        # Get available years
        context['available_years'] = (
            JapaneseHoliday.objects
            .values_list('year', flat=True)
            .distinct()
            .order_by('year')
        )

        # Current year filter
        context['current_year'] = self.request.GET.get('year')

        return context


class HolidayDetailView(DetailView):
    """Detail view for a specific holiday."""

    model = JapaneseHoliday
    template_name = 'holiday/holiday_detail.html'
    context_object_name = 'holiday'


class HolidayAPIView(ListView):
    """API view to return holidays as JSON."""

    model = JapaneseHoliday

    def get(self, request, *args, **kwargs):
        """Return holidays as JSON response."""
        year = request.GET.get('year')

        queryset = self.get_queryset()
        if year:
            try:
                year = int(year)
                queryset = queryset.filter(year=year)
            except ValueError:
                return JsonResponse({'error': 'Invalid year parameter'}, status=400)

        holidays_data = {}
        for holiday in queryset:
            holidays_data[holiday.date.strftime('%Y-%m-%d')] = {
                'name': holiday.name,
                'name_en': holiday.name_en,
                'year': holiday.year,
            }

        return JsonResponse(holidays_data)


class UpcomingHolidaysView(ListView):
    """View for upcoming holidays."""

    model = JapaneseHoliday
    template_name = 'holiday/upcoming_holidays.html'
    context_object_name = 'holidays'

    def get_queryset(self):
        """Get upcoming holidays."""
        return JapaneseHoliday.get_upcoming_holidays(limit=10)