from django.shortcuts import render
from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.utils.translation import gettext_lazy as _
from datetime import datetime
from typing import Dict, Any


class PastYearsOverviewView(LoginRequiredMixin, TemplateView):
    """Overview page showing all available past years."""
    template_name = 'past_years/overview.html'

    def get_context_data(self, **kwargs: Any) -> Dict[str, Any]:
        context = super().get_context_data(**kwargs)
        current_year = datetime.now().year
        start_year = 2021
        end_year = current_year - 1

        # Generate list of available years
        available_years = list(range(start_year, end_year + 1))
        available_years.reverse()  # Show most recent years first

        context.update({
            'available_years': available_years,
            'page_title': _('Past Years Analysis'),
            'page_description': _('Historical data analysis from previous academic years'),
        })
        return context


class YearAnalysisView(LoginRequiredMixin, TemplateView):
    """Main analysis page for a specific year."""
    template_name = 'past_years/year_analysis.html'

    def get_context_data(self, **kwargs: Any) -> Dict[str, Any]:
        context = super().get_context_data(**kwargs)
        year = kwargs.get('year', datetime.now().year - 1)

        context.update({
            'year': year,
            'page_title': _('Analysis for {year}').format(year=year),
            'page_description': _('Comprehensive analysis and statistics for the year {year}').format(year=year),
            'breadcrumbs': [
                {'name': _('Past Years'), 'url': 'past_years:overview'},
                {'name': str(year), 'url': None},
            ],
        })
        return context


class YearCoursesView(LoginRequiredMixin, TemplateView):
    """Courses analysis for a specific year."""
    template_name = 'past_years/year_courses.html'

    def get_context_data(self, **kwargs: Any) -> Dict[str, Any]:
        context = super().get_context_data(**kwargs)
        year = kwargs.get('year', datetime.now().year - 1)

        context.update({
            'year': year,
            'page_title': _('Courses Analysis - {year}').format(year=year),
            'page_description': _('Course statistics and analysis for {year}').format(year=year),
            'breadcrumbs': [
                {'name': _('Past Years'), 'url': 'past_years:overview'},
                {'name': str(year), 'url': f'past_years:year_{year}'},
                {'name': _('Courses'), 'url': None},
            ],
        })
        return context


class YearStudentsView(LoginRequiredMixin, TemplateView):
    """Students analysis for a specific year."""
    template_name = 'past_years/year_students.html'

    def get_context_data(self, **kwargs: Any) -> Dict[str, Any]:
        context = super().get_context_data(**kwargs)
        year = kwargs.get('year', datetime.now().year - 1)

        context.update({
            'year': year,
            'page_title': _('Students Analysis - {year}').format(year=year),
            'page_description': _('Student activity and performance analysis for {year}').format(year=year),
            'breadcrumbs': [
                {'name': _('Past Years'), 'url': 'past_years:overview'},
                {'name': str(year), 'url': f'past_years:year_{year}'},
                {'name': _('Students'), 'url': None},
            ],
        })
        return context


class YearTeachersView(LoginRequiredMixin, TemplateView):
    """Teachers analysis for a specific year."""
    template_name = 'past_years/year_teachers.html'

    def get_context_data(self, **kwargs: Any) -> Dict[str, Any]:
        context = super().get_context_data(**kwargs)
        year = kwargs.get('year', datetime.now().year - 1)

        context.update({
            'year': year,
            'page_title': _('Teachers Analysis - {year}').format(year=year),
            'page_description': _('Teacher activity and course management analysis for {year}').format(year=year),
            'breadcrumbs': [
                {'name': _('Past Years'), 'url': 'past_years:overview'},
                {'name': str(year), 'url': f'past_years:year_{year}'},
                {'name': _('Teachers'), 'url': None},
            ],
        })
        return context


class YearAnalyticsView(LoginRequiredMixin, TemplateView):
    """Advanced analytics for a specific year."""
    template_name = 'past_years/year_analytics.html'

    def get_context_data(self, **kwargs: Any) -> Dict[str, Any]:
        context = super().get_context_data(**kwargs)
        year = kwargs.get('year', datetime.now().year - 1)

        context.update({
            'year': year,
            'page_title': _('Advanced Analytics - {year}').format(year=year),
            'page_description': _('Detailed analytics and insights for {year}').format(year=year),
            'breadcrumbs': [
                {'name': _('Past Years'), 'url': 'past_years:overview'},
                {'name': str(year), 'url': f'past_years:year_{year}'},
                {'name': _('Analytics'), 'url': None},
            ],
        })
        return context
