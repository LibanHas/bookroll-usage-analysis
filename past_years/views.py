from django.shortcuts import render, redirect
from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.utils.translation import gettext_lazy as _
from django.contrib import messages
from django.http import JsonResponse
from django.views.decorators.http import require_POST
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.views import View
from datetime import datetime
from typing import Dict, Any
import json
import logging
from django.core.cache import cache
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt
from .models import PastYearCourseCategory, PastYearCourseActivity, PastYearStudentGrades, PastYearLogAnalytics, PastYearGradeAnalytics, clear_all_past_years_cache

logger = logging.getLogger(__name__)


class PastYearsOverviewView(LoginRequiredMixin, TemplateView):
    """Overview page showing all available past years."""
    template_name = 'past_years/overview.html'

    def get_context_data(self, **kwargs: Any) -> Dict[str, Any]:
        context = super().get_context_data(**kwargs)

        # Get available academic years from course categories
        available_years = PastYearCourseCategory.get_available_academic_years()

        # If no years found in categories, fall back to default range
        if not available_years:
            current_year = datetime.now().year
            start_year = 2019
            end_year = current_year - 1
            available_years = list(range(start_year, end_year + 1))
            available_years.reverse()
            logger.info(f"No years found in categories, using fallback years: {available_years}")

        # Get log analytics data
        try:
            # Get monthly log counts
            monthly_log_data = PastYearLogAnalytics.get_log_counts_by_period('month')

            # Get yearly log counts
            yearly_log_data = PastYearLogAnalytics.get_log_counts_by_period('year')

            # Get summary statistics
            log_summary = PastYearLogAnalytics.get_log_summary_stats()

            # Prepare chart data for JavaScript
            monthly_chart_data = json.dumps(monthly_log_data.get('data', []))
            yearly_chart_data = json.dumps(yearly_log_data.get('data', []))

        except Exception as e:
            logger.error(f"Error fetching log analytics: {str(e)}")
            monthly_log_data = {'data': [], 'total_logs': 0}
            yearly_log_data = {'data': [], 'total_logs': 0}
            log_summary = {'total_unique_logs': 0}
            monthly_chart_data = '[]'
            yearly_chart_data = '[]'

        # Get grade performance analytics data
        try:
            # Get yearly grade performance data only (academic year-based)
            yearly_grade_data = PastYearGradeAnalytics.get_grade_performance_by_period()

            # Get normal distribution grade performance data (new statistical approach)
            normal_distribution_data = PastYearGradeAnalytics.get_grade_performance_normal_distribution()

            # Get grade performance summary statistics
            grade_summary = PastYearGradeAnalytics.get_grade_performance_summary_stats()

            # Get time spent vs grade correlation data for available years
            correlation_data_by_year = {}
            for year in available_years[:5]:  # Limit to first 5 years for performance
                try:
                    correlation_data = PastYearGradeAnalytics.get_time_spent_vs_grade_correlation(year)
                    # Include data if it has correlation_data OR if it's demo data
                    if correlation_data.get('correlation_data') or correlation_data.get('metadata', {}).get('is_demo'):
                        correlation_data_by_year[year] = correlation_data
                        logger.info(f"Added correlation data for year {year}: {len(correlation_data.get('correlation_data', []))} data points, is_demo: {correlation_data.get('metadata', {}).get('is_demo', False)}")
                except Exception as e:
                    logger.warning(f"Could not get correlation data for year {year}: {str(e)}")

            # Prepare chart data for JavaScript (yearly only) with course transparency
            yearly_grade_chart_data = json.dumps({
                'top_25': yearly_grade_data.get('top_25_data', []),
                'bottom_25': yearly_grade_data.get('bottom_25_data', []),
                'course_transparency': {
                    'enabled': True,
                    'message': 'Course details available for each academic year'
                }
            })

            # Prepare normal distribution chart data for JavaScript
            normal_distribution_chart_data = json.dumps({
                'high_performers': normal_distribution_data.get('high_performers_data', []),
                'low_performers': normal_distribution_data.get('low_performers_data', []),
                'distribution_stats': normal_distribution_data.get('distribution_stats', []),
                'course_transparency': {
                    'enabled': True,
                    'message': 'Statistical analysis with course details available'
                }
            })

            # Prepare time spent vs grade correlation chart data for JavaScript
            time_grade_correlation_chart_data = json.dumps(correlation_data_by_year)

        except Exception as e:
            logger.error(f"Error fetching grade performance analytics: {str(e)}")
            yearly_grade_data = {'top_25_data': [], 'bottom_25_data': [], 'performance_summary': {}}
            normal_distribution_data = {'high_performers_data': [], 'low_performers_data': [], 'distribution_stats': [], 'performance_summary': {}}
            grade_summary = {'total_students_analyzed': 0, 'performance_metrics': {}}
            correlation_data_by_year = {}
            yearly_grade_chart_data = '{"top_25": [], "bottom_25": [], "course_transparency": {"enabled": false}}'
            normal_distribution_chart_data = '{"high_performers": [], "low_performers": [], "distribution_stats": [], "course_transparency": {"enabled": false}}'
            time_grade_correlation_chart_data = '{}'

        context.update({
            'available_years': available_years,
            'page_title': _('Past Years Analysis'),
            'page_description': _('Historical data analysis from previous academic years'),
            'monthly_log_data': monthly_log_data,
            'yearly_log_data': yearly_log_data,
            'log_summary': log_summary,
            'monthly_chart_data': monthly_chart_data,
            'yearly_chart_data': yearly_chart_data,
            'yearly_grade_chart_data': yearly_grade_chart_data,
            'normal_distribution_chart_data': normal_distribution_chart_data,
            'time_grade_correlation_chart_data': time_grade_correlation_chart_data,
            'yearly_grade_data': yearly_grade_data,
            'normal_distribution_data': normal_distribution_data,
            'correlation_data_by_year': correlation_data_by_year,
            'grade_summary': grade_summary,
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

        logger.info(f"Processing courses analysis request for academic year {year}")

        # Get courses data for the academic year
        courses_data = PastYearCourseCategory.get_courses_by_academic_year(year)
        total_courses = courses_data.get('total_courses', 0)

        logger.info(f"Found {total_courses} courses for academic year {year}")

        # Early exit if no courses found
        if total_courses == 0:
            logger.info(f"No courses found for academic year {year}, skipping activity analysis")
            context.update({
                'year': year,
                'page_title': _('Courses Analysis - {year}').format(year=year),
                'page_description': _('Course statistics and analysis for {year}').format(year=year),
                'breadcrumbs': [
                    {'name': _('Past Years'), 'url': 'past_years:overview'},
                    {'name': str(year), 'url': f'past_years:year_{year}'},
                    {'name': _('Courses'), 'url': None},
                ],
                'courses_data': courses_data,
                'enhanced_categories': {},
                'activity_data': {
                    'course_activities': [],
                    'overall_stats': {},
                    'daily_trends': [],
                    'top_operations': []
                },
                'engagement_patterns': {
                    'hourly_patterns': [],
                    'daily_patterns': [],
                    'monthly_trends': []
                },
                'daily_trends_json': '[]',
                'top_operations_json': '[]',
                'hourly_patterns_json': '[]',
                'monthly_trends_json': '[]',
                'has_data': False,
                'has_activity_data': False,
            })
            return context

        # Extract course IDs efficiently
        course_ids = []
        for category in courses_data.get('categories', {}).values():
            for child_category in category.get('children', {}).values():
                course_ids.extend([course['id'] for course in child_category.get('courses', [])])

        logger.info(f"Extracted {len(course_ids)} course IDs for activity analysis")

        # Get activity data for the courses
        activity_data = PastYearCourseActivity.get_course_activity_summary(year, course_ids)

        # Only get engagement patterns if we have activity data
        engagement_patterns = {}
        if activity_data.get('overall_stats', {}).get('total_activities', 0) > 0:
            engagement_patterns = PastYearCourseActivity.get_course_engagement_patterns(year)
        else:
            logger.info(f"No activity data found for academic year {year}, skipping engagement patterns")
            engagement_patterns = {
                'hourly_patterns': [],
                'daily_patterns': [],
                'monthly_trends': []
            }

        # Create activity mapping for efficient lookup
        activity_by_course = {}
        for activity in activity_data.get('course_activities', []):
            course_id = int(activity['course_id']) if activity['course_id'].isdigit() else activity['course_id']
            activity_by_course[course_id] = activity

        # Enhance courses data with activity information efficiently
        enhanced_categories = {}
        for category_id, category in courses_data.get('categories', {}).items():
            enhanced_category = {
                'id': category['id'],
                'name': category['name'],
                'academic_year': category['academic_year'],
                'course_count': category['course_count'],
                'children': {}
            }

            for child_id, child_category in category.get('children', {}).items():
                enhanced_child = {
                    'id': child_category['id'],
                    'name': child_category['name'],
                    'academic_year': child_category['academic_year'],
                    'course_count': child_category['course_count'],
                    'courses': []
                }

                for course in child_category.get('courses', []):
                    enhanced_course = course.copy()
                    enhanced_course['activity'] = activity_by_course.get(course['id'], {})
                    enhanced_child['courses'].append(enhanced_course)

                enhanced_category['children'][child_id] = enhanced_child

            enhanced_categories[category_id] = enhanced_category

        # Prepare chart data for templates
        daily_trends_json = json.dumps(activity_data.get('daily_trends', []))
        top_operations_json = json.dumps(activity_data.get('top_operations', []))
        hourly_patterns_json = json.dumps(engagement_patterns.get('hourly_patterns', []))
        monthly_trends_json = json.dumps(engagement_patterns.get('monthly_trends', []))

        has_data = total_courses > 0
        has_activity_data = len(activity_data.get('course_activities', [])) > 0

        logger.info(f"Courses analysis completed for year {year}: {total_courses} courses, {len(activity_data.get('course_activities', []))} with activity data")

        context.update({
            'year': year,
            'page_title': _('Courses Analysis - {year}').format(year=year),
            'page_description': _('Course statistics and analysis for {year}').format(year=year),
            'breadcrumbs': [
                {'name': _('Past Years'), 'url': 'past_years:overview'},
                {'name': str(year), 'url': f'past_years:year_{year}'},
                {'name': _('Courses'), 'url': None},
            ],
            'courses_data': courses_data,
            'enhanced_categories': enhanced_categories,
            'activity_data': activity_data,
            'engagement_patterns': engagement_patterns,
            'daily_trends_json': daily_trends_json,
            'top_operations_json': top_operations_json,
            'hourly_patterns_json': hourly_patterns_json,
            'monthly_trends_json': monthly_trends_json,
            'has_data': has_data,
            'has_activity_data': has_activity_data,
        })

        return context


class YearStudentsView(LoginRequiredMixin, TemplateView):
    """Students analysis for a specific year."""
    template_name = 'past_years/year_students.html'

    def get_context_data(self, **kwargs: Any) -> Dict[str, Any]:
        context = super().get_context_data(**kwargs)
        year = kwargs.get('year', datetime.now().year - 1)

        logger.info(f"Processing student analytics request for academic year {year}")

        # Check if we should show all activities or just activities for courses with grades
        show_all_activities = self.request.GET.get('show_all_activities', 'false').lower() == 'true'
        logger.debug(f"YEAR STUDENTS VIEW: show_all_activities = {show_all_activities}")

        # Generate cache keys based on year and activity filter setting
        cache_key_base = f'student_analytics_{year}'
        activity_filter_suffix = '_all_activities' if show_all_activities else '_graded_only'

        # Individual cache keys for different data sections
        main_analytics_cache_key = f'{cache_key_base}_main{activity_filter_suffix}'
        chart_data_cache_key = f'{cache_key_base}_charts{activity_filter_suffix}'
        engagement_cache_key = f'{cache_key_base}_engagement{activity_filter_suffix}'
        courses_context_cache_key = f'{cache_key_base}_courses_context'

        logger.debug(f"CACHE: Using cache keys - main: {main_analytics_cache_key}, charts: {chart_data_cache_key}")

        # Try to get cached data first
        cached_main_analytics = cache.get(main_analytics_cache_key)
        cached_chart_data = cache.get(chart_data_cache_key)
        cached_engagement_data = cache.get(engagement_cache_key)
        cached_courses_context = cache.get(courses_context_cache_key)

        # Check if we have all cached data
        if cached_main_analytics and cached_chart_data and cached_engagement_data and cached_courses_context:
            logger.info(f"CACHE HIT: Using cached student analytics for year {year} (show_all_activities={show_all_activities})")

            # Use cached data
            student_analytics = cached_main_analytics
            chart_data = cached_chart_data
            engagement_categories = cached_engagement_data
            courses_context = cached_courses_context

        else:
            logger.info(f"CACHE MISS: Generating fresh student analytics for year {year} (show_all_activities={show_all_activities})")

            # Get courses data for the academic year
            courses_data = PastYearCourseCategory.get_courses_by_academic_year(year)
            total_courses = courses_data.get('total_courses', 0)
            logger.debug(f"YEAR STUDENTS VIEW: Found {total_courses} courses for academic year {year}")

            # Extract course IDs for the academic year
            course_ids = []
            if total_courses > 0:
                for category in courses_data.get('categories', {}).values():
                    for child_category in category.get('children', {}).values():
                        course_ids.extend([str(course['id']) for course in child_category.get('courses', [])])

            logger.info(f"Found {len(course_ids)} courses for academic year {year} to analyze student data")

            if show_all_activities:
                logger.info(f"Showing ALL activities for academic year {year} (not filtered by course IDs)")
                # Get comprehensive student analytics for ALL courses in the academic year
                student_analytics = PastYearStudentGrades.get_student_analytics_for_year(year, None)
            else:
                logger.info(f"Showing activities only for courses with grades ({len(course_ids)} courses)")
                # Get comprehensive student analytics filtered by academic year courses with grades
                student_analytics = PastYearStudentGrades.get_student_analytics_for_year(year, course_ids)

            # Debug the student analytics result
            logger.debug(f"YEAR STUDENTS VIEW: Student analytics keys: {list(student_analytics.keys())}")

            summary_stats = student_analytics.get('summary_stats', {})
            grade_analytics = student_analytics.get('grade_analytics', {})
            access_analytics = student_analytics.get('access_analytics', {})
            combined_analytics = student_analytics.get('combined_analytics', {})

            # Prepare chart data for templates
            chart_data = {
                'grade_distribution_json': json.dumps(
                    grade_analytics.get('grade_distribution', [])
                ),
                'activity_types_json': json.dumps(
                    access_analytics.get('activity_types', [])
                ),
                'correlation_data_json': json.dumps(
                    combined_analytics.get('student_course_correlations', [])
                ),
                'top_activity_types_json': json.dumps(
                    combined_analytics.get('top_activity_types', [])
                )
            }

            # Prepare engagement categories for display
            engagement_categories = combined_analytics.get('engagement_categories', {})

            # Prepare courses context data
            courses_context = {
                'courses_data': courses_data,
                'total_courses_in_year': len(course_ids),
                'course_ids': course_ids
            }

            # Cache the data with different TTL based on data freshness needs
            # Main analytics: 2 hours (most expensive to generate)
            cache.set(main_analytics_cache_key, student_analytics, 7200)
            # Chart data: 2 hours
            cache.set(chart_data_cache_key, chart_data, 7200)
            # Engagement data: 2 hours
            cache.set(engagement_cache_key, engagement_categories, 7200)
            # Courses context: 1 hour (less expensive to regenerate)
            cache.set(courses_context_cache_key, courses_context, 3600)

            logger.info(f"CACHE SET: Cached student analytics data for year {year} (show_all_activities={show_all_activities})")

        # Extract data from cached or fresh results
        summary_stats = student_analytics.get('summary_stats', {})
        grade_analytics = student_analytics.get('grade_analytics', {})
        access_analytics = student_analytics.get('access_analytics', {})
        combined_analytics = student_analytics.get('combined_analytics', {})

        # Check if we have data
        has_data = bool(
            grade_analytics.get('overall_stats', {}).get('total_students', 0) > 0 or
            access_analytics.get('student_access', [])
        )
        logger.debug(f"YEAR STUDENTS VIEW: has_data = {has_data}")

        # Log the key metrics that will be displayed
        logger.debug(f"YEAR STUDENTS VIEW: Key metrics for template:")
        logger.debug(f"  - total_students_with_grades: {summary_stats.get('total_students_with_grades', 0)}")
        logger.debug(f"  - total_courses_with_grades: {summary_stats.get('total_courses_with_grades', 0)}")
        logger.debug(f"  - total_activities: {summary_stats.get('total_activities', 0)}")
        logger.debug(f"  - overall_avg_grade: {summary_stats.get('overall_avg_grade', 0)}")

        context.update({
            'year': year,
            'page_title': _('Students Analysis - {year}').format(year=year),
            'page_description': _('Student activity and performance analysis for {year}').format(year=year),
            'breadcrumbs': [
                {'name': _('Past Years'), 'url': 'past_years:overview'},
                {'name': str(year), 'url': f'past_years:year_{year}'},
                {'name': _('Students'), 'url': None},
            ],
            'student_analytics': student_analytics,
            'courses_data': courses_context['courses_data'],
            'has_data': has_data,
            'show_all_activities': show_all_activities,
            'total_courses_in_year': courses_context['total_courses_in_year'],
            'grade_distribution_json': chart_data['grade_distribution_json'],
            'activity_types_json': chart_data['activity_types_json'],
            'correlation_data_json': chart_data['correlation_data_json'],
            'top_activity_types_json': chart_data['top_activity_types_json'],
            'engagement_categories': engagement_categories,
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


class ClearCacheView(LoginRequiredMixin, View):
    """View to clear all past years related cache"""

    def post(self, request):
        """Handle cache clearing request"""
        try:
            logger.info(f"Cache clear requested by user: {request.user.username}")

            # Clear all past years cache
            result = clear_all_past_years_cache()

            if result['success']:
                logger.info(f"Cache cleared successfully: {result['message']}")
                return JsonResponse({
                    'success': True,
                    'message': result['message'],
                    'details': {
                        'method': result['method'],
                        'keys_cleared': result['keys_cleared'],
                        'patterns_cleared': result.get('patterns_cleared', [])
                    }
                })
            else:
                logger.error(f"Cache clear failed: {result['message']}")
                return JsonResponse({
                    'success': False,
                    'message': result['message'],
                    'error': result.get('original_error', 'Unknown error')
                }, status=500)

        except Exception as e:
            logger.error(f"Unexpected error during cache clear: {str(e)}")
            return JsonResponse({
                'success': False,
                'message': 'An unexpected error occurred while clearing cache',
                'error': str(e)
            }, status=500)

    def get(self, request):
        """Return cache status information"""
        try:
            from django.core.cache import cache
            from django.core.cache.backends.redis import RedisCache

            cache_info = {
                'cache_backend': type(cache).__name__,
                'is_redis': isinstance(cache, RedisCache),
                'cache_location': getattr(cache, '_cache', {}).get('_server', 'Unknown') if hasattr(cache, '_cache') else 'Unknown'
            }

            return JsonResponse({
                'success': True,
                'cache_info': cache_info
            })

        except Exception as e:
            return JsonResponse({
                'success': False,
                'message': 'Could not retrieve cache information',
                'error': str(e)
            }, status=500)


class CourseGradeDistributionView(LoginRequiredMixin, View):
    """AJAX view to get grade distribution data for a specific course."""

    def get(self, request, year, course_id):
        """Return grade distribution data for a course in JSON format."""
        try:
            # Create cache key for this specific course distribution
            cache_key = f'course_grade_distribution_{year}_{course_id}'

            # Try to get cached data first
            cached_distribution = cache.get(cache_key)

            if cached_distribution:
                logger.info(f"CACHE HIT: Using cached grade distribution for course {course_id} in year {year}")
                return JsonResponse({
                    'success': True,
                    'data': cached_distribution,
                    'cached': True
                })

            logger.info(f"CACHE MISS: Generating fresh grade distribution for course {course_id} in year {year}")

            # Get the distribution data
            distribution_data = PastYearCourseCategory.get_course_grade_distribution(
                course_id=course_id,
                academic_year=year
            )

            if 'error' in distribution_data:
                return JsonResponse({
                    'success': False,
                    'error': distribution_data['error']
                }, status=404)

            # Cache the distribution data for 1 hour
            # Course-specific distributions are less expensive to regenerate
            cache.set(cache_key, distribution_data, 3600)

            logger.info(f"CACHE SET: Cached grade distribution for course {course_id} in year {year}")

            return JsonResponse({
                'success': True,
                'data': distribution_data,
                'cached': False
            })

        except Exception as e:
            logger.error(f"Error fetching distribution for course {course_id}: {str(e)}")
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=500)
