#!/usr/bin/env python
import os
import django
import json

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leaf_school.settings')
django.setup()

from past_years.models import PastYearGradeAnalytics

print('=== TESTING CORRELATION FUNCTIONALITY ===')

# Test the correlation method for year 2024
year = 2024
print(f'\nTesting correlation for year {year}...')

try:
    result = PastYearGradeAnalytics.get_time_spent_vs_grade_correlation(year)

    print(f'Success: {not result.get("error")}')
    print(f'Error: {result.get("error")}')
    print(f'Data points: {len(result.get("correlation_data", []))}')
    print(f'Method: {result.get("metadata", {}).get("method")}')
    print(f'Is demo: {result.get("metadata", {}).get("is_demo")}')

    stats = result.get("statistics", {})
    if stats:
        print(f'Correlation coefficient: {stats.get("correlation_coefficient", 0):.3f}')
        print(f'Average grade: {stats.get("average_grade", 0):.2f}')
        print(f'Average hours: {stats.get("average_hours", 0):.2f}')
        print(f'Sample size: {stats.get("sample_size", 0)}')

    # Show sample data
    sample_data = result.get("correlation_data", [])[:3]
    if sample_data:
        print(f'\nSample data:')
        for i, data in enumerate(sample_data):
            print(f'  {i+1}: student={data["student_id"]}, grade={data["average_grade"]:.2f}, minutes={data["total_time_spent_minutes"]:.2f}')

except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()

print('\n=== TEST COMPLETE ===')