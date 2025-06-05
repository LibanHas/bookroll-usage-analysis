#!/usr/bin/env python3
"""
Debug script for engagement vs grade analysis
Run with: python debug_engagement_vs_grade.py
"""

import os
import sys
import django

# Add the project directory to Python path
sys.path.append('/home/alma/workbench/leaf_school/leaf_school')

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leaf_school.settings')
django.setup()

# Now import Django models and functions
from past_years.models import PastYearCourseCategory
from past_years.analytics import (
    get_engagement_vs_grade_performance,
    _calculate_engagement_vs_grade_for_year,
    _get_engagement_data_for_students,
    _get_grade_data_for_students,
    _combine_engagement_and_grade_data
)

def debug_engagement_vs_grade():
    """Debug the engagement vs grade analysis step by step"""
    print("🔍 DEBUGGING ENGAGEMENT VS GRADE ANALYSIS")
    print("=" * 50)

    # Test years to check
    test_years = [2019, 2020, 2021, 2022, 2023, 2024]

    for year in test_years:
        print(f"\n📅 TESTING ACADEMIC YEAR {year}")
        print("-" * 30)

        # STEP 1: Check if students are found for this year
        try:
            student_user_ids = PastYearCourseCategory.get_student_user_ids_for_academic_year(year)
            print(f"✅ Step 1 - Students found: {len(student_user_ids)} students")
            if len(student_user_ids) == 0:
                print(f"❌ No students found for academic year {year} - SKIPPING")
                continue
            # Show sample student IDs
            print(f"   Sample student IDs: {student_user_ids[:5]}")
        except Exception as e:
            print(f"❌ Step 1 - Error getting students: {str(e)}")
            continue

        # STEP 2: Check engagement data from ClickHouse
        try:
            engagement_data = _get_engagement_data_for_students(year, student_user_ids)
            print(f"✅ Step 2 - Engagement data found: {len(engagement_data)} students with engagement")
            if len(engagement_data) == 0:
                print(f"❌ No engagement data found for academic year {year}")
                # Show sample data structure
                print(f"   Students checked: {len(student_user_ids)}")
            else:
                # Show sample engagement data
                sample_student = list(engagement_data.keys())[0]
                sample_data = engagement_data[sample_student]
                print(f"   Sample engagement data: {sample_student} -> {sample_data}")
        except Exception as e:
            print(f"❌ Step 2 - Error getting engagement data: {str(e)}")
            continue

        # STEP 3: Check grade data from analysis_db (with Benesse filter)
        try:
            grade_data = _get_grade_data_for_students(year, student_user_ids)
            print(f"✅ Step 3 - Grade data found: {len(grade_data)} students with Benesse grades")
            if len(grade_data) == 0:
                print(f"❌ No Benesse grade data found for academic year {year}")

                # Let's check if there are ANY grades (without Benesse filter)
                print(f"   Checking if ANY grades exist (without Benesse filter)...")
                # We need to create a temporary function to check without Benesse filter

            else:
                # Show sample grade data
                sample_student = list(grade_data.keys())[0]
                sample_data = grade_data[sample_student]
                print(f"   Sample grade data: {sample_student} -> {sample_data}")
        except Exception as e:
            print(f"❌ Step 3 - Error getting grade data: {str(e)}")
            continue

        # STEP 4: Check combined data
        if engagement_data and grade_data:
            try:
                combined_data = _combine_engagement_and_grade_data(engagement_data, grade_data)
                print(f"✅ Step 4 - Combined data: {len(combined_data)} students with both engagement and grades")

                if len(combined_data) < 10:
                    print(f"❌ Not enough combined data for analysis (need 10+, got {len(combined_data)})")
                else:
                    print(f"✅ Sufficient data for analysis: {len(combined_data)} students")
                    # Show sample combined data
                    if combined_data:
                        sample = combined_data[0]
                        print(f"   Sample combined: {sample['student_id']} - Grade: {sample['average_grade']}, Activities: {sample['total_activities']}")

            except Exception as e:
                print(f"❌ Step 4 - Error combining data: {str(e)}")
        else:
            print(f"❌ Step 4 - Cannot combine data (engagement: {len(engagement_data)}, grades: {len(grade_data)})")

        print()

    print("\n🔍 TESTING FULL FUNCTION")
    print("-" * 30)

    # Test the full function
    try:
        result = get_engagement_vs_grade_performance(start_year=2019, end_year=2024)
        print(f"✅ Full function result:")
        print(f"   Yearly data entries: {len(result.get('yearly_data', []))}")
        print(f"   Summary stats: {result.get('summary_stats', {})}")

        if result.get('yearly_data'):
            print(f"   Sample yearly data: {result['yearly_data'][0]}")
        else:
            print(f"❌ No yearly data returned")

    except Exception as e:
        print(f"❌ Full function error: {str(e)}")

def debug_benesse_filter():
    """Debug the Benesse grade filter specifically"""
    print("\n🔍 DEBUGGING BENESSE FILTER")
    print("=" * 30)

    from django.db import connections

    try:
        with connections['analysis_db'].cursor() as cursor:
            # Check total grades
            cursor.execute("SELECT COUNT(*) FROM course_student_scores WHERE quiz IS NOT NULL AND quiz >= 0 AND quiz <= 100")
            total_grades = cursor.fetchone()[0]
            print(f"Total valid grades in database: {total_grades}")

            # Check Benesse grades
            cursor.execute("SELECT COUNT(*) FROM course_student_scores WHERE quiz IS NOT NULL AND quiz >= 0 AND quiz <= 100 AND (name LIKE '%Benesse%' OR name LIKE '%ベネッセ%')")
            benesse_grades = cursor.fetchone()[0]
            print(f"Benesse grades in database: {benesse_grades}")

            # Check grades by year
            for year in [2019, 2020, 2021, 2022, 2023, 2024]:
                cursor.execute(f"SELECT COUNT(*) FROM course_student_scores WHERE quiz IS NOT NULL AND quiz >= 0 AND quiz <= 100 AND course_name LIKE '%{year}年度%'")
                year_grades = cursor.fetchone()[0]

                cursor.execute(f"SELECT COUNT(*) FROM course_student_scores WHERE quiz IS NOT NULL AND quiz >= 0 AND quiz <= 100 AND course_name LIKE '%{year}年度%' AND (name LIKE '%Benesse%' OR name LIKE '%ベネッセ%')")
                year_benesse_grades = cursor.fetchone()[0]

                print(f"Year {year}: {year_grades} total grades, {year_benesse_grades} Benesse grades")

            # Sample some name values to see what we have
            print(f"\nSample grade names:")
            cursor.execute("SELECT DISTINCT name FROM course_student_scores WHERE name IS NOT NULL LIMIT 10")
            names = cursor.fetchall()
            for name in names:
                print(f"   - {name[0]}")

    except Exception as e:
        print(f"❌ Database error: {str(e)}")

if __name__ == "__main__":
    debug_engagement_vs_grade()
    debug_benesse_filter()