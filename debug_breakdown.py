#!/usr/bin/env python
"""
Debug script to test the activity breakdown functionality
"""

import os
import sys
import django

# Add the project directory to the Python path
sys.path.append('/home/alma/workbench/leaf_school')

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leaf_school.settings')
django.setup()

from core.models import MostActiveContents

def test_breakdown():
    """Test the get_most_active_contents_with_breakdown method"""
    try:
        print("Testing get_most_active_contents_with_breakdown method...")
        contents = MostActiveContents.get_most_active_contents_with_breakdown(limit=5)

        print(f"Found {len(contents)} contents:")
        for i, content in enumerate(contents, 1):
            print(f"\n{i}. Content: {content['contents_name']}")
            print(f"   Total Activities: {content['total_activities']}")
            print(f"   Breakdown Total: {content.get('breakdown_total', 'N/A')}")
            print(f"   Activity Breakdown:")

            breakdown = content.get('activity_breakdown', {})
            if breakdown:
                for activity_type, count in breakdown.items():
                    percentage = (count / content['total_activities']) * 100 if content['total_activities'] > 0 else 0
                    print(f"     - {activity_type}: {count} ({percentage:.1f}%)")
            else:
                print("     No breakdown data available")

        print("\nTest completed successfully!")
        return True

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_breakdown()