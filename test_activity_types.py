#!/usr/bin/env python
"""
Simple test script to verify the get_activity_types method works correctly
"""

import os
import sys
import django

# Add the project directory to the Python path
sys.path.append('/home/alma/workbench/leaf_school/leaf_school')

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leaf_school.settings')
django.setup()

from core.models import MostActiveContents

def test_activity_types():
    """Test the get_activity_types method"""
    try:
        print("Testing get_activity_types method...")
        activity_types = MostActiveContents.get_activity_types()

        print(f"Found {len(activity_types)} activity types:")
        for value, label in activity_types:
            print(f"  - Value: '{value}', Label: '{label}'")

        print("\nTest completed successfully!")
        return True

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return False

if __name__ == "__main__":
    test_activity_types()