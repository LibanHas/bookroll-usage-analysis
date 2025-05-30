#!/usr/bin/env python

import os
import sys
import django

# Setup Django
sys.path.append('/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leaf_school.settings')
django.setup()

from core.models import MostActiveStudents
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_time_calculation():
    print("Testing simplified time spent calculation...")

    try:
        result = MostActiveStudents.get_time_spent_distribution('last_3_months')

        print("\n=== RESULTS ===")
        print(f"Statistics: {result['statistics']}")
        print(f"Bins count: {len(result['bins'])}")
        print(f"Normal curve points: {len(result['normal_curve'])}")
        print(f"Distribution data points: {len(result['distribution_data'])}")

        if result['bins']:
            print(f"\nFirst few bins:")
            for i, bin_data in enumerate(result['bins'][:5]):
                print(f"  Bin {i+1}: {bin_data['bin_start']:.2f}-{bin_data['bin_end']:.2f}h, frequency: {bin_data['frequency']}")

        return True

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_time_calculation()