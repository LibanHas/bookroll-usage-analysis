#!/usr/bin/env python
"""
Simple script to test logging configuration
"""
import os
import sys
import django
from pathlib import Path

# Add the project directory to Python path
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'leaf_school.settings')
django.setup()

import logging

def test_logging():
    """Test different loggers and levels"""

    # Test root logger
    root_logger = logging.getLogger()
    print(f"Root logger level: {root_logger.level}")
    print(f"Root logger handlers: {root_logger.handlers}")

    # Test past_years.models logger
    models_logger = logging.getLogger('past_years.models')
    print(f"Models logger level: {models_logger.level}")
    print(f"Models logger handlers: {models_logger.handlers}")
    print(f"Models logger effective level: {models_logger.getEffectiveLevel()}")

    # Test logging at different levels
    print("\n=== Testing logging levels ===")

    models_logger.debug("DEBUG: This is a debug message from past_years.models")
    models_logger.info("INFO: This is an info message from past_years.models")
    models_logger.warning("WARNING: This is a warning message from past_years.models")
    models_logger.error("ERROR: This is an error message from past_years.models")

    # Test the specific logger from models.py
    print("\n=== Testing logger from models.py ===")
    from past_years.models import logger as models_file_logger
    print(f"Models file logger name: {models_file_logger.name}")
    print(f"Models file logger level: {models_file_logger.level}")
    print(f"Models file logger effective level: {models_file_logger.getEffectiveLevel()}")

    models_file_logger.debug("DEBUG: Test message from models.py logger")
    models_file_logger.info("INFO: Test message from models.py logger")

    print("\n=== Test completed ===")

if __name__ == '__main__':
    test_logging()