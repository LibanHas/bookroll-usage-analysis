#!/bin/bash

# Create necessary directories if they don't exist
mkdir -p /app/logs /app/static /app/staticfiles /app/media

# Set proper permissions for mounted volumes
chown -R www-data:www-data /app/logs /app/static /app/staticfiles /app/media
chmod -R 755 /app/logs /app/static /app/staticfiles /app/media

# Create and set permissions for log file
touch /app/logs/django.log
chown www-data:www-data /app/logs/django.log
chmod 644 /app/logs/django.log

# Collect static files
python manage.py collectstatic --noinput

# Start Daphne
exec daphne -b 0.0.0.0 -p 8000 leaf_school.asgi:application