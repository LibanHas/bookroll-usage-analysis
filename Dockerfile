# Use a lightweight Python base image
FROM python:3.10-slim

# Set environment variables to ensure non-interactive installs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system dependencies required for database drivers
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    pkg-config \
    default-libmysqlclient-dev \
    libssl-dev \
    libffi-dev \
    wget \
    curl \    
    && rm -rf /var/lib/apt/lists/*

# Install pip and ensure it's the latest version
RUN python -m pip install --upgrade pip setuptools wheel

# Copy requirements file and install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code to the container
COPY . .

# Create a non-root user for security
RUN adduser --disabled-password appuser
USER appuser

# Expose the application port
EXPOSE 8000

# Set the entry point for the container
# CMD ["gunicorn", "--bind", "0.0.0.0:8000", "your_project.wsgi:application"]
# CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "3", "--threads", "2", "your_project.wsgi:application"]


CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
