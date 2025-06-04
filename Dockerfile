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
    npm \
    gettext \
    && rm -rf /var/lib/apt/lists/*

# Install pip and ensure it's the latest version
RUN python -m pip install --upgrade pip setuptools wheel

# Copy requirements file and install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
RUN npm install -g wscat

# Copy application code to the container
COPY . .

# Create necessary directories for development
RUN mkdir -p /app/logs /app/media /app/staticfiles

# For development, we'll run as root to avoid permission issues with mounted volumes
# In production, use a non-root user for security

# Expose the application port
EXPOSE 8080

# Default command - can be overridden by docker-compose
CMD ["python", "manage.py", "runserver", "0.0.0.0:8080"]
