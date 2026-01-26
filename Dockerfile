# Use Python 3.11 for better dependency compatibility
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies required for Python packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Make migration script executable
RUN chmod +x scripts/migrate_db.sh

# Expose port (Coolify will inject PORT env var)
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8000}/health || exit 1

# Start command - uses PORT from environment or defaults to 8000
# Using 4 workers for better concurrency and performance
CMD ["sh", "-c", "uvicorn app.main:socket_app --host 0.0.0.0 --port ${PORT:-8000} --workers 4"]
