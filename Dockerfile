FROM python:3.9-slim

# Install system dependencies (if needed by watchdog)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY simple_sync_v5.py .

# Create directories for mounts
RUN mkdir -p /config /cache /output /inputs

# Set up non-root user for security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app /cache
USER appuser

# Default command (can be overridden)
ENTRYPOINT ["python", "simple_sync_v5.py"]
CMD ["--config", "/config/config.yaml"]