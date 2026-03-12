FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies first (cache layer)
COPY backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend application code
COPY backend/ .

# Copy root-level static files (main React SPA + admin)
COPY static/ /app/root_static/

# Ensure static directories exist
RUN mkdir -p static/css static/assets

# Copy the main SPA into backend static if not already present
# The 542KB React SPA at root static/ is the full app; backend has a simpler version
RUN if [ -f /app/root_static/index.html ]; then \
      cp /app/root_static/admin.html /app/static/admin.html 2>/dev/null || true; \
    fi

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=20s \
    CMD curl -f http://localhost:8000/api/health || exit 1

# Production: gunicorn with uvicorn workers for concurrency
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2", "--loop", "uvloop", "--http", "httptools"]
