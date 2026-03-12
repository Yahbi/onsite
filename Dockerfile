FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies (cached layer)
COPY backend/requirements.txt ./backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

# Copy entire application
COPY main.py ./
COPY backend/ ./backend/
COPY backend/static ./static

# Ensure runtime data directories exist
RUN mkdir -p backend/data backend/static

# Railway sets PORT env var — app reads it at runtime
ENV PORT=8080

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=30s \
    CMD curl -f http://localhost:${PORT}/api/health || exit 1

# Single worker on Railway to save memory — uvloop + httptools for speed
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT} --workers 1 --loop uvloop --http httptools"]
