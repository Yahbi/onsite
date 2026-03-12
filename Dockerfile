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

# Default — Railway overrides PORT at runtime
ENV PORT=8080

# No HEALTHCHECK in Dockerfile — Railway handles it via railway.toml

CMD ["python3", "main.py"]
