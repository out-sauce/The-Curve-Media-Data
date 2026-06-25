# Research-stage browser scraper needs a real Chromium, so we build a Docker image
# (instead of Railway's default buildpack) that bundles Chromium + its OS libraries.
# Python 3.11+ base satisfies Playwright's requirement.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

# Install Python deps first for layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Chromium + the system libraries it needs (fonts, X libs, etc.).
RUN playwright install --with-deps chromium

COPY . .

# Railway injects $PORT; default to 8000 for local `docker run`.
ENV PORT=8000
CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port ${PORT}"]
