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

# Playwright's own driver is a bundled Node binary, and it has to RUN before
# `playwright install --with-deps` can fetch anything. Newer drivers link
# libatomic, which the slim image does not carry: without it the step dies with
# "libatomic.so.1: cannot open shared object file" (exit 127), as the 13 Nov 2026
# deploy did. Installed here so a later playwright bump cannot fail the same way.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libatomic1 \
    && rm -rf /var/lib/apt/lists/*

# Install Chromium + the system libraries it needs (fonts, X libs, etc.).
RUN playwright install --with-deps chromium

COPY . .

# Railway injects $PORT; default to 8000 for local `docker run`.
ENV PORT=8000
CMD ["sh", "-c", "uvicorn api:app --host 0.0.0.0 --port ${PORT}"]
