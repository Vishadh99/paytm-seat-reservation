FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY docker-entrypoint.sh ./

RUN useradd --create-home appuser
USER appuser

EXPOSE 8080
# WEB_CONCURRENCY = worker processes (~1 per vCPU). Correctness never depends on it:
# every decision is made inside Postgres, so N workers or N replicas behave the same.
CMD ["./docker-entrypoint.sh"]
