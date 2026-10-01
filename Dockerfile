FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN useradd --create-home appuser
USER appuser

EXPOSE 8080
# Single worker on purpose: asyncio handles the concurrency, and a single process
# keeps Prometheus counters exact (no multiprocess aggregation to get wrong).
# Large backlog so a connection stampede queues in the kernel instead of being refused.
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT} --backlog 4096 --timeout-keep-alive 30 --no-access-log"]
