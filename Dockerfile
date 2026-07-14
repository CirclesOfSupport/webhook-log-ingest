# Cloud Run JOB (not a service).
#
# No Flask, no gunicorn, no $PORT, no /health. A job runs to completion and exits;
# it is never sent an HTTP request. The service runbook (cloud_run_service_deploy.md)
# describes the SERVICE shape -- do not copy its gunicorn ENTRYPOINT here. Cloud Run
# will happily deploy a job that never binds a port; a service will not.
FROM python:3.12-slim

# PYTHONUNBUFFERED IS LOAD-BEARING, NOT HYGIENE.
#
# Without it Python buffers stdout, and NOTHING reaches Cloud Logging until the
# process exits. A job that hangs then looks IDENTICAL to a job that never started,
# and the [checkpoint] progress lines -- the only visibility into a long crawl --
# cannot mechanically appear. Days were spent in 2026-07 telling Logan to watch for
# output that could not be printed. (CONVENTIONS.md, observability check #3.)
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./

# Overridden per-job by the deploy: the freshness job sets `--command python
# --args freshness_check.py`. Default is the ingest.
ENTRYPOINT ["python", "main.py"]
