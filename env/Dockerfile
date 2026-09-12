FROM python:3.11-slim

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" "SQLAlchemy>=2.0" \
    "pydantic>=2.6" "psycopg[binary]>=3.1"

COPY app ./app

ENV APP_VERSION=1.0.0
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=3s \
  CMD python -c "import urllib.request;urllib.request.urlopen('http://localhost:8000/api/status')" || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
