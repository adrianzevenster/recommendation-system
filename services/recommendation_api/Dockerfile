FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
COPY requirements/base.txt /app/requirements/base.txt
COPY requirements/recommendation_api.txt /app/requirements/recommendation_api.txt
RUN pip install --no-cache-dir -r /app/requirements/recommendation_api.txt
COPY common /app/common
COPY services /app/services
CMD ["/bin/sh", "-lc", "uvicorn services.recommendation_api.app:app --host 0.0.0.0 --port 8000"]
