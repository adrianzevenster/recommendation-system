FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY common /app/common
COPY services /app/services
CMD ["/bin/sh", "-lc", "python -m services.trainer.app"]
