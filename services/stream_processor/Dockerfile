FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
COPY requirements/base.txt /app/requirements/base.txt
COPY requirements/stream_processor.txt /app/requirements/stream_processor.txt
RUN pip install --no-cache-dir -r /app/requirements/stream_processor.txt
COPY common /app/common
COPY services /app/services
CMD ["/bin/sh", "-lc", "python -m services.stream_processor.app"]
