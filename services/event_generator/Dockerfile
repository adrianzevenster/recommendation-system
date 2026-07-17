FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
COPY requirements/base.txt /app/requirements/base.txt
COPY requirements/event_generator.txt /app/requirements/event_generator.txt
RUN pip install --no-cache-dir -r /app/requirements/event_generator.txt
COPY common /app/common
COPY services /app/services
CMD ["/bin/sh", "-lc", "python -m services.event_generator.app"]
