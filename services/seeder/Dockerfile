FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
COPY requirements/base.txt /app/requirements/base.txt
COPY requirements/seeder.txt /app/requirements/seeder.txt
RUN pip install --no-cache-dir -r /app/requirements/seeder.txt
COPY common /app/common
COPY services /app/services
CMD ["/bin/sh", "-lc", "python -m services.seeder.app"]
