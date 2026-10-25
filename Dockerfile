FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin bot
COPY app.py api.py storage.py engine.py rules.py policy.py incidents.py daily_report.py approval_policy.json ./
USER 10001:10001
CMD ["python", "-m", "app"]
