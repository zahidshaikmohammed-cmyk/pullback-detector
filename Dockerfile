FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml .
COPY pullback_detector ./pullback_detector
RUN pip install --no-cache-dir .

COPY config ./config
COPY README.md .
CMD ["python", "-m", "pullback_detector"]
