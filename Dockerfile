# ---------- Stage 1: builder ----------
FROM python:3.12-slim AS builder

WORKDIR /install
COPY requirements.txt .

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc build-essential && \
    pip wheel --no-cache-dir -r requirements.txt

# ---------- Stage 2: runtime ----------
FROM python:3.12-slim

WORKDIR /app
COPY --from=builder /install /wheels
RUN pip install --no-cache-dir /wheels/*.whl && \
    rm -rf /wheels /root/.cache

COPY load.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "load.py"]
