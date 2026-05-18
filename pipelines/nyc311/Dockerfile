FROM python:3.11-slim

WORKDIR /app

# System deps for DuckDB native extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expose Dagster UI port
EXPOSE 3000

# Start Dagster UI + daemon (serves the pipeline.py definitions)
CMD ["dagster", "dev", "-f", "orchestrate/pipeline.py", "--host", "0.0.0.0", "--port", "3000"]
