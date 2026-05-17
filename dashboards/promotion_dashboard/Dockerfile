FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8505

ENV OSP_WAREHOUSE_PATH=/data/warehouse.duckdb
ENV OSP_CI_CD_LOG_TABLE=ci_cd_log

CMD ["streamlit", "run", "app.py", \
     "--server.port=8505", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
