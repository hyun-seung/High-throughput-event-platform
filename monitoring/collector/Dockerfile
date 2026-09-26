FROM python:3.14.0-slim
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka==2.15.1
COPY scripts/poc/검증_근거.py /app/검증_근거.py
COPY monitoring/collector/수집기.py /app/수집기.py
USER 65534:65534
CMD ["python", "-u", "수집기.py"]
