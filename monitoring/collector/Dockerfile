FROM python:3.14.0-slim
WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka==2.15.1
COPY scripts/poc/evidence.py /app/evidence.py
COPY monitoring/collector/collector.py /app/collector.py
USER 65534:65534
CMD ["python", "-u", "collector.py"]
