FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml README.md .
COPY src/ ./src/
RUN pip install --no-cache-dir cryptography && \
    pip install --no-cache-dir .[mysql]

COPY eventbus_main.py .

RUN mkdir -p /data

EXPOSE 8765

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8765/health')" || exit 1

ENTRYPOINT ["python", "-u", "eventbus_main.py"]
