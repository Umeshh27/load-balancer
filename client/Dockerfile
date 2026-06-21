FROM python:3.11-slim

WORKDIR /app

# Install system deps for matplotlib
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && rm -rf /var/lib/apt/lists/*

COPY client/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy proto files and generate code
COPY protos/ /app/protos/
RUN mkdir -p /app/generated && \
    python -m grpc_tools.protoc -I/app/protos --python_out=/app/generated --grpc_python_out=/app/generated /app/protos/worker.proto && \
    python -m grpc_tools.protoc -I/app/protos --python_out=/app/generated --grpc_python_out=/app/generated /app/protos/enrichment.proto && \
    touch /app/generated/__init__.py

COPY client/ /app/

# Copy test scripts
COPY reproduce_write_skew.py /app/reproduce_write_skew.py
COPY benchmark_locking.py /app/benchmark_locking.py

CMD ["tail", "-f", "/dev/null"]
