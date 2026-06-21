FROM python:3.11-slim

WORKDIR /app

COPY worker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy proto files and generate code
COPY protos/ /app/protos/
RUN mkdir -p /app/generated && \
    python -m grpc_tools.protoc -I/app/protos --python_out=/app/generated --grpc_python_out=/app/generated /app/protos/worker.proto && \
    python -m grpc_tools.protoc -I/app/protos --python_out=/app/generated --grpc_python_out=/app/generated /app/protos/enrichment.proto && \
    touch /app/generated/__init__.py

COPY worker/ /app/

EXPOSE 50051

CMD ["python", "server.py"]
