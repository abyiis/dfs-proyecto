FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    grpcio==1.64.0 \
    grpcio-tools==1.64.0

COPY proto/ /app/proto/
COPY datanode/server.py /app/datanode/server.py

RUN python -m grpc_tools.protoc \
    -I /app/proto \
    --python_out=/app/proto \
    --grpc_python_out=/app/proto \
    /app/proto/dfs.proto

RUN mkdir -p /data/blocks

CMD ["python", "/app/datanode/server.py"]
