FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    grpcio==1.64.0 \
    grpcio-tools==1.64.0 \
    sqlalchemy==2.0.30

COPY proto/ /app/proto/
COPY namenode/server.py /app/namenode/server.py

RUN python -m grpc_tools.protoc \
    -I /app/proto \
    --python_out=/app/proto \
    --grpc_python_out=/app/proto \
    /app/proto/dfs.proto

RUN mkdir -p /data

CMD ["python", "/app/namenode/server.py"]
