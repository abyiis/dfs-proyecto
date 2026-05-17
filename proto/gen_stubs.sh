#!/bin/bash
# Genera los stubs gRPC a partir del .proto
# Se ejecuta en el contenedor durante el build

python -m grpc_tools.protoc \
  -I /app/proto \
  --python_out=/app/proto \
  --grpc_python_out=/app/proto \
  /app/proto/dfs.proto

# Fix imports (grpc_tools genera imports absolutos que rompen en algunos setups)
sed -i 's/^import dfs_pb2/import dfs_pb2/' /app/proto/dfs_pb2_grpc.py 2>/dev/null || true
echo "Stubs generados OK"
