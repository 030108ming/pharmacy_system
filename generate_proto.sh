#!/bin/bash
# Run this once to generate Python gRPC code from proto file
echo "Generating gRPC Python code..."
pip install grpcio-tools --quiet
python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./proto \
    --grpc_python_out=./proto \
    ./proto/pharmacy.proto
echo "✅ Done! Generated files in ./proto/"
ls ./proto/
