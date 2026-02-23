FROM python:3.11-slim

WORKDIR /app

RUN pip install grpcio grpcio-tools psycopg2-binary --no-cache-dir

COPY proto/pharmacy.proto /app/proto/pharmacy.proto

RUN python -m grpc_tools.protoc \
    -I/app/proto \
    --python_out=/app/proto \
    --grpc_python_out=/app/proto \
    /app/proto/pharmacy.proto

COPY node2_api_server/server.py /app/server.py

CMD ["python", "/app/server.py"]
