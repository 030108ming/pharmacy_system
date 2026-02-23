# 💊 Distributed Pharmacy Inventory Management System

**CSE-5306 Distributed Systems — Project 2**

## System Overview

A distributed pharmacy inventory management system built with two architectures:
1. **gRPC Microservice** — 6 containerized nodes with load balancing and DB replication
2. **REST Monolith** — single containerized service for comparison

## Six Functional Requirements

| # | Feature | gRPC Method | REST Endpoint |
|---|---------|-------------|---------------|
| 1 | Add Drug | `AddDrug` | `POST /drugs` |
| 2 | Get Drug by ID | `GetDrug` | `GET /drugs/{id}` |
| 3 | Update Stock | `UpdateStock` | `PUT /drugs/{id}/stock` |
| 4 | Delete Drug | `DeleteDrug` | `DELETE /drugs/{id}` |
| 5 | List All Drugs | `ListDrugs` | `GET /drugs` |
| 6 | Low Stock Alert | `GetLowStock` | `GET /drugs/alert/low-stock` |

## Architecture

### gRPC Microservice (6 Nodes)

```
Client (Python)
      ↓ gRPC
Node 1: NGINX Load Balancer       (port 8080)
   ↙              ↘
Node 2: gRPC API Server A         (port 50051)
Node 3: gRPC API Server B         (port 50051)
      ↓
Node 4: PostgreSQL Primary DB     (port 5432)
      ↓ streaming replication
Node 5: PostgreSQL Replica DB     (port 5433)

Node 6: pgAdmin Monitor Panel     (port 5050)
```

### REST Monolith (Comparison)

```
Client (Python/curl)
      ↓ HTTP
FastAPI Monolith                  (port 9000)
      ↓
PostgreSQL DB                     (internal)
```

## Quick Start

### Prerequisites
- Docker Desktop
- Python 3.10+
- Git

### Step 1 — Clone & Setup

```bash
git clone <your-repo-url>
cd pharmacy_system
pip install -r requirements.txt
```

### Step 2 — Generate gRPC Proto Code

```bash
chmod +x generate_proto.sh
./generate_proto.sh
```

### Step 3 — Start All Services

```bash
docker-compose up --build -d
```

Wait ~15 seconds for all services to initialize.

### Step 4 — Verify Services Running

```bash
docker ps
```

You should see 8 containers running:
- `node1-nginx-lb`
- `node2-api-server-a`
- `node3-api-server-b`
- `node4-db-primary`
- `node5-db-replica`
- `node6-pgadmin`
- `monolith-rest-api`
- `mono-db`

### Step 5 — Run Test Client (gRPC)

```bash
python client/test_client.py
```

### Step 6 — Test REST Monolith

```bash
# Add a drug
curl -X POST http://localhost:9000/drugs \
  -H "Content-Type: application/json" \
  -d '{"name":"Aspirin","quantity":500,"price":2.99,"expiry_date":"2026-12-31","category":"Pain Relief"}'

# List all drugs
curl http://localhost:9000/drugs

# Low stock alert
curl http://localhost:9000/drugs/alert/low-stock?threshold=100
```

### Step 7 — Run Performance Benchmark

```bash
cd evaluation
python benchmark.py
python plot_results.py
```

### Step 8 — View pgAdmin Dashboard (Node 6)

Open http://localhost:5050
- Email: `admin@pharmacy.com`
- Password: `admin`
- Add server: host=`node4-db-primary`, port=5432, user=`postgres`, password=`postgres`

## Stop All Services

```bash
docker-compose down -v
```

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.11 |
| gRPC Framework | grpcio + protobuf |
| REST Framework | FastAPI + uvicorn |
| Database | PostgreSQL 15 |
| Load Balancer | NGINX |
| Containerization | Docker + Docker Compose |
| Monitoring | pgAdmin 4 |

## AI Tools Usage

This project was developed with assistance from Claude (Anthropic) for:
- System architecture design
- Proto file generation
- Docker configuration
- Benchmark script creation
- Report writing

## Project Structure

```
pharmacy_system/
├── proto/                    # gRPC proto definitions
├── node1_nginx/              # NGINX load balancer config
├── node2_api_server/         # gRPC API server (used for nodes 2 & 3)
├── node4_db_primary/         # PostgreSQL primary with init SQL
├── monolith_rest/            # FastAPI REST comparison
├── client/                   # Test client scripts
├── evaluation/               # Benchmark + plotting scripts
├── docker-compose.yml
├── requirements.txt
└── README.md
```
