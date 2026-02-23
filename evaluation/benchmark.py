"""
Performance Benchmark Script
Tests both gRPC microservice and REST monolith under varying loads
Usage: python benchmark.py
"""
import grpc
import requests
import time
import threading
import statistics
import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../proto'))
import pharmacy_pb2
import pharmacy_pb2_grpc

GRPC_HOST = "localhost:8080"
REST_HOST = "http://localhost:9000"

# ─── gRPC Benchmark ──────────────────────────────────────────────────────────

def grpc_add_drug(stub, result_list):
    start = time.time()
    try:
        resp = stub.AddDrug(pharmacy_pb2.AddDrugRequest(
            name="TestDrug", quantity=100, price=9.99,
            expiry_date="2027-01-01", category="Test"
        ))
        elapsed = (time.time() - start) * 1000
        result_list.append(elapsed if resp.success else None)
    except Exception:
        result_list.append(None)

def grpc_list_drugs(stub, result_list):
    start = time.time()
    try:
        stub.ListDrugs(pharmacy_pb2.ListDrugsRequest())
        elapsed = (time.time() - start) * 1000
        result_list.append(elapsed)
    except Exception:
        result_list.append(None)

def run_grpc_benchmark(num_users, scenario="write"):
    channel = grpc.insecure_channel(GRPC_HOST)
    stub = pharmacy_pb2_grpc.PharmacyServiceStub(channel)
    results = []
    threads = []

    start_all = time.time()
    for _ in range(num_users):
        if scenario == "write":
            t = threading.Thread(target=grpc_add_drug, args=(stub, results))
        else:
            t = threading.Thread(target=grpc_list_drugs, args=(stub, results))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total_time = time.time() - start_all
    valid = [r for r in results if r is not None]
    return {
        "users": num_users,
        "success": len(valid),
        "avg_latency_ms": round(statistics.mean(valid), 2) if valid else 0,
        "throughput_rps": round(len(valid) / total_time, 2) if total_time > 0 else 0
    }

# ─── REST Benchmark ──────────────────────────────────────────────────────────

def rest_add_drug(result_list):
    start = time.time()
    try:
        resp = requests.post(f"{REST_HOST}/drugs", json={
            "name": "TestDrug", "quantity": 100, "price": 9.99,
            "expiry_date": "2027-01-01", "category": "Test"
        }, timeout=10)
        elapsed = (time.time() - start) * 1000
        result_list.append(elapsed if resp.status_code == 200 else None)
    except Exception:
        result_list.append(None)

def rest_list_drugs(result_list):
    start = time.time()
    try:
        resp = requests.get(f"{REST_HOST}/drugs", timeout=10)
        elapsed = (time.time() - start) * 1000
        result_list.append(elapsed if resp.status_code == 200 else None)
    except Exception:
        result_list.append(None)

def run_rest_benchmark(num_users, scenario="write"):
    results = []
    threads = []

    start_all = time.time()
    for _ in range(num_users):
        if scenario == "write":
            t = threading.Thread(target=rest_add_drug, args=(results,))
        else:
            t = threading.Thread(target=rest_list_drugs, args=(results,))
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total_time = time.time() - start_all
    valid = [r for r in results if r is not None]
    return {
        "users": num_users,
        "success": len(valid),
        "avg_latency_ms": round(statistics.mean(valid), 2) if valid else 0,
        "throughput_rps": round(len(valid) / total_time, 2) if total_time > 0 else 0
    }

# ─── Main ─────────────────────────────────────────────────────────────────────

def print_table(title, results):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")
    print(f"  {'Users':<10} {'Avg Latency (ms)':<20} {'Throughput (req/s)':<20}")
    print(f"  {'-'*50}")
    for r in results:
        print(f"  {r['users']:<10} {r['avg_latency_ms']:<20} {r['throughput_rps']:<20}")

if __name__ == "__main__":
    user_counts = [10, 50, 100, 500, 1000]

    print("\n🚀 Starting Benchmark...")
    print("Make sure both systems are running:")
    print("  gRPC:  localhost:8080")
    print("  REST:  localhost:9000")
    input("\nPress Enter to start...\n")

    # Write benchmarks
    grpc_write = []
    rest_write = []
    for n in user_counts:
        print(f"  Testing {n} concurrent users (WRITE)...")
        grpc_write.append(run_grpc_benchmark(n, "write"))
        time.sleep(1)
        rest_write.append(run_rest_benchmark(n, "write"))
        time.sleep(1)

    # Read benchmarks
    grpc_read = []
    rest_read = []
    for n in user_counts:
        print(f"  Testing {n} concurrent users (READ)...")
        grpc_read.append(run_grpc_benchmark(n, "read"))
        time.sleep(1)
        rest_read.append(run_rest_benchmark(n, "read"))
        time.sleep(1)

    print_table("gRPC Microservice — Write (Add Drug)", grpc_write)
    print_table("REST Monolith — Write (Add Drug)", rest_write)
    print_table("gRPC Microservice — Read (List Drugs)", grpc_read)
    print_table("REST Monolith — Read (List Drugs)", rest_read)

    # Save results
    all_results = {
        "grpc_write": grpc_write, "rest_write": rest_write,
        "grpc_read": grpc_read, "rest_read": rest_read
    }
    with open("results.json", "w") as f:
        json.dump(all_results, f, indent=2)
    print("\n✅ Results saved to results.json")
    print("Run: python plot_results.py to generate graphs")
