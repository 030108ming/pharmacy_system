"""
Plot benchmark results from results.json
Usage: python plot_results.py
"""
import json
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

with open("results.json") as f:
    data = json.load(f)

def extract(results, key):
    return [r[key] for r in results]

users = extract(data["grpc_write"], "users")

fig = plt.figure(figsize=(14, 10))
fig.suptitle("Distributed Pharmacy System - Performance Comparison\ngRPC Microservice vs REST Monolith", fontsize=14, fontweight='bold')

gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.4, wspace=0.35)

# Plot 1: Write Latency
ax1 = fig.add_subplot(gs[0, 0])
ax1.plot(users, extract(data["grpc_write"], "avg_latency_ms"), 'b-o', label='gRPC Microservice', linewidth=2)
ax1.plot(users, extract(data["rest_write"], "avg_latency_ms"), 'r-s', label='REST Monolith', linewidth=2)
ax1.set_title("Write Latency (Add Drug)")
ax1.set_xlabel("Concurrent Users")
ax1.set_ylabel("Avg Latency (ms)")
ax1.legend()
ax1.grid(True, alpha=0.3)

# Plot 2: Write Throughput
ax2 = fig.add_subplot(gs[0, 1])
ax2.plot(users, extract(data["grpc_write"], "throughput_rps"), 'b-o', label='gRPC Microservice', linewidth=2)
ax2.plot(users, extract(data["rest_write"], "throughput_rps"), 'r-s', label='REST Monolith', linewidth=2)
ax2.set_title("Write Throughput (Add Drug)")
ax2.set_xlabel("Concurrent Users")
ax2.set_ylabel("Throughput (req/s)")
ax2.legend()
ax2.grid(True, alpha=0.3)

# Plot 3: Read Latency
ax3 = fig.add_subplot(gs[1, 0])
ax3.plot(users, extract(data["grpc_read"], "avg_latency_ms"), 'b-o', label='gRPC Microservice', linewidth=2)
ax3.plot(users, extract(data["rest_read"], "avg_latency_ms"), 'r-s', label='REST Monolith', linewidth=2)
ax3.set_title("Read Latency (List Drugs)")
ax3.set_xlabel("Concurrent Users")
ax3.set_ylabel("Avg Latency (ms)")
ax3.legend()
ax3.grid(True, alpha=0.3)

# Plot 4: Read Throughput
ax4 = fig.add_subplot(gs[1, 1])
ax4.plot(users, extract(data["grpc_read"], "throughput_rps"), 'b-o', label='gRPC Microservice', linewidth=2)
ax4.plot(users, extract(data["rest_read"], "throughput_rps"), 'r-s', label='REST Monolith', linewidth=2)
ax4.set_title("Read Throughput (List Drugs)")
ax4.set_xlabel("Concurrent Users")
ax4.set_ylabel("Throughput (req/s)")
ax4.legend()
ax4.grid(True, alpha=0.3)

plt.savefig("performance_comparison.png", dpi=150, bbox_inches='tight')
print("✅ Graph saved to performance_comparison.png")
plt.show()
