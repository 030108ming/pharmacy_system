import grpc
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../proto'))
import pharmacy_pb2
import pharmacy_pb2_grpc

def run(host="localhost", port="8080"):
    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = pharmacy_pb2_grpc.PharmacyServiceStub(channel)

    print("=" * 50)
    print("🏥 Pharmacy Distributed System - Test Client")
    print("=" * 50)

    # 1. Add Drug
    print("\n[1] Adding drugs...")
    drugs_to_add = [
        ("Aspirin", 500, 2.99, "2026-12-31", "Pain Relief"),
        ("Ibuprofen", 30, 4.99, "2026-06-30", "Pain Relief"),
        ("Amoxicillin", 150, 12.99, "2025-12-31", "Antibiotic"),
        ("Vitamin D", 10, 7.99, "2027-01-01", "Supplement"),
    ]
    added_ids = []
    for name, qty, price, exp, cat in drugs_to_add:
        resp = stub.AddDrug(pharmacy_pb2.AddDrugRequest(
            name=name, quantity=qty, price=price, expiry_date=exp, category=cat
        ))
        if resp.success:
            print(f"  ✅ Added: {name} (ID: {resp.drug.id})")
            added_ids.append(resp.drug.id)
        else:
            print(f"  ❌ Failed: {resp.message}")

    # 2. Get Drug
    print(f"\n[2] Getting drug ID={added_ids[0]}...")
    resp = stub.GetDrug(pharmacy_pb2.GetDrugRequest(id=added_ids[0]))
    if resp.success:
        d = resp.drug
        print(f"  ✅ Found: {d.name}, Qty: {d.quantity}, Price: ${d.price}")

    # 3. Update Stock
    print(f"\n[3] Updating stock of ID={added_ids[0]} to 999...")
    resp = stub.UpdateStock(pharmacy_pb2.UpdateStockRequest(id=added_ids[0], quantity=999))
    if resp.success:
        print(f"  ✅ Updated: {resp.drug.name} new qty = {resp.drug.quantity}")

    # 4. List All Drugs
    print("\n[4] Listing all drugs...")
    resp = stub.ListDrugs(pharmacy_pb2.ListDrugsRequest())
    for d in resp.drugs:
        print(f"  📦 [{d.id}] {d.name} | Qty: {d.quantity} | ${d.price} | Exp: {d.expiry_date}")

    # 5. Low Stock Alert
    print("\n[5] Low stock alert (threshold=100)...")
    resp = stub.GetLowStock(pharmacy_pb2.LowStockRequest(threshold=100))
    if resp.drugs:
        for d in resp.drugs:
            print(f"  ⚠️  LOW STOCK: {d.name} - only {d.quantity} left!")
    else:
        print("  ✅ No low stock items")

    # 6. Delete Drug
    print(f"\n[6] Deleting drug ID={added_ids[-1]}...")
    resp = stub.DeleteDrug(pharmacy_pb2.DeleteDrugRequest(id=added_ids[-1]))
    print(f"  {'✅' if resp.success else '❌'} {resp.message}")

    print("\n" + "=" * 50)
    print("✅ All tests completed!")
    print("=" * 50)

if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = sys.argv[2] if len(sys.argv) > 2 else "8080"
    run(host, port)
