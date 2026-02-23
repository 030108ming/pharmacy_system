import grpc
from concurrent import futures
import time
import os
import psycopg2
import psycopg2.pool
import sys

sys.path.insert(0, '/app/proto')
import pharmacy_pb2
import pharmacy_pb2_grpc

DB_HOST = os.environ.get("DB_HOST", "db-primary")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "pharmacy")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "postgres")

def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )

def init_db():
    for i in range(10):
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS drugs (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    quantity INTEGER NOT NULL DEFAULT 0,
                    price FLOAT NOT NULL DEFAULT 0.0,
                    expiry_date VARCHAR(50),
                    category VARCHAR(100)
                )
            """)
            conn.commit()
            cur.close()
            conn.close()
            print("Database initialized successfully")
            return
        except Exception as e:
            print(f"DB init attempt {i+1} failed: {e}")
            time.sleep(3)
    raise Exception("Could not connect to database after 10 attempts")

class PharmacyServicer(pharmacy_pb2_grpc.PharmacyServiceServicer):

    def AddDrug(self, request, context):
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO drugs (name, quantity, price, expiry_date, category) VALUES (%s,%s,%s,%s,%s) RETURNING id",
                (request.name, request.quantity, request.price, request.expiry_date, request.category)
            )
            drug_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
            conn.close()
            drug = pharmacy_pb2.Drug(
                id=drug_id, name=request.name, quantity=request.quantity,
                price=request.price, expiry_date=request.expiry_date, category=request.category
            )
            return pharmacy_pb2.DrugResponse(success=True, message="Drug added", drug=drug)
        except Exception as e:
            return pharmacy_pb2.DrugResponse(success=False, message=str(e))

    def GetDrug(self, request, context):
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT id, name, quantity, price, expiry_date, category FROM drugs WHERE id=%s", (request.id,))
            row = cur.fetchone()
            cur.close()
            conn.close()
            if not row:
                return pharmacy_pb2.DrugResponse(success=False, message="Drug not found")
            drug = pharmacy_pb2.Drug(id=row[0], name=row[1], quantity=row[2], price=row[3], expiry_date=row[4], category=row[5])
            return pharmacy_pb2.DrugResponse(success=True, message="Found", drug=drug)
        except Exception as e:
            return pharmacy_pb2.DrugResponse(success=False, message=str(e))

    def UpdateStock(self, request, context):
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("UPDATE drugs SET quantity=%s WHERE id=%s RETURNING id, name, quantity, price, expiry_date, category", (request.quantity, request.id))
            row = cur.fetchone()
            conn.commit()
            cur.close()
            conn.close()
            if not row:
                return pharmacy_pb2.DrugResponse(success=False, message="Drug not found")
            drug = pharmacy_pb2.Drug(id=row[0], name=row[1], quantity=row[2], price=row[3], expiry_date=row[4], category=row[5])
            return pharmacy_pb2.DrugResponse(success=True, message="Stock updated", drug=drug)
        except Exception as e:
            return pharmacy_pb2.DrugResponse(success=False, message=str(e))

    def DeleteDrug(self, request, context):
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("DELETE FROM drugs WHERE id=%s RETURNING id", (request.id,))
            row = cur.fetchone()
            conn.commit()
            cur.close()
            conn.close()
            if not row:
                return pharmacy_pb2.DeleteResponse(success=False, message="Drug not found")
            return pharmacy_pb2.DeleteResponse(success=True, message=f"Drug {request.id} deleted")
        except Exception as e:
            return pharmacy_pb2.DeleteResponse(success=False, message=str(e))

    def ListDrugs(self, request, context):
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT id, name, quantity, price, expiry_date, category FROM drugs ORDER BY id")
            rows = cur.fetchall()
            cur.close()
            conn.close()
            drugs = [pharmacy_pb2.Drug(id=r[0], name=r[1], quantity=r[2], price=r[3], expiry_date=r[4], category=r[5]) for r in rows]
            return pharmacy_pb2.ListDrugsResponse(drugs=drugs)
        except Exception as e:
            return pharmacy_pb2.ListDrugsResponse(drugs=[])

    def GetLowStock(self, request, context):
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT id, name, quantity, price, expiry_date, category FROM drugs WHERE quantity <= %s ORDER BY quantity", (request.threshold,))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            drugs = [pharmacy_pb2.Drug(id=r[0], name=r[1], quantity=r[2], price=r[3], expiry_date=r[4], category=r[5]) for r in rows]
            return pharmacy_pb2.ListDrugsResponse(drugs=drugs)
        except Exception as e:
            return pharmacy_pb2.ListDrugsResponse(drugs=[])

def serve():
    init_db()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    pharmacy_pb2_grpc.add_PharmacyServiceServicer_to_server(PharmacyServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("gRPC server started on port 50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
