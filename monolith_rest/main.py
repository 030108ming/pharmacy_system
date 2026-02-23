from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import psycopg2
import os
import time

app = FastAPI(title="Pharmacy Monolith REST API")

DB_HOST = os.environ.get("DB_HOST", "db-mono")
DB_NAME = os.environ.get("DB_NAME", "pharmacy")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASS = os.environ.get("DB_PASS", "postgres")

def get_conn():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def init_db():
    for i in range(10):
        try:
            conn = get_conn()
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
            print("Monolith DB ready")
            return
        except Exception as e:
            print(f"Attempt {i+1}: {e}")
            time.sleep(3)

class DrugCreate(BaseModel):
    name: str
    quantity: int
    price: float
    expiry_date: str
    category: str

class StockUpdate(BaseModel):
    quantity: int

@app.on_event("startup")
def startup():
    init_db()

@app.post("/drugs")
def add_drug(drug: DrugCreate):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO drugs (name, quantity, price, expiry_date, category) VALUES (%s,%s,%s,%s,%s) RETURNING id",
        (drug.name, drug.quantity, drug.price, drug.expiry_date, drug.category)
    )
    drug_id = cur.fetchone()[0]
    conn.commit()
    cur.close(); conn.close()
    return {"id": drug_id, **drug.dict()}

@app.get("/drugs/{drug_id}")
def get_drug(drug_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name, quantity, price, expiry_date, category FROM drugs WHERE id=%s", (drug_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Drug not found")
    return {"id": row[0], "name": row[1], "quantity": row[2], "price": row[3], "expiry_date": row[4], "category": row[5]}

@app.put("/drugs/{drug_id}/stock")
def update_stock(drug_id: int, update: StockUpdate):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE drugs SET quantity=%s WHERE id=%s RETURNING id, name, quantity, price, expiry_date, category", (update.quantity, drug_id))
    row = cur.fetchone()
    conn.commit()
    cur.close(); conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Drug not found")
    return {"id": row[0], "name": row[1], "quantity": row[2], "price": row[3], "expiry_date": row[4], "category": row[5]}

@app.delete("/drugs/{drug_id}")
def delete_drug(drug_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM drugs WHERE id=%s RETURNING id", (drug_id,))
    row = cur.fetchone()
    conn.commit()
    cur.close(); conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Drug not found")
    return {"success": True, "message": f"Drug {drug_id} deleted"}

@app.get("/drugs")
def list_drugs():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name, quantity, price, expiry_date, category FROM drugs ORDER BY id")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"id": r[0], "name": r[1], "quantity": r[2], "price": r[3], "expiry_date": r[4], "category": r[5]} for r in rows]

@app.get("/drugs/alert/low-stock")
def low_stock(threshold: int = 100):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name, quantity, price, expiry_date, category FROM drugs WHERE quantity <= %s ORDER BY quantity", (threshold,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"id": r[0], "name": r[1], "quantity": r[2], "price": r[3], "expiry_date": r[4], "category": r[5]} for r in rows]
