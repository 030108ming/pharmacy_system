-- Primary DB initialization
CREATE TABLE IF NOT EXISTS drugs (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 0,
    price FLOAT NOT NULL DEFAULT 0.0,
    expiry_date VARCHAR(50),
    category VARCHAR(100)
);

-- Seed sample data
INSERT INTO drugs (name, quantity, price, expiry_date, category) VALUES
('Aspirin', 500, 2.99, '2026-12-31', 'Pain Relief'),
('Ibuprofen', 300, 4.99, '2026-06-30', 'Pain Relief'),
('Amoxicillin', 150, 12.99, '2025-12-31', 'Antibiotic'),
('Metformin', 200, 8.99, '2027-01-15', 'Diabetes'),
('Lisinopril', 50, 6.99, '2026-03-20', 'Blood Pressure'),
('Vitamin C', 1000, 1.99, '2027-06-01', 'Supplement'),
('Paracetamol', 800, 1.49, '2026-09-15', 'Pain Relief'),
('Omeprazole', 25, 9.99, '2025-11-30', 'Antacid');
