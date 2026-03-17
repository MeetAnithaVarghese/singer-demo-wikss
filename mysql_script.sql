
DROP DATABASE IF EXISTS singer_demonstration;
CREATE DATABASE singer_demonstration;
USE singer_demonstration;

-- 1. Products Table (Standard Inventory)
CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sku VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    unit_price DECIMAL(10, 2),
    stock_quantity INT,
    last_restock_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 2. Suppliers Table (Contact Data)
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id INT AUTO_INCREMENT PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    contact_name VARCHAR(100),
    contact_email VARCHAR(150),
    country VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE
);

-- 3. Sales Pipeline (High-churn data for testing Incremental Sync)
CREATE TABLE IF NOT EXISTS sales_pipeline (
    deal_id INT AUTO_INCREMENT PRIMARY KEY,
    account_name VARCHAR(255),
    deal_value INT,
    stage VARCHAR(50), -- e.g., 'Discovery', 'Proposal', 'Closed'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Seed Products
INSERT INTO products (sku, product_name, category, unit_price, stock_quantity) VALUES
('SKU-001', 'Enterprise Server Rack', 'Hardware', 1200.00, 15),
('SKU-002', 'Wireless Access Point', 'Networking', 199.50, 45),
('SKU-003', 'Fiber Optic Cable 10m', 'Cables', 25.00, 200),
('SKU-004', 'Managed Switch 24-Port', 'Networking', 450.00, 10),
('SKU-005', 'Smart UPS 1500VA', 'Power', 299.99, 5);

-- Seed Suppliers with USA-based names
INSERT INTO suppliers (company_name, contact_name, contact_email, country) VALUES
('Miller Tech Solutions', 'James Miller', 'j.miller@millertech.com', 'USA'),
('Davis Global Logistics', 'Emma Davis', 'emma.davis@davislogistics.com', 'USA'),
('Wilson Hardware Group', 'Michael Wilson', 'm.wilson@wilsonhardware.com', 'USA'),
('Taylor Networking', 'Olivia Taylor', 'olivia.t@taylornet.com', 'USA'),
('Anderson Supply Co.', 'William Anderson', 'bill.a@andersonsupply.com', 'USA');

INSERT INTO sales_pipeline (account_name, deal_value, stage) VALUES
('Seattle Cloud Services', 85000, 'Proposal'),
('Austin FinTech Partners', 42000, 'Discovery'),
('Denver BioLabs', 125000, 'Negotiation'),
('Chicago Retail Group', 15000, 'Closed Won');


--
-- 1. Create the user with a specific password
CREATE USER 'singer_admin' IDENTIFIED BY 'password123';

-- 2. Grant permissions ONLY to your demonstration database
GRANT ALL PRIVILEGES ON singer_demonstration.* TO 'singer_admin';

-- 3. Update the internal privilege tables
FLUSH PRIVILEGES;

-- 1. Grant the PROCESS privilege (required for schema discovery)
GRANT PROCESS ON *.* TO 'singer_admin';

-- 2. Ensure it can see the metadata for the demo database
GRANT SELECT ON performance_schema.* TO 'singer_admin';

-- 3. Finalize the changes
FLUSH PRIVILEGES;