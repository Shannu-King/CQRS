CREATE TABLE product_sales_view (
    product_id INTEGER PRIMARY KEY,
    total_quantity_sold INTEGER NOT NULL DEFAULT 0,
    total_revenue DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    order_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE category_metrics_view (
    category_name VARCHAR(255) PRIMARY KEY,
    total_revenue DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    total_orders INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE customer_ltv_view (
    customer_id INTEGER PRIMARY KEY,
    total_spent DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    order_count INTEGER NOT NULL DEFAULT 0,
    last_order_date TIMESTAMP
);

CREATE TABLE hourly_sales_view (
    hour_timestamp TIMESTAMP PRIMARY KEY,
    total_orders INTEGER NOT NULL DEFAULT 0,
    total_revenue DECIMAL(10, 2) NOT NULL DEFAULT 0.00
);

CREATE TABLE sync_status (
    id INTEGER PRIMARY KEY DEFAULT 1,
    last_processed_event_timestamp TIMESTAMP,
    lag_seconds DECIMAL(10, 2) DEFAULT 0.00
);

INSERT INTO sync_status (id, lag_seconds) VALUES (1, 0.00);

CREATE TABLE product_categories (
    product_id INTEGER PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL
);

CREATE TABLE processed_events (
    event_id VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    event_timestamp TIMESTAMP
);
