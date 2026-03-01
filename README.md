# CQRS + Event-Driven Analytics System

This project implements an e-commerce analytics backend using **CQRS**, **transactional outbox**, and **RabbitMQ**.

## Highlights

- CQRS separation with dedicated write/read models.
- Transactional outbox to prevent dual-write inconsistency.
- Event-driven materialized views for fast analytics queries.
- Idempotent consumers using `processed_events(event_id)`.
- Full Docker Compose setup with health checks for all services.

## Architecture

- **command-service** (`:8080`): accepts write commands (`POST /api/products`, `POST /api/orders`), writes to write model, appends outbox events atomically.
- **consumer-service**: consumes events from RabbitMQ (`order-events`, `product-events`), updates read-model materialized tables, enforces idempotency via `processed_events`.
- **query-service** (`:8081`): read-only analytics API over denormalized read model.
- **db** (PostgreSQL): hosts both `write_db` and `read_db`.
- **broker** (RabbitMQ): asynchronous event transport.

## Data Flow

1. Client sends command to command-service.
2. Command-service performs transactional write in `write_db` and inserts event row in `outbox`.
3. Outbox publisher sends events to RabbitMQ and marks outbox rows as published.
4. Consumer-service processes events and updates read-model tables in `read_db`.
5. Query-service serves fast analytics reads from read-model tables.

## Databases

### Write model (`write_db`)

- `products`
- `orders`
- `order_items`
- `outbox`

### Read model (`read_db`)

- `product_sales_view`
- `category_metrics_view`
- `customer_ltv_view`
- `hourly_sales_view`
- `product_categories`
- `processed_events`
- `sync_status`

## Endpoints

### Command Service

- `GET /health`
- `POST /api/products`
- `POST /api/orders`

### Query Service

- `GET /health`
- `GET /api/analytics/products/{productId}/sales`
- `GET /api/analytics/categories/{category}/revenue`
- `GET /api/analytics/customers/{customerId}/lifetime-value`
- `GET /api/analytics/sync-status`

## Setup

```bash
docker-compose up --build
```

## Quick Verification

Run full mandatory + negative checks in one command:

```powershell
powershell -ExecutionPolicy Bypass -File .\verify-all.ps1
```

Expected summary:

- `Core Score: 16/16`
- `Extra Negative Tests: 3/3`
- `Overall: PASS`

## Demo

### 1) Create a product

```powershell
Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/products' \
	-ContentType 'application/json' \
	-Body '{"name":"Demo Phone","category":"electronics","price":499,"stock":20}'
```

### 2) Create an order

```powershell
Invoke-RestMethod -Method Post -Uri 'http://localhost:8080/api/orders' \
	-ContentType 'application/json' \
	-Body '{"customerId":101,"items":[{"productId":1,"quantity":2,"price":499}]}'
```

### 3) Read analytics (after a few seconds)

```powershell
Invoke-RestMethod -Method Get -Uri 'http://localhost:8081/api/analytics/products/1/sales'
Invoke-RestMethod -Method Get -Uri 'http://localhost:8081/api/analytics/categories/electronics/revenue'
Invoke-RestMethod -Method Get -Uri 'http://localhost:8081/api/analytics/customers/101/lifetime-value'
Invoke-RestMethod -Method Get -Uri 'http://localhost:8081/api/analytics/sync-status'
```

Services become available at:

- Command Service: `http://localhost:8080`
- Query Service: `http://localhost:8081`
- RabbitMQ Management: `http://localhost:15672` (`guest` / `guest`)

## Environment Variables

See `.env.example` for required variables and placeholders.

## Notes

- Consumers are idempotent using `processed_events(event_id)`.
- `sync-status` exposes `lastProcessedEventTimestamp` and computed `lagSeconds`.
- Event processing follows at-least-once semantics with message ack/nack handling.
