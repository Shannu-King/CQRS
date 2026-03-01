package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

var db *sql.DB

type ProductReq struct {
	Name     string  `json:"name"`
	Category string  `json:"category"`
	Price    float64 `json:"price"`
	Stock    int     `json:"stock"`
}

type OrderItemReq struct {
	ProductID int     `json:"productId"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

type OrderReq struct {
	CustomerID int            `json:"customerId"`
	Items      []OrderItemReq `json:"items"`
}

type OrderEventItem struct {
	ProductID int     `json:"productId"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
	Category  string  `json:"category"`
}

func main() {
	log.Println("Starting Command Service...")

	initDB()
	go outboxPublisher()

	r := mux.NewRouter()
	r.HandleFunc("/health", healthHandler).Methods("GET")
	r.HandleFunc("/api/products", createProductHandler).Methods("POST")
	r.HandleFunc("/api/orders", createOrderHandler).Methods("POST")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Listening on :%s\n", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		log.Fatal(err)
	}
}

func initDB() {
	var err error
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		log.Fatal("DATABASE_URL is required")
	}

	for i := 0; i < 15; i++ {
		db, err = sql.Open("postgres", connStr)
		if err == nil {
			err = db.Ping()
			if err == nil {
				log.Println("Connected to Write Database.")
				return
			}
		}
		log.Println("Database not ready, retrying...")
		time.Sleep(2 * time.Second)
	}

	log.Fatal("Could not connect to database: ", err)
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func createProductHandler(w http.ResponseWriter, r *http.Request) {
	var req ProductReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Name == "" || req.Category == "" || req.Price <= 0 || req.Stock < 0 {
		http.Error(w, "invalid product payload", http.StatusBadRequest)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	var productID int
	err = tx.QueryRow(
		"INSERT INTO products (name, category, price, stock) VALUES ($1, $2, $3, $4) RETURNING id",
		req.Name, req.Category, req.Price, req.Stock,
	).Scan(&productID)
	if err != nil {
		log.Println("Error creating product:", err)
		http.Error(w, "failed to create product", http.StatusInternalServerError)
		return
	}

	eventPayload := map[string]interface{}{
		"eventId":   uuid.NewString(),
		"eventType": "ProductCreated",
		"productId": productID,
		"name":      req.Name,
		"category":  req.Category,
		"price":     req.Price,
		"stock":     req.Stock,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	payload, err := json.Marshal(eventPayload)
	if err != nil {
		http.Error(w, "failed to marshal event payload", http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec("INSERT INTO outbox (topic, payload) VALUES ($1, $2)", "product-events", payload)
	if err != nil {
		log.Println("Error inserting product outbox event:", err)
		http.Error(w, "failed to create outbox event", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "failed to commit transaction", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"productId": productID})
}

func createOrderHandler(w http.ResponseWriter, r *http.Request) {
	var req OrderReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.CustomerID <= 0 || len(req.Items) == 0 {
		http.Error(w, "invalid order payload", http.StatusBadRequest)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		http.Error(w, "failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	total := 0.0
	eventItems := make([]OrderEventItem, 0, len(req.Items))

	for _, item := range req.Items {
		if item.ProductID <= 0 || item.Quantity <= 0 || item.Price <= 0 {
			http.Error(w, "invalid order item payload", http.StatusBadRequest)
			return
		}

		var currentStock int
		var category string
		err = tx.QueryRow(
			"SELECT stock, category FROM products WHERE id = $1 FOR UPDATE",
			item.ProductID,
		).Scan(&currentStock, &category)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				http.Error(w, "product not found", http.StatusBadRequest)
				return
			}
			http.Error(w, "failed to check product stock", http.StatusInternalServerError)
			return
		}

		if currentStock < item.Quantity {
			http.Error(w, "insufficient stock", http.StatusConflict)
			return
		}

		_, err = tx.Exec(
			"UPDATE products SET stock = stock - $1 WHERE id = $2",
			item.Quantity,
			item.ProductID,
		)
		if err != nil {
			http.Error(w, "failed to update stock", http.StatusInternalServerError)
			return
		}

		total += item.Price * float64(item.Quantity)
		eventItems = append(eventItems, OrderEventItem{
			ProductID: item.ProductID,
			Quantity:  item.Quantity,
			Price:     item.Price,
			Category:  category,
		})
	}

	var orderID int
	err = tx.QueryRow(
		"INSERT INTO orders (customer_id, total, status) VALUES ($1, $2, $3) RETURNING id",
		req.CustomerID, total, "Created",
	).Scan(&orderID)
	if err != nil {
		http.Error(w, "failed to insert order", http.StatusInternalServerError)
		return
	}

	for _, item := range req.Items {
		_, err = tx.Exec(
			"INSERT INTO order_items (order_id, product_id, quantity, price) VALUES ($1, $2, $3, $4)",
			orderID, item.ProductID, item.Quantity, item.Price,
		)
		if err != nil {
			http.Error(w, "failed to insert order item", http.StatusInternalServerError)
			return
		}
	}

	eventPayload := map[string]interface{}{
		"eventId":    uuid.NewString(),
		"eventType":  "OrderCreated",
		"orderId":    orderID,
		"customerId": req.CustomerID,
		"items":      eventItems,
		"total":      total,
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	}
	payloadBytes, err := json.Marshal(eventPayload)
	if err != nil {
		http.Error(w, "failed to marshal event payload", http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(
		"INSERT INTO outbox (topic, payload) VALUES ($1, $2)",
		"order-events", payloadBytes,
	)
	if err != nil {
		http.Error(w, "failed to create outbox event", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "failed to commit transaction", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"orderId": orderID})
}

type outboxEvent struct {
	ID      int
	Topic   string
	Payload []byte
}

func outboxPublisher() {
	browserPause := 5 * time.Second
	time.Sleep(browserPause)

	brokerURL := os.Getenv("BROKER_URL")
	if brokerURL == "" {
		log.Fatal("BROKER_URL is required")
	}

	var conn *amqp.Connection
	var err error
	for i := 0; i < 15; i++ {
		conn, err = amqp.Dial(brokerURL)
		if err == nil {
			break
		}
		log.Println("Broker not ready, retrying...")
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ:", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal("Failed to open channel:", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare("order-events", true, false, false, false, nil)
	if err != nil {
		log.Println("Failed to declare order-events queue:", err)
	}
	_, err = ch.QueueDeclare("product-events", true, false, false, false, nil)
	if err != nil {
		log.Println("Failed to declare product-events queue:", err)
	}

	for {
		rows, err := db.Query(
			"SELECT id, topic, payload FROM outbox WHERE published_at IS NULL AND created_at <= NOW() - INTERVAL '2 seconds' ORDER BY created_at ASC LIMIT 20",
		)
		if err != nil {
			log.Println("Error querying outbox:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		events := make([]outboxEvent, 0, 20)
		for rows.Next() {
			var evt outboxEvent
			if err := rows.Scan(&evt.ID, &evt.Topic, &evt.Payload); err != nil {
				log.Println("Error scanning outbox row:", err)
				continue
			}
			events = append(events, evt)
		}
		rows.Close()

		if len(events) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}

		for _, evt := range events {
			messageID := ""
			var envelope map[string]interface{}
			if err := json.Unmarshal(evt.Payload, &envelope); err == nil {
				if v, ok := envelope["eventId"].(string); ok {
					messageID = v
				}
			}

			err = ch.PublishWithContext(
				context.Background(),
				"",
				evt.Topic,
				false,
				false,
				amqp.Publishing{
					ContentType:  "application/json",
					Body:         evt.Payload,
					DeliveryMode: amqp.Persistent,
					MessageId:    messageID,
				},
			)
			if err != nil {
				log.Println("Error publishing event ID", evt.ID, ":", err)
				continue
			}

			_, err = db.Exec("UPDATE outbox SET published_at = NOW() WHERE id = $1", evt.ID)
			if err != nil {
				log.Println("Error updating published_at for event ID", evt.ID, ":", err)
			}
		}
	}
}
