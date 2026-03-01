package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	amqp "github.com/rabbitmq/amqp091-go"
)

var db *sql.DB

type OrderEventItem struct {
	ProductID int     `json:"productId"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
	Category  string  `json:"category"`
}

type OrderEvent struct {
	EventID    string           `json:"eventId"`
	EventType  string           `json:"eventType"`
	OrderID    int              `json:"orderId"`
	CustomerID int              `json:"customerId"`
	Items      []OrderEventItem `json:"items"`
	Total      float64          `json:"total"`
	Timestamp  string           `json:"timestamp"`
}

type ProductEvent struct {
	EventID   string  `json:"eventId"`
	EventType string  `json:"eventType"`
	ProductID int     `json:"productId"`
	Name      string  `json:"name"`
	Category  string  `json:"category"`
	Price     float64 `json:"price"`
	Timestamp string  `json:"timestamp"`
}

func main() {
	log.Println("Starting Consumer Service...")

	initDB()
	go startHealthServer()
	startConsumer()
}

func initDB() {
	var err error
	connStr := os.Getenv("READ_DATABASE_URL")
	if connStr == "" {
		log.Fatal("READ_DATABASE_URL is required")
	}

	for i := 0; i < 15; i++ {
		db, err = sql.Open("postgres", connStr)
		if err == nil {
			err = db.Ping()
			if err == nil {
				log.Println("Connected to Read Database.")
				return
			}
		}
		log.Println("Database not ready, retrying...")
		time.Sleep(2 * time.Second)
	}
	log.Fatal("Could not connect to database: ", err)
}

func startHealthServer() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		if err := db.Ping(); err != nil {
			http.Error(w, "db not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}

func startConsumer() {
	time.Sleep(5 * time.Second) // wait for broker

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

	// Ensure queues exist
	qOrders, err := ch.QueueDeclare("order-events", true, false, false, false, nil)
	if err != nil {
		log.Fatal("Failed to declare order-events queue:", err)
	}

	msgsOrders, err := ch.Consume(
		qOrders.Name,
		"",    // consumer tag
		false, // auto-ack disabled for reliable processing
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("Failed to register consumer:", err)
	}

	// Product events queue
	qProducts, err := ch.QueueDeclare("product-events", true, false, false, false, nil)
	if err != nil {
		log.Fatal("Failed to declare product-events queue:", err)
	}

	msgsProducts, err := ch.Consume(
		qProducts.Name,
		"",    // consumer tag
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("Failed to register consumer:", err)
	}

	forever := make(chan struct{})

	go func() {
		for d := range msgsOrders {
			handleOrderMessage(d)
		}
	}()

	go func() {
		for d := range msgsProducts {
			handleProductMessage(d)
		}
	}()

	log.Println("Waiting for messages. To exit press CTRL+C")
	<-forever
}

func handleOrderMessage(d amqp.Delivery) {
	var event OrderEvent
	if err := json.Unmarshal(d.Body, &event); err != nil {
		log.Println("Error decoding order event:", err)
		_ = d.Nack(false, false)
		return
	}

	if event.EventType != "OrderCreated" {
		_ = d.Ack(false)
		return
	}

	if event.EventID == "" {
		event.EventID = d.MessageId
	}
	if event.EventID == "" {
		log.Println("Skipping order event without event ID")
		_ = d.Nack(false, false)
		return
	}

	eventTime := parseEventTime(event.Timestamp)
	err := processWithIdempotency(event.EventID, eventTime, func(tx *sql.Tx) error {
		_, err := tx.Exec(`
			INSERT INTO customer_ltv_view (customer_id, total_spent, order_count, last_order_date)
			VALUES ($1, $2, 1, $3)
			ON CONFLICT (customer_id) DO UPDATE SET
			total_spent = customer_ltv_view.total_spent + EXCLUDED.total_spent,
			order_count = customer_ltv_view.order_count + 1,
			last_order_date = GREATEST(customer_ltv_view.last_order_date, EXCLUDED.last_order_date)
		`, event.CustomerID, event.Total, eventTime)
		if err != nil {
			return err
		}

		hourTimestamp := eventTime.UTC().Truncate(time.Hour)
		_, err = tx.Exec(`
			INSERT INTO hourly_sales_view (hour_timestamp, total_orders, total_revenue)
			VALUES ($1, 1, $2)
			ON CONFLICT (hour_timestamp) DO UPDATE SET
			total_orders = hourly_sales_view.total_orders + 1,
			total_revenue = hourly_sales_view.total_revenue + EXCLUDED.total_revenue
		`, hourTimestamp, event.Total)
		if err != nil {
			return err
		}

		categoryRevenue := map[string]float64{}
		categoryOrderMarker := map[string]bool{}

		for _, item := range event.Items {
			itemRevenue := float64(item.Quantity) * item.Price

			_, err = tx.Exec(`
				INSERT INTO product_sales_view (product_id, total_quantity_sold, total_revenue, order_count)
				VALUES ($1, $2, $3, 1)
				ON CONFLICT (product_id) DO UPDATE SET
				total_quantity_sold = product_sales_view.total_quantity_sold + EXCLUDED.total_quantity_sold,
				total_revenue = product_sales_view.total_revenue + EXCLUDED.total_revenue,
				order_count = product_sales_view.order_count + 1
			`, item.ProductID, item.Quantity, itemRevenue)
			if err != nil {
				return err
			}

			categoryName := item.Category
			if categoryName == "" {
				_ = tx.QueryRow("SELECT category_name FROM product_categories WHERE product_id = $1", item.ProductID).Scan(&categoryName)
			}
			if categoryName == "" {
				categoryName = "unknown"
			}

			categoryRevenue[categoryName] += itemRevenue
			categoryOrderMarker[categoryName] = true
		}

		for categoryName, revenue := range categoryRevenue {
			ordersToAdd := 0
			if categoryOrderMarker[categoryName] {
				ordersToAdd = 1
			}

			_, err = tx.Exec(`
				INSERT INTO category_metrics_view (category_name, total_revenue, total_orders)
				VALUES ($1, $2, $3)
				ON CONFLICT (category_name) DO UPDATE SET
				total_revenue = category_metrics_view.total_revenue + EXCLUDED.total_revenue,
				total_orders = category_metrics_view.total_orders + EXCLUDED.total_orders
			`, categoryName, revenue, ordersToAdd)
			if err != nil {
				return err
			}
		}

		return updateSyncStatus(tx, eventTime)
	})

	if err != nil {
		log.Println("Error processing order event:", err)
		_ = d.Nack(false, true)
		return
	}

	_ = d.Ack(false)
}

func handleProductMessage(d amqp.Delivery) {
	var event ProductEvent
	if err := json.Unmarshal(d.Body, &event); err != nil {
		log.Println("Error decoding product event:", err)
		_ = d.Nack(false, false)
		return
	}

	if event.EventType != "ProductCreated" && event.EventType != "PriceChanged" {
		_ = d.Ack(false)
		return
	}

	if event.EventID == "" {
		event.EventID = d.MessageId
	}
	if event.EventID == "" {
		log.Println("Skipping product event without event ID")
		_ = d.Nack(false, false)
		return
	}

	eventTime := parseEventTime(event.Timestamp)
	err := processWithIdempotency(event.EventID, eventTime, func(tx *sql.Tx) error {
		if event.Category != "" {
			_, err := tx.Exec(`
				INSERT INTO category_metrics_view (category_name, total_revenue, total_orders)
				VALUES ($1, 0, 0)
				ON CONFLICT (category_name) DO NOTHING
			`, event.Category)
			if err != nil {
				return err
			}

			_, err = tx.Exec(`
				INSERT INTO product_categories (product_id, category_name)
				VALUES ($1, $2)
				ON CONFLICT (product_id) DO UPDATE SET category_name = EXCLUDED.category_name
			`, event.ProductID, event.Category)
			if err != nil {
				return err
			}
		}

		return updateSyncStatus(tx, eventTime)
	})

	if err != nil {
		log.Println("Error processing product event:", err)
		_ = d.Nack(false, true)
		return
	}

	_ = d.Ack(false)
}

func processWithIdempotency(eventID string, eventTime time.Time, updater func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	result, err := tx.Exec(`
		INSERT INTO processed_events (event_id, processed_at, event_timestamp)
		VALUES ($1, NOW(), $2)
		ON CONFLICT (event_id) DO NOTHING
	`, eventID, eventTime)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if affected == 0 {
		return tx.Commit()
	}

	if err := updater(tx); err != nil {
		return err
	}

	return tx.Commit()
}

func parseEventTime(ts string) time.Time {
	if ts == "" {
		return time.Now().UTC()
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return time.Now().UTC()
	}
	return t.UTC()
}

func updateSyncStatus(tx *sql.Tx, eventTime time.Time) error {
	lag := time.Since(eventTime).Seconds()
	if lag < 0 {
		lag = 0
	}

	_, err := tx.Exec(`
		INSERT INTO sync_status (id, last_processed_event_timestamp, lag_seconds)
		VALUES (1, $1, $2)
		ON CONFLICT (id) DO UPDATE SET
			last_processed_event_timestamp = GREATEST(sync_status.last_processed_event_timestamp, EXCLUDED.last_processed_event_timestamp),
			lag_seconds = EXCLUDED.lag_seconds
	`, eventTime, lag)
	return err
}

func getenvInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
