package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

var db *sql.DB

func main() {
	log.Println("Starting Query Service...")

	initDB()

	r := mux.NewRouter()

	r.HandleFunc("/health", healthHandler).Methods("GET")
	r.HandleFunc("/api/analytics/products/{productId}/sales", productSalesHandler).Methods("GET")
	r.HandleFunc("/api/analytics/categories/{category}/revenue", categoryRevenueHandler).Methods("GET")
	r.HandleFunc("/api/analytics/customers/{customerId}/lifetime-value", customerLTVHandler).Methods("GET")
	r.HandleFunc("/api/analytics/sync-status", syncStatusHandler).Methods("GET")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}
	log.Printf("Listening on :%s\n", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		log.Fatal(err)
	}
}

func initDB() {
	var err error
	connStr := os.Getenv("READ_DATABASE_URL")
	if connStr == "" {
		log.Fatal("READ_DATABASE_URL is required")
	}

	for i := 0; i < 10; i++ {
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

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// --- Handlers ---

func productSalesHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	productID, err := strconv.Atoi(vars["productId"])
	if err != nil {
		http.Error(w, "invalid productId", http.StatusBadRequest)
		return
	}

	var totalQuantitySold int
	var totalRevenue float64
	var orderCount int

	err = db.QueryRow(
		"SELECT total_quantity_sold, total_revenue, order_count FROM product_sales_view WHERE product_id = $1",
		productID,
	).Scan(&totalQuantitySold, &totalRevenue, &orderCount)

	if err == sql.ErrNoRows {
		// Return 0s if no sales yet
		json.NewEncoder(w).Encode(map[string]interface{}{
			"productId":         productID,
			"totalQuantitySold": 0,
			"totalRevenue":      0.0,
			"orderCount":        0,
		})
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"productId":         productID,
		"totalQuantitySold": totalQuantitySold,
		"totalRevenue":      totalRevenue,
		"orderCount":        orderCount,
	})
}

func categoryRevenueHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	category := vars["category"]

	var totalRevenue float64
	var totalOrders int

	err := db.QueryRow(
		"SELECT total_revenue, total_orders FROM category_metrics_view WHERE category_name = $1",
		category,
	).Scan(&totalRevenue, &totalOrders)

	if err == sql.ErrNoRows {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"category":     category,
			"totalRevenue": 0.0,
			"totalOrders":  0,
		})
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"category":     category,
		"totalRevenue": totalRevenue,
		"totalOrders":  totalOrders,
	})
}

func customerLTVHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	customerID, err := strconv.Atoi(vars["customerId"])
	if err != nil {
		http.Error(w, "invalid customerId", http.StatusBadRequest)
		return
	}

	var totalSpent float64
	var orderCount int
	var lastOrderDate time.Time

	err = db.QueryRow(
		"SELECT total_spent, order_count, last_order_date FROM customer_ltv_view WHERE customer_id = $1",
		customerID,
	).Scan(&totalSpent, &orderCount, &lastOrderDate)

	if err == sql.ErrNoRows {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"customerId":    customerID,
			"totalSpent":    0.0,
			"orderCount":    0,
			"lastOrderDate": nil,
		})
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"customerId":    customerID,
		"totalSpent":    totalSpent,
		"orderCount":    orderCount,
		"lastOrderDate": lastOrderDate.Format(time.RFC3339),
	})
}

func syncStatusHandler(w http.ResponseWriter, r *http.Request) {
	var lastProcessedEventTimestamp sql.NullTime

	err := db.QueryRow(
		"SELECT last_processed_event_timestamp FROM sync_status WHERE id = 1",
	).Scan(&lastProcessedEventTimestamp)

	if err != nil && err != sql.ErrNoRows {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var timestampStr *string
	if lastProcessedEventTimestamp.Valid {
		t := lastProcessedEventTimestamp.Time.Format(time.RFC3339)
		timestampStr = &t
	}

	lag := 0.0
	if lastProcessedEventTimestamp.Valid {
		lag = time.Since(lastProcessedEventTimestamp.Time).Seconds()
		if lag < 0 {
			lag = 0
		}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"lastProcessedEventTimestamp": timestampStr,
		"lagSeconds":                  lag,
	})
}
