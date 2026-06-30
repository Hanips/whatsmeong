package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"

	"database/sql"
)

// getEnv adalah helper untuk membaca environment variable.
func getEnv(key string) string {
	return os.Getenv(key)
}

func main() {
	dbLog := waLog.Stdout("Database", "INFO", true)

	dbURL := getEnv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL environment variable is required (e.g. from Neon.tech)")
	}

	apiKey := getEnv("API_KEY")
	if apiKey == "" {
		log.Fatal("API_KEY environment variable is required for security! Silakan atur di Environment Variables Render.com")
	}

	// Connect to PostgreSQL (Neon.tech or similar)
	var err error
	waContainer, err = sqlstore.New(context.Background(), "postgres", dbURL, dbLog)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	appDB, err = sql.Open("postgres", dbURL)
	if err == nil {
		// Neon.tech auto-suspends after inactivity — keep connections healthy
		appDB.SetMaxIdleConns(1)
		appDB.SetMaxOpenConns(5)
		appDB.SetConnMaxLifetime(4 * time.Minute)
		appDB.SetConnMaxIdleTime(2 * time.Minute)
		_, err = appDB.Exec(`CREATE TABLE IF NOT EXISTS wa_settings (key text PRIMARY KEY, value text)`)
		if err != nil {
			log.Printf("Warning: failed to create wa_settings table: %v", err)
		}
		_, err = appDB.Exec(`CREATE TABLE IF NOT EXISTS wa_poll_options (msg_id text, option_hash text, option_text text)`)
		if err != nil {
			log.Printf("Warning: failed to create wa_poll_options table: %v", err)
		}
	} else {
		log.Printf("Warning: failed to open direct DB connection: %v", err)
	}

	// Get the first linked device
	deviceStore, err := waContainer.GetFirstDevice(context.Background())
	if err != nil {
		log.Fatalf("Failed to get device: %v", err)
	}

	clientLog := waLog.Stdout("Client", "INFO", true)
	client = whatsmeow.NewClient(deviceStore, clientLog)
	client.AddEventHandler(eventHandler)

	if client.Store.ID == nil {
		// No ID stored, need to link device
		go startQRFlow()
	} else {
		// Already logged in
		go func() {
			var err error
			for i := 1; i <= 5; i++ {
				err = client.Connect()
				if err == nil {
					fmt.Println("WhatsApp client connected!")
					return
				}
				log.Printf("Failed to connect (attempt %d/5): %v. Retrying in 5 seconds...", i, err)
				time.Sleep(5 * time.Second)
			}
			log.Printf("Failed to connect to WhatsApp after 5 attempts. Server running, but WA is offline.")
		}()
	}

	port := getEnv("PORT")
	if port == "" {
		port = "8080"
	}

	// Setup HTTP Handlers
	http.HandleFunc("/", handleRoot)
	http.HandleFunc("/ping", handlePing)
	http.HandleFunc("/qr", handleQR)
	http.HandleFunc("/send", handleSend)
	http.HandleFunc("/broadcast", handleBroadcast)
	http.HandleFunc("/logout", handleLogout)
	http.HandleFunc("/api/verify", handleVerify)
	http.HandleFunc("/api/webhook", handleWebhookApi)

	server := &http.Server{Addr: ":" + port}

	go func() {
		fmt.Println("Server running on port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	fmt.Println("Shutting down...")
	client.Disconnect()
	server.Shutdown(context.Background())
}
