package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	_ "github.com/lib/pq"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

var client *whatsmeow.Client
var qrCode string

func main() {
	dbLog := waLog.Stdout("Database", "INFO", true)

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL environment variable is required (e.g. from Neon.tech)")
	}

	// Connect to PostgreSQL (Neon.tech or similar)
	container, err := sqlstore.New(context.Background(), "postgres", dbURL, dbLog)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Get the first linked device
	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		log.Fatalf("Failed to get device: %v", err)
	}

	clientLog := waLog.Stdout("Client", "INFO", true)
	client = whatsmeow.NewClient(deviceStore, clientLog)
	client.AddEventHandler(eventHandler)

	if client.Store.ID == nil {
		// No ID stored, need to link device
		qrChan, _ := client.GetQRChannel(context.Background())
		err = client.Connect()
		if err != nil {
			log.Fatalf("Failed to connect: %v", err)
		}
		go func() {
			for evt := range qrChan {
				if evt.Event == "code" {
					qrCode = evt.Code
					fmt.Println("QR code available. Visit /qr to see it or generate it from this string:", evt.Code)
				} else {
					fmt.Println("Login event:", evt.Event)
				}
			}
		}()
	} else {
		// Already logged in
		err = client.Connect()
		if err != nil {
			log.Fatalf("Failed to connect: %v", err)
		}
		fmt.Println("WhatsApp client connected!")
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Setup HTTP Handlers
	http.HandleFunc("/", handleRoot)
	http.HandleFunc("/ping", handlePing)
	http.HandleFunc("/qr", handleQR)
	http.HandleFunc("/send", handleSend)
	http.HandleFunc("/logout", handleLogout)

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

func eventHandler(evt interface{}) {
	switch v := evt.(type) {
	case *events.Message:
		// Abaikan pesan dari diri sendiri
		if v.Info.IsFromMe {
			return
		}

		// Ambil teks pesan
		msgText := ""
		if v.Message.GetConversation() != "" {
			msgText = v.Message.GetConversation()
		} else if v.Message.GetExtendedTextMessage() != nil {
			msgText = v.Message.GetExtendedTextMessage().GetText()
		}

		// Jika ada yang chat "tes"
		if strings.ToLower(strings.TrimSpace(msgText)) == "tes" {
			replyText := "✅ WA Manager Status: AKTIF & SIAP!\nServer berjalan di Render Cloud."
			replyMsg := &waE2E.Message{
				Conversation: &replyText,
			}
			// Kirim balasan
			client.SendMessage(context.Background(), v.Info.Chat, replyMsg)
		}
	}
}

func handlePing(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("pong"))
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(`<h1>WA Manager Aktif</h1><ul><li><a href="/qr">/qr (Login)</a></li><li>/logout?key=API_KEY_ANDA</li></ul>`))
}

func handleLogout(w http.ResponseWriter, r *http.Request) {
	apiKey := os.Getenv("API_KEY")
	if apiKey != "" && r.URL.Query().Get("key") != apiKey {
		http.Error(w, "Unauthorized. Akses ditolak! Gunakan URL: /logout?key=API_KEY_ANDA", http.StatusUnauthorized)
		return
	}

	if client.Store.ID == nil {
		w.Write([]byte("Belum ada akun WhatsApp yang login."))
		return
	}

	err := client.Logout(context.Background())
	if err != nil {
		w.Write([]byte(fmt.Sprintf("Gagal logout: %v", err)))
		return
	}

	w.Write([]byte("✅ Berhasil Logout! WhatsApp lama sudah diputus. Silakan buka /qr untuk menautkan nomor WA yang baru."))
}

func handleQR(w http.ResponseWriter, r *http.Request) {
	if client.Store.ID != nil {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Already logged in!"))
		return
	}
	if qrCode == "" {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("QR code not generated yet, please refresh in a few seconds..."))
		return
	}

	// Simply redirect to a public QR code generator to show the QR easily
	qrURL := fmt.Sprintf("https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=%s", url.QueryEscape(qrCode))
	html := fmt.Sprintf(`
		<html>
		<head><title>WhatsApp Login</title></head>
		<body>
			<h2>Scan this QR Code with your WhatsApp</h2>
			<img src="%s" alt="QR Code">
			<p>String: %s</p>
			<script>
				setTimeout(function(){ location.reload(); }, 10000); // refresh every 10s
			</script>
		</body>
		</html>
	`, qrURL, qrCode)

	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(html))
}

type SendRequest struct {
	Phone   string `json:"phone"`
	Message string `json:"message"`
}

func handleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	apiKey := os.Getenv("API_KEY")
	if apiKey != "" {
		authHeader := r.Header.Get("Authorization")
		if authHeader != "Bearer "+apiKey {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	var req SendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	if req.Phone == "" || req.Message == "" {
		http.Error(w, "Phone and message are required", http.StatusBadRequest)
		return
	}

	if client.Store.ID == nil {
		http.Error(w, "WhatsApp client not logged in", http.StatusServiceUnavailable)
		return
	}

	// Parse JID (e.g. 628123456789)
	targetJID := types.NewJID(req.Phone, types.DefaultUserServer)

	// Build the message
	msg := &waE2E.Message{
		Conversation: &req.Message,
	}

	resp, err := client.SendMessage(context.Background(), targetJID, msg)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to send message: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":    "success",
		"messageId": resp.ID,
		"timestamp": fmt.Sprintf("%v", resp.Timestamp),
	})
}
