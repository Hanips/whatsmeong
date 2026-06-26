package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
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
				} else if evt.Event == "timeout" {
					fmt.Println("QR code expired (timeout). Restarting server to generate a new one...")
					qrCode = ""
					go func() {
						client.Disconnect()
						os.Exit(0)
					}()
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
	case events.PermanentDisconnect:
		fmt.Printf("Fatal Error (Permanent Disconnect): %s. Menghentikan dan me-restart server...\n", v.PermanentDisconnectDescription())
		go func() {
			client.Disconnect()
			os.Exit(0)
		}()
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
		w.Write([]byte("Sesi kosong. Server sedang memulai ulang untuk menyiapkan QR Code baru... (Refresh halaman /qr dalam 10 detik)."))
		go func() {
			client.Disconnect()
			os.Exit(0)
		}()
		return
	}

	err := client.Logout(context.Background())
	if err != nil {
		w.Write([]byte(fmt.Sprintf("Gagal logout: %v", err)))
		return
	}

	w.Write([]byte("✅ Berhasil Logout! Server sedang memulai ulang untuk menyiapkan QR Code baru... (Refresh halaman /qr dalam 10 detik)."))

	// Restart server secara paksa agar whatsmeow membuat jalur QR Code baru
	go func() {
		client.Disconnect()
		os.Exit(0)
	}()
}

func handleQR(w http.ResponseWriter, r *http.Request) {
	apiKey := os.Getenv("API_KEY")
	if apiKey != "" && r.URL.Query().Get("key") != apiKey {
		http.Error(w, "Unauthorized. Akses ditolak! Gunakan URL: /qr?key=API_KEY_ANDA", http.StatusUnauthorized)
		return
	}

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
	Phone       string `json:"phone"`
	Message     string `json:"message"`
	ImageURL    string `json:"image_url"`
	ImageBase64 string `json:"image_base64"`
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

	if req.Phone == "" || (req.Message == "" && req.ImageURL == "" && req.ImageBase64 == "") {
		http.Error(w, "Phone and (message or image) are required", http.StatusBadRequest)
		return
	}

	if client.Store.ID == nil {
		http.Error(w, "WhatsApp client not logged in", http.StatusServiceUnavailable)
		return
	}

	// Parse JID (e.g. 628123456789)
	targetJID := types.NewJID(req.Phone, types.DefaultUserServer)

	// Proses Gambar jika ada
	var imageBytes []byte
	var mimeType string
	if req.ImageURL != "" {
		// Gunakan custom HTTP client dengan timeout
		httpClient := &http.Client{Timeout: 30 * time.Second}
		imgReq, err := http.NewRequest("GET", req.ImageURL, nil)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid image URL: %v", err), http.StatusBadRequest)
			return
		}
		// Tambahkan User-Agent agar tidak diblokir oleh sistem keamanan web (seperti Wikimedia/Cloudflare)
		imgReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
		
		imgResp, err := httpClient.Do(imgReq)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to download image URL: %v", err), http.StatusBadRequest)
			return
		}
		defer imgResp.Body.Close()

		if imgResp.StatusCode != 200 {
			http.Error(w, fmt.Sprintf("Image URL returned status code %d", imgResp.StatusCode), http.StatusBadRequest)
			return
		}
		
		// Batasi maksimal 15MB untuk mencegah server crash (OOM)
		imageBytes, err = io.ReadAll(io.LimitReader(imgResp.Body, 15*1024*1024))
		if err != nil {
			http.Error(w, "Failed to read image", http.StatusInternalServerError)
			return
		}
	} else if req.ImageBase64 != "" {
		imageBytes, _ = base64.StdEncoding.DecodeString(req.ImageBase64)
	}

	// Build the message
	msg := &waE2E.Message{}

	if len(imageBytes) > 0 {
		mimeType = http.DetectContentType(imageBytes)
		if !strings.HasPrefix(mimeType, "image/") {
			http.Error(w, fmt.Sprintf("Invalid image file. Detected format: %s", mimeType), http.StatusBadRequest)
			return
		}

		// Upload gambar ke server WhatsApp
		uploaded, err := client.Upload(context.Background(), imageBytes, whatsmeow.MediaImage)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to upload image to WhatsApp: %v", err), http.StatusInternalServerError)
			return
		}

		msg.ImageMessage = &waE2E.ImageMessage{
			Caption:       proto.String(req.Message),
			Mimetype:      proto.String(mimeType),
			URL:           &uploaded.URL,
			DirectPath:    &uploaded.DirectPath,
			MediaKey:      uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256,
			FileSHA256:    uploaded.FileSHA256,
			FileLength:    proto.Uint64(uploaded.FileLength),
		}
	} else {
		// Pesan Teks Biasa
		msg.ExtendedTextMessage = &waE2E.ExtendedTextMessage{
			Text: &req.Message,
		}
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
