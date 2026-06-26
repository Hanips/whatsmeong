package main

import (
	"context"
	"bytes"
	"crypto/sha256"
	"database/sql"
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
	"sync"
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
var qrCodeMu sync.RWMutex
var appDB *sql.DB

func main() {
	dbLog := waLog.Stdout("Database", "INFO", true)

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		log.Fatal("DATABASE_URL environment variable is required (e.g. from Neon.tech)")
	}

	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		log.Fatal("API_KEY environment variable is required for security! Silakan atur di Environment Variables Render.com")
	}

	// Connect to PostgreSQL (Neon.tech or similar)
	container, err := sqlstore.New(context.Background(), "postgres", dbURL, dbLog)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	appDB, err = sql.Open("postgres", dbURL)
	if err == nil {
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
					qrCodeMu.Lock()
					qrCode = evt.Code
					qrCodeMu.Unlock()
					fmt.Println("QR code available. Visit /qr to see it or generate it from this string:", evt.Code)
				} else if evt.Event == "timeout" {
					fmt.Println("QR code expired (timeout). Restarting server to generate a new one...")
					qrCodeMu.Lock()
					qrCode = ""
					qrCodeMu.Unlock()
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

func eventHandler(evt interface{}) {
	switch v := evt.(type) {
	case *events.StreamReplaced:
		// Terjadi saat WA dibuka di perangkat/browser lain. Ini BUKAN error fatal.
		// Kita cukup reconnect agar bot tetap hidup.
		fmt.Println("Stream replaced (WA dibuka di tempat lain). Reconnecting...")
		go func() {
			time.Sleep(3 * time.Second)
			err := client.Connect()
			if err != nil {
				fmt.Printf("Reconnect gagal setelah StreamReplaced: %v. Restarting...\n", err)
				client.Disconnect()
				os.Exit(0)
			}
			fmt.Println("Reconnect berhasil!")
		}()
	case events.PermanentDisconnect:
		desc := v.PermanentDisconnectDescription()
		fmt.Printf("Permanent Disconnect: %s\n", desc)
		// Hanya benar-benar exit jika sesi logout permanen
		if _, isLoggedOut := v.(*events.LoggedOut); isLoggedOut {
			fmt.Println("Sesi WA telah logout permanen. Restarting untuk QR baru...")
			go func() {
				client.Disconnect()
				os.Exit(0)
			}()
		} else {
			// Untuk disconnect lain (misal timeout network), coba reconnect
			fmt.Println("Non-fatal disconnect, mencoba reconnect...")
			go func() {
				time.Sleep(5 * time.Second)
				err := client.Connect()
				if err != nil {
					fmt.Printf("Reconnect gagal: %v. Restarting...\n", err)
					client.Disconnect()
					os.Exit(0)
				}
				fmt.Println("Reconnect berhasil!")
			}()
		}
	case *events.Message:
		// Abaikan pesan dari diri sendiri atau pesan kosong
		if v.Info.IsFromMe || v.Message == nil {
			return
		}

		// Handle Poll Update Message
		if v.Message.GetPollUpdateMessage() != nil {
			pollVote, err := client.DecryptPollVote(context.Background(), v)
			if err == nil && len(pollVote.GetSelectedOptions()) > 0 {
				go func(sender string, name string) {
					if appDB == nil { return }
					var hookURL string
					err := appDB.QueryRow("SELECT value FROM wa_settings WHERE key = 'webhook_url'").Scan(&hookURL)
					if err == nil && hookURL != "" {
						firstHash := fmt.Sprintf("%X", pollVote.GetSelectedOptions()[0])
						var optionText string
						msgId := v.Message.GetPollUpdateMessage().GetPollCreationMessageKey().GetID()
						errDB := appDB.QueryRow("SELECT option_text FROM wa_poll_options WHERE msg_id = $1 AND option_hash = $2", msgId, firstHash).Scan(&optionText)
						if errDB != nil { optionText = "PollVote:" + firstHash }

						payload := map[string]string{
							"sender": sender,
							"name": name,
							"message": optionText,
							"type": "poll_vote",
							"timestamp": time.Now().Format(time.RFC3339),
						}
						body, _ := json.Marshal(payload)
						httpClient := &http.Client{Timeout: 10 * time.Second}
						httpClient.Post(hookURL, "application/json", bytes.NewBuffer(body))
					}
				}(v.Info.Sender.User, v.Info.PushName)
			}
			return
		}

		// Ambil teks pesan
		msgText := ""
		if v.Message.GetConversation() != "" {
			msgText = v.Message.GetConversation()
		} else if v.Message.GetExtendedTextMessage() != nil {
			msgText = v.Message.GetExtendedTextMessage().GetText()
		} else if v.Message.GetImageMessage() != nil {
			msgText = v.Message.GetImageMessage().GetCaption()
		} else if v.Message.GetDocumentMessage() != nil {
			msgText = v.Message.GetDocumentMessage().GetCaption()
		}

		// Panggil Webhook jika ada
		go func(sender string, name string, text string) {
			if appDB == nil { return }
			var hookURL string
			err := appDB.QueryRow("SELECT value FROM wa_settings WHERE key = 'webhook_url'").Scan(&hookURL)
			if err == nil && hookURL != "" {
				payload := map[string]string{
					"sender": sender,
					"name": name,
					"message": text,
					"timestamp": time.Now().Format(time.RFC3339),
				}
				body, _ := json.Marshal(payload)
				httpClient := &http.Client{Timeout: 10 * time.Second}
				httpClient.Post(hookURL, "application/json", bytes.NewBuffer(body))
			}
		}(v.Info.Sender.User, v.Info.PushName, msgText)

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
	
	status := "Menunggu Login"
	statusColor := "#ef4444" // red
	if client.Store.ID != nil {
		status = "Terhubung"
		statusColor = "#10b981" // green
	}

	htmlTemplate := `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>WA Manager Dashboard</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #f5f5f7;
            --card-bg: rgba(255, 255, 255, 0.8);
            --primary: #10b981;
            --primary-hover: #059669;
            --text-main: #1d1d1f;
            --text-muted: #86868b;
            --border: rgba(0, 0, 0, 0.08);
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: 'Inter', sans-serif;
            background-color: var(--bg-color);
            color: var(--text-main);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 2rem;
        }
        .container {
            width: 100%;
            max-width: 600px;
        }
        .card {
            background: var(--card-bg);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            border: 1px solid var(--border);
            border-radius: 1.5rem;
            padding: 2.5rem;
            box-shadow: 0 4px 24px rgba(0, 0, 0, 0.04);
        }
        .header {
            text-align: center;
            margin-bottom: 2rem;
        }
        .title {
            font-size: 2rem;
            font-weight: 700;
            color: var(--text-main);
            margin-bottom: 0.5rem;
            letter-spacing: -0.02em;
        }
        .status-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.25rem 1rem;
            border-radius: 9999px;
            font-size: 0.875rem;
            font-weight: 500;
            background: rgba(0,0,0,0.03);
            border: 1px solid var(--border);
        }
        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background-color: {{STATUS_COLOR}};
            box-shadow: 0 0 10px {{STATUS_COLOR}};
        }
        .form-group {
            margin-bottom: 1.5rem;
        }
        .label {
            display: block;
            font-size: 0.875rem;
            font-weight: 500;
            color: var(--text-muted);
            margin-bottom: 0.5rem;
        }
        .input {
            width: 100%;
            background: #ffffff;
            border: 1px solid var(--border);
            border-radius: 0.75rem;
            padding: 0.875rem 1rem;
            color: var(--text-main);
            font-size: 1rem;
            font-family: inherit;
            transition: all 0.2s;
            box-shadow: inset 0 2px 4px rgba(0,0,0,0.02);
        }
        .input:focus {
            outline: none;
            border-color: var(--primary);
            box-shadow: 0 0 0 2px rgba(16, 185, 129, 0.2);
        }
        .actions {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 1rem;
            margin-bottom: 2rem;
        }
        .btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 0.5rem;
            width: 100%;
            padding: 0.875rem 1.5rem;
            border-radius: 0.75rem;
            font-weight: 600;
            font-size: 1rem;
            cursor: pointer;
            text-decoration: none;
            transition: all 0.2s;
            border: none;
        }
        .btn-primary {
            background: var(--primary);
            color: white;
            box-shadow: 0 4px 6px -1px rgba(16, 185, 129, 0.2);
        }
        .btn-primary:hover {
            background: var(--primary-hover);
            transform: translateY(-2px);
        }
        .btn-danger {
            background: rgba(239, 68, 68, 0.05);
            color: #ef4444;
            border: 1px solid rgba(239, 68, 68, 0.1);
        }
        .btn-danger:hover {
            background: rgba(239, 68, 68, 0.1);
            transform: translateY(-2px);
        }
        .test-section {
            background: rgba(0,0,0,0.02);
            border-radius: 1rem;
            padding: 1.5rem;
            border: 1px solid var(--border);
        }
        .test-title {
            font-size: 1.125rem;
            font-weight: 600;
            margin-bottom: 1rem;
        }
        #toast {
            position: fixed;
            bottom: 2rem;
            right: 2rem;
            background: var(--primary);
            color: white;
            padding: 1rem 1.5rem;
            border-radius: 0.5rem;
            font-weight: 500;
            opacity: 0;
            transform: translateY(1rem);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            pointer-events: none;
        }
        #toast.show {
            opacity: 1;
            transform: translateY(0);
        }
    </style>
</head>
<body>
    <div class="container" id="lockScreen">
        <div class="card" style="max-width: 400px; margin: 0 auto; text-align: center;">
            <div style="display:inline-flex; align-items:center; justify-content:center; width:64px; height:64px; border-radius:50%; background:rgba(16,185,129,0.1); color:var(--primary); margin-bottom:1.5rem;">
                <svg width="32" height="32" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 15v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2zm10-10V7a4 4 0 00-8 0v4h8z"></path></svg>
            </div>
            <h1 class="title" style="font-size:1.5rem;">Akses Tertutup</h1>
            <p class="label" style="margin-bottom: 2rem;">Masukkan API Key untuk membuka WA Manager</p>
            <div class="form-group">
                <input type="password" id="lockApiKey" class="input" placeholder="API_KEY" onkeypress="if(event.key === 'Enter') verifyKey()">
            </div>
            <button class="btn btn-primary" onclick="verifyKey()" id="btnVerify">Buka Kunci</button>
        </div>
    </div>

    <div class="container" id="dashboardScreen" style="display: none; opacity: 0; transition: opacity 0.5s;">
        <div class="card">
            <div class="header">
                <h1 class="title">WA Manager</h1>
                <div class="status-badge">
                    <div class="status-dot"></div>
                    {{STATUS_TEXT}}
                </div>
            </div>

            <input type="hidden" id="apiKey">

            <div class="actions">
                <a href="/qr" id="linkQr" class="btn btn-primary">
                    <svg width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 4v1m6 11h2m-6 0h-2v4m0-11v3m0 0h.01M12 12h4.01M16 20h4M4 12h4m12 0h.01M5 8h2a1 1 0 001-1V5a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1zm12 0h2a1 1 0 001-1V5a1 1 0 00-1-1h-2a1 1 0 00-1 1v2a1 1 0 001 1zM5 20h2a1 1 0 001-1v-2a1 1 0 00-1-1H5a1 1 0 00-1 1v2a1 1 0 001 1z"></path></svg>
                    Buka QR (Login)
                </a>
                <a href="/logout" id="linkLogout" class="btn btn-danger" onclick="return confirm('Yakin ingin logout dari sesi ini?')">
                    <svg width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"></path></svg>
                    Logout Sesi
                </a>
            </div>

            <div class="test-section" style="margin-bottom: 2rem;">
                <h2 class="test-title">Pengaturan Webhook</h2>
                <div class="form-group">
                    <input type="text" id="webhookUrl" class="input" placeholder="https://domain.com/webhook (Kosongkan untuk off)">
                </div>
                <button class="btn btn-primary" onclick="saveWebhook()" id="saveHookBtn">
                    Simpan Webhook
                </button>
            </div>

            <div class="test-section">
                <h2 class="test-title">Uji Coba Pengiriman API</h2>
                <div class="form-group">
                    <input type="text" id="testPhone" class="input" placeholder="Nomor Tujuan (misal: 62857...)">
                </div>
                <div class="form-group" style="margin-bottom: 1rem;">
                    <input type="text" id="testMessage" class="input" placeholder="Tuliskan pesan percobaan...">
                </div>
                <button class="btn btn-primary" onclick="sendMessage()" id="sendBtn">
                    <svg width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8"></path></svg>
                    Kirim Pesan
                </button>
            </div>
        </div>
    </div>
    <div id="toast">Notifikasi</div>

    <script>
        async function verifyKey() {
            const key = document.getElementById('lockApiKey').value;
            const btn = document.getElementById('btnVerify');
            btn.innerHTML = 'Memverifikasi...';
            try {
                const res = await fetch('/api/verify?key=' + encodeURIComponent(key));
                if (res.ok) {
                    document.getElementById('apiKey').value = key;
                    const param = key ? '?key=' + encodeURIComponent(key) : '';
                    document.getElementById('linkQr').href = '/qr' + param;
                    document.getElementById('linkLogout').href = '/logout' + param;
                    
                    document.getElementById('lockScreen').style.display = 'none';
                    const db = document.getElementById('dashboardScreen');
                    db.style.display = 'block';
                    setTimeout(() => db.style.opacity = '1', 50);

                    // Fetch current webhook
                    const hookRes = await fetch('/api/webhook?key=' + encodeURIComponent(key));
                    if (hookRes.ok) {
                        const hookData = await hookRes.json();
                        document.getElementById('webhookUrl').value = hookData.webhook_url || '';
                    }
                } else {
                    showToast('API Key Salah!', true);
                }
            } catch (e) {
                showToast('Koneksi Error', true);
            }
            btn.innerHTML = 'Buka Kunci';
        }

        async function saveWebhook() {
            const key = document.getElementById('apiKey').value;
            const url = document.getElementById('webhookUrl').value;
            const btn = document.getElementById('saveHookBtn');
            btn.innerHTML = 'Menyimpan...';
            try {
                const res = await fetch('/api/webhook?key=' + encodeURIComponent(key), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ webhook_url: url })
                });
                if (res.ok) showToast('Webhook berhasil disimpan!');
                else showToast('Gagal menyimpan webhook', true);
            } catch (e) {
                showToast('Error', true);
            }
            btn.innerHTML = 'Simpan Webhook';
        }

        function showToast(msg, isError = false) {
            const toast = document.getElementById('toast');
            toast.textContent = msg;
            toast.style.background = isError ? '#ef4444' : '#10b981';
            toast.classList.add('show');
            setTimeout(() => toast.classList.remove('show'), 3000);
        }

        async function sendMessage() {
            const key = document.getElementById('apiKey').value;
            const phone = document.getElementById('testPhone').value;
            const message = document.getElementById('testMessage').value;
            const btn = document.getElementById('sendBtn');

            if (!phone || !message) {
                showToast('Nomor HP dan pesan wajib diisi!', true);
                return;
            }

            btn.disabled = true;
            btn.innerHTML = 'Mengirim...';

            try {
                let headers = { 'Content-Type': 'application/json' };
                if (key) {
                    headers['Authorization'] = 'Bearer ' + key;
                }

                const response = await fetch('/send', {
                    method: 'POST',
                    headers: headers,
                    body: JSON.stringify({ phone, message })
                });
                
                let data;
                const text = await response.text();
                try { data = JSON.parse(text); } catch(e) { data = { message: text } }
                
                if (response.ok) {
                    showToast('Sukses! Pesan terkirim.');
                    document.getElementById('testMessage').value = '';
                } else {
                    showToast('Gagal: ' + (data.message || response.statusText), true);
                }
            } catch (err) {
                showToast('Terjadi kesalahan koneksi', true);
            } finally {
                btn.disabled = false;
                btn.innerHTML = '<svg width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 19l9 2-9-18-9 18 9-2zm0 0v-8"></path></svg> Kirim Pesan';
            }
        }
    </script>
</body>
</html>`

	html := strings.ReplaceAll(htmlTemplate, "{{STATUS_COLOR}}", statusColor)
	html = strings.ReplaceAll(html, "{{STATUS_TEXT}}", status)

	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(html))
}

func handleLogout(w http.ResponseWriter, r *http.Request) {
	apiKey := os.Getenv("API_KEY")
	if apiKey != "" && r.URL.Query().Get("key") != apiKey {
		http.Error(w, "Unauthorized. Akses ditolak! Gunakan URL: /logout?key=API_KEY_ANDA", http.StatusUnauthorized)
		return
	}

	htmlTemplate := `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Logout</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #f5f5f7;
            --card-bg: rgba(255, 255, 255, 0.8);
            --primary: #10b981;
            --text-main: #1d1d1f;
            --text-muted: #86868b;
            --border: rgba(0, 0, 0, 0.08);
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: 'Inter', sans-serif;
            background-color: var(--bg-color);
            color: var(--text-main);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 2rem;
        }
        .card {
            background: var(--card-bg);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            border: 1px solid var(--border);
            border-radius: 1.5rem;
            padding: 3rem;
            box-shadow: 0 4px 24px rgba(0, 0, 0, 0.04);
            text-align: center;
            max-width: 400px;
            width: 100%;
        }
        .icon {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 64px;
            height: 64px;
            border-radius: 50%;
            background: rgba(16, 185, 129, 0.1);
            color: var(--primary);
            margin-bottom: 1.5rem;
        }
        .title { font-size: 1.5rem; font-weight: 600; margin-bottom: 0.5rem; letter-spacing: -0.02em; }
        .subtitle { color: var(--text-muted); font-size: 0.875rem; margin-bottom: 2rem; line-height: 1.5; }
        .btn-primary {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 100%;
            padding: 0.875rem 1.5rem;
            border-radius: 0.75rem;
            background: var(--primary);
            color: white;
            text-decoration: none;
            font-weight: 600;
            transition: all 0.2s;
        }
        .btn-primary:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(16, 185, 129, 0.2); }
    </style>
</head>
<body>
    <div class="card">
        <div class="icon">
            <svg width="32" height="32" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path></svg>
        </div>
        <h1 class="title">{{TITLE}}</h1>
        <p class="subtitle">{{MESSAGE}}</p>
        <a href="/" class="btn-primary">Kembali ke Dashboard</a>
    </div>
    <script>
        setTimeout(() => { window.location.href = "/qr"; }, 10000);
    </script>
</body>
</html>`

	var title, message string
	if client.Store.ID == nil {
		title = "Sesi Kosong"
		message = "Tidak ada perangkat yang terhubung. Server sedang me-restart untuk menyiapkan QR Code baru... (Otomatis beralih ke halaman QR dalam 10 detik)"
		html := strings.ReplaceAll(htmlTemplate, "{{TITLE}}", title)
		html = strings.ReplaceAll(html, "{{MESSAGE}}", message)
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(html))
		go func() {
			client.Disconnect()
			os.Exit(0)
		}()
		return
	}

	err := client.Logout(context.Background())
	if err != nil {
		title = "Gagal Logout"
		message = fmt.Sprintf("Terjadi kesalahan: %v", err)
	} else {
		title = "Berhasil Logout"
		message = "Sesi perangkat Anda telah dihapus. Server sedang me-restart untuk menyiapkan QR Code baru... (Otomatis beralih ke halaman QR dalam 10 detik)"
	}
	html := strings.ReplaceAll(htmlTemplate, "{{TITLE}}", title)
	html = strings.ReplaceAll(html, "{{MESSAGE}}", message)

	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(html))

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
		html := `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sudah Login</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root { --bg-color: #f5f5f7; --card-bg: rgba(255, 255, 255, 0.8); --primary: #10b981; --text-main: #1d1d1f; --text-muted: #86868b; --border: rgba(0, 0, 0, 0.08); }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: 'Inter', sans-serif; background-color: var(--bg-color); color: var(--text-main); min-height: 100vh; display: flex; align-items: center; justify-content: center; padding: 2rem; }
        .card { background: var(--card-bg); backdrop-filter: blur(20px); border: 1px solid var(--border); border-radius: 1.5rem; padding: 3rem; box-shadow: 0 4px 24px rgba(0, 0, 0, 0.04); text-align: center; max-width: 400px; width: 100%; }
        .icon { display: inline-flex; align-items: center; justify-content: center; width: 64px; height: 64px; border-radius: 50%; background: rgba(16, 185, 129, 0.1); color: var(--primary); margin-bottom: 1.5rem; }
        .title { font-size: 1.5rem; font-weight: 600; margin-bottom: 0.5rem; letter-spacing: -0.02em; }
        .subtitle { color: var(--text-muted); font-size: 0.875rem; margin-bottom: 2rem; line-height: 1.5; }
        .btn-primary { display: inline-flex; align-items: center; justify-content: center; width: 100%; padding: 0.875rem 1.5rem; border-radius: 0.75rem; background: var(--primary); color: white; text-decoration: none; font-weight: 600; transition: all 0.2s; }
        .btn-primary:hover { transform: translateY(-2px); box-shadow: 0 4px 12px rgba(16, 185, 129, 0.2); }
    </style>
</head>
<body>
    <div class="card">
        <div class="icon">
            <svg width="32" height="32" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
        </div>
        <h1 class="title">Sudah Terhubung</h1>
        <p class="subtitle">Perangkat WhatsApp Anda sudah login dan aktif. Anda tidak perlu memindai QR Code lagi.</p>
        <a href="/" class="btn-primary">Kembali ke Dashboard</a>
    </div>
</body>
</html>`
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(html))
		return
	}
	
	qrCodeMu.RLock()
	currentQR := qrCode
	qrCodeMu.RUnlock()
	
	if currentQR == "" {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("QR code not generated yet, please refresh in a few seconds..."))
		return
	}

	// Simply redirect to a public QR code generator to show the QR easily
	qrURL := fmt.Sprintf("https://api.qrserver.com/v1/create-qr-code/?size=300x300&color=1d1d1f&bgcolor=ffffff&data=%s", url.QueryEscape(currentQR))
	htmlTemplate := `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>WhatsApp Login QR</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #f5f5f7;
            --card-bg: rgba(255, 255, 255, 0.8);
            --primary: #10b981;
            --text-main: #1d1d1f;
            --text-muted: #86868b;
            --border: rgba(0, 0, 0, 0.08);
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: 'Inter', sans-serif;
            background-color: var(--bg-color);
            color: var(--text-main);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 2rem;
        }
        .card {
            background: var(--card-bg);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            border: 1px solid var(--border);
            border-radius: 1.5rem;
            padding: 3rem;
            box-shadow: 0 4px 24px rgba(0, 0, 0, 0.04);
            text-align: center;
            max-width: 400px;
            width: 100%;
        }
        .title {
            font-size: 1.5rem;
            font-weight: 600;
            margin-bottom: 0.5rem;
            letter-spacing: -0.02em;
        }
        .subtitle {
            color: var(--text-muted);
            font-size: 0.875rem;
            margin-bottom: 2rem;
            line-height: 1.5;
        }
        .qr-wrapper {
            background: #ffffff;
            padding: 1.5rem;
            border-radius: 1rem;
            display: inline-block;
            margin-bottom: 2rem;
            position: relative;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05);
            border: 1px solid var(--border);
        }
        .qr-wrapper img {
            display: block;
            width: 250px;
            height: 250px;
        }
        .loader {
            position: absolute;
            top: 0; left: 0; right: 0; bottom: 0;
            border-radius: 1rem;
            border: 4px solid transparent;
            border-top-color: var(--primary);
            animation: spin 2s linear infinite;
            pointer-events: none;
        }
        @keyframes spin { 100% { transform: rotate(360deg); } }
        .footer {
            font-size: 0.875rem;
            color: var(--text-muted);
        }
        .btn-back {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            gap: 0.5rem;
            color: var(--primary);
            text-decoration: none;
            font-weight: 500;
            margin-top: 1.5rem;
            transition: opacity 0.2s;
        }
        .btn-back:hover { opacity: 0.8; }
    </style>
</head>
<body>
    <div class="card">
        <h1 class="title">Tautkan Perangkat</h1>
        <p class="subtitle">Buka WhatsApp di HP Anda, ketuk Menu (⋮) atau Pengaturan, lalu pilih <b>Perangkat Tertaut</b> dan scan QR Code di bawah.</p>
        
        <div class="qr-wrapper">
            <div class="loader"></div>
            <img src="{{QR_URL}}" alt="WhatsApp QR Code">
        </div>

        <p class="footer">Memperbarui otomatis dalam <span id="countdown" style="font-weight: bold; color: var(--text-main);">10</span> detik...</p>
        <a href="/" class="btn-back">
            <svg width="20" height="20" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18"></path></svg>
            Kembali ke Dashboard
        </a>
    </div>

    <script>
        let count = 10;
        setInterval(() => {
            count--;
            if(count > 0) document.getElementById('countdown').innerText = count;
            else location.reload();
        }, 1000);
    </script>
</body>
</html>`

	html := strings.ReplaceAll(htmlTemplate, "{{QR_URL}}", qrURL)

	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(html))
}

type SendRequest struct {
	Phone          string  `json:"phone"`
	Message        string  `json:"message"`
	ImageURL       string  `json:"image_url"`
	ImageBase64    string  `json:"image_base64"`
	DocumentURL    string  `json:"document_url"`
	DocumentBase64 string  `json:"document_base64"`
	FileName       string  `json:"file_name"`
	ContactName    string  `json:"contact_name"`
	ContactVcard   string   `json:"contact_vcard"`
	LocationLat    float64  `json:"location_lat"`
	LocationLng    float64  `json:"location_lng"`
	LocationName   string   `json:"location_name"`
	PollName       string   `json:"poll_name"`
	PollOptions    []string `json:"poll_options"`
	PollSelectable int      `json:"poll_selectable"`
}

type BroadcastRequest struct {
	Phones  []string    `json:"phones"`
	Payload SendRequest `json:"payload"`
	DelayMs int         `json:"delay_ms"`
}

func sendInternal(req SendRequest) (whatsmeow.SendResponse, error) {
	if client.Store.ID == nil {
		return whatsmeow.SendResponse{}, fmt.Errorf("WhatsApp client not logged in")
	}

	var targetJID types.JID
	if strings.Contains(req.Phone, "@g.us") || strings.Contains(req.Phone, "-") {
		// Group
		cleanPhone := strings.ReplaceAll(req.Phone, "@g.us", "")
		targetJID = types.NewJID(cleanPhone, types.GroupServer)
	} else {
		// User
		cleanPhone := strings.ReplaceAll(req.Phone, "+", "")
		cleanPhone = strings.ReplaceAll(cleanPhone, " ", "")
		cleanPhone = strings.ReplaceAll(cleanPhone, "-", "")
		targetJID = types.NewJID(cleanPhone, types.DefaultUserServer)
	}

	var mediaBytes []byte
	var isDocument bool

	if req.DocumentURL != "" || req.DocumentBase64 != "" {
		isDocument = true
		if req.DocumentURL != "" {
			httpClient := &http.Client{Timeout: 60 * time.Second}
			mediaReq, err := http.NewRequest("GET", req.DocumentURL, nil)
			if err != nil { return whatsmeow.SendResponse{}, fmt.Errorf("Invalid document URL: %v", err) }
			mediaReq.Header.Set("User-Agent", "Mozilla/5.0")
			mediaResp, err := httpClient.Do(mediaReq)
			if err != nil { return whatsmeow.SendResponse{}, fmt.Errorf("Failed to download document: %v", err) }
			defer mediaResp.Body.Close()
			if mediaResp.StatusCode != 200 { return whatsmeow.SendResponse{}, fmt.Errorf("Document URL returned %d", mediaResp.StatusCode) }
			mediaBytes, err = io.ReadAll(io.LimitReader(mediaResp.Body, 50*1024*1024))
			if err != nil { return whatsmeow.SendResponse{}, err }
		} else {
			var err error
			mediaBytes, err = base64.StdEncoding.DecodeString(req.DocumentBase64)
			if err != nil { return whatsmeow.SendResponse{}, err }
		}
	} else if req.ImageURL != "" || req.ImageBase64 != "" {
		if req.ImageURL != "" {
			httpClient := &http.Client{Timeout: 30 * time.Second}
			mediaReq, err := http.NewRequest("GET", req.ImageURL, nil)
			if err != nil { return whatsmeow.SendResponse{}, fmt.Errorf("Invalid image URL: %v", err) }
			mediaReq.Header.Set("User-Agent", "Mozilla/5.0")
			mediaResp, err := httpClient.Do(mediaReq)
			if err != nil { return whatsmeow.SendResponse{}, fmt.Errorf("Failed to download image: %v", err) }
			defer mediaResp.Body.Close()
			if mediaResp.StatusCode != 200 { return whatsmeow.SendResponse{}, fmt.Errorf("Image URL returned %d", mediaResp.StatusCode) }
			mediaBytes, err = io.ReadAll(io.LimitReader(mediaResp.Body, 15*1024*1024))
			if err != nil { return whatsmeow.SendResponse{}, err }
		} else {
			var err error
			mediaBytes, err = base64.StdEncoding.DecodeString(req.ImageBase64)
			if err != nil { return whatsmeow.SendResponse{}, err }
		}
	}

	var msg *waE2E.Message
	if req.PollName != "" && len(req.PollOptions) > 0 {
		selectable := req.PollSelectable
		if selectable <= 0 { selectable = 1 }
		msg = client.BuildPollCreation(req.PollName, req.PollOptions, selectable)
	} else {
		msg = &waE2E.Message{}
	}

	if msg.PollCreationMessage == nil {
		if len(mediaBytes) > 0 {
			mimeType := http.DetectContentType(mediaBytes)
			if isDocument {
				uploaded, err := client.Upload(context.Background(), mediaBytes, whatsmeow.MediaDocument)
				if err != nil { return whatsmeow.SendResponse{}, err }
				fileName := req.FileName
				if fileName == "" { fileName = "document.pdf" }
				msg.DocumentMessage = &waE2E.DocumentMessage{
					Caption: proto.String(req.Message), Mimetype: proto.String(mimeType),
					FileName: proto.String(fileName), URL: &uploaded.URL,
					DirectPath: &uploaded.DirectPath, MediaKey: uploaded.MediaKey,
					FileEncSHA256: uploaded.FileEncSHA256, FileSHA256: uploaded.FileSHA256,
					FileLength: proto.Uint64(uploaded.FileLength),
				}
			} else {
				uploaded, err := client.Upload(context.Background(), mediaBytes, whatsmeow.MediaImage)
				if err != nil { return whatsmeow.SendResponse{}, err }
				msg.ImageMessage = &waE2E.ImageMessage{
					Caption: proto.String(req.Message), Mimetype: proto.String(mimeType),
					URL: &uploaded.URL, DirectPath: &uploaded.DirectPath,
					MediaKey: uploaded.MediaKey, FileEncSHA256: uploaded.FileEncSHA256,
					FileSHA256: uploaded.FileSHA256, FileLength: proto.Uint64(uploaded.FileLength),
				}
			}
		} else if req.ContactVcard != "" {
			msg.ContactMessage = &waE2E.ContactMessage{
				DisplayName: proto.String(req.ContactName),
				Vcard:       proto.String(req.ContactVcard),
			}
		} else if req.LocationLat != 0 && req.LocationLng != 0 {
			msg.LocationMessage = &waE2E.LocationMessage{
				DegreesLatitude:  proto.Float64(req.LocationLat),
				DegreesLongitude: proto.Float64(req.LocationLng),
				Name:             proto.String(req.LocationName),
				Address:          proto.String(req.Message),
			}
		} else {
			msg.ExtendedTextMessage = &waE2E.ExtendedTextMessage{
				Text: &req.Message,
			}
		}
	}



	resp, err := client.SendMessage(context.Background(), targetJID, msg)
	if err == nil && req.PollName != "" && appDB != nil {
		for _, opt := range req.PollOptions {
			h := sha256.Sum256([]byte(opt))
			hashHex := fmt.Sprintf("%X", h)
			appDB.Exec("INSERT INTO wa_poll_options (msg_id, option_hash, option_text) VALUES ($1, $2, $3)", resp.ID, hashHex, opt)
		}
	}
	return resp, err
}

func handleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	apiKey := os.Getenv("API_KEY")
	if apiKey != "" && r.Header.Get("Authorization") != "Bearer "+apiKey {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req SendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	if req.Phone == "" {
		http.Error(w, "Phone is required", http.StatusBadRequest)
		return
	}

	resp, err := sendInternal(req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success", "messageId": resp.ID, "timestamp": fmt.Sprintf("%v", resp.Timestamp),
	})
}

func handleBroadcast(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	apiKey := os.Getenv("API_KEY")
	if apiKey != "" && r.Header.Get("Authorization") != "Bearer "+apiKey {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req BroadcastRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	if len(req.Phones) == 0 {
		http.Error(w, "No phones provided", http.StatusBadRequest)
		return
	}

	go func(phones []string, payload SendRequest, delay int) {
		for _, phone := range phones {
			payload.Phone = phone
			sendInternal(payload)
			if delay > 0 {
				time.Sleep(time.Duration(delay) * time.Millisecond)
			}
		}
	}(req.Phones, req.Payload, req.DelayMs)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "processing",
		"message": fmt.Sprintf("Broadcasting to %d numbers in background", len(req.Phones)),
	})
}

func handleVerify(w http.ResponseWriter, r *http.Request) {
	apiKey := os.Getenv("API_KEY")
	reqKey := r.URL.Query().Get("key")
	w.Header().Set("Content-Type", "application/json")
	if apiKey == "" || reqKey == apiKey {
		json.NewEncoder(w).Encode(map[string]bool{"valid": true})
	} else {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]bool{"valid": false})
	}
}

func handleWebhookApi(w http.ResponseWriter, r *http.Request) {
	apiKey := os.Getenv("API_KEY")
	if apiKey != "" && r.URL.Query().Get("key") != apiKey {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if r.Method == "GET" {
		var hookURL string
		if appDB != nil {
			appDB.QueryRow("SELECT value FROM wa_settings WHERE key = 'webhook_url'").Scan(&hookURL)
		}
		json.NewEncoder(w).Encode(map[string]string{"webhook_url": hookURL})
		return
	}

	if r.Method == "POST" {
		var req struct {
			WebhookURL string `json:"webhook_url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if appDB != nil {
			_, err := appDB.Exec("INSERT INTO wa_settings (key, value) VALUES ('webhook_url', $1) ON CONFLICT (key) DO UPDATE SET value = $1", req.WebhookURL)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
		return
	}
	
	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}
