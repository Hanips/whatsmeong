package main

import (
	"context"
	"embed"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

//go:embed templates/*
var templateFiles embed.FS

// loadTemplate membaca file HTML dari embed filesystem.
func loadTemplate(name string) string {
	data, err := templateFiles.ReadFile("templates/" + name)
	if err != nil {
		return "<h1>Template not found: " + name + "</h1>"
	}
	return string(data)
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

	html := loadTemplate("dashboard.html")
	html = strings.ReplaceAll(html, "{{STATUS_COLOR}}", statusColor)
	html = strings.ReplaceAll(html, "{{STATUS_TEXT}}", status)

	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(html))
}

func handleLogout(w http.ResponseWriter, r *http.Request) {
	apiKey := getEnv("API_KEY")
	if apiKey != "" && r.URL.Query().Get("key") != apiKey {
		http.Error(w, "Unauthorized. Akses ditolak! Gunakan URL: /logout?key=API_KEY_ANDA", http.StatusUnauthorized)
		return
	}

	// Buat URL redirect /qr dengan API key agar tidak kena 401
	qrRedirectURL := "/qr"
	if apiKey != "" {
		qrRedirectURL = "/qr?key=" + url.QueryEscape(apiKey)
	}

	htmlTemplate := loadTemplate("logout.html")
	htmlTemplate = strings.ReplaceAll(htmlTemplate, "{{QR_REDIRECT_URL}}", qrRedirectURL)

	var title, message string
	if client.Store.ID == nil {
		title = "Sesi Kosong"
		message = "Tidak ada perangkat yang terhubung. Otomatis beralih ke halaman QR dalam 15 detik."
		html := strings.ReplaceAll(htmlTemplate, "{{TITLE}}", title)
		html = strings.ReplaceAll(html, "{{MESSAGE}}", message)
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(html))
		// Tidak perlu startQRFlow() di sini, QR flow sudah berjalan sendiri
		return
	}

	err := client.Logout(context.Background())
	if err != nil {
		title = "Gagal Logout"
		message = fmt.Sprintf("Terjadi kesalahan: %v", err)
	} else {
		title = "Berhasil Logout"
		message = "Sesi perangkat Anda telah dihapus. Otomatis beralih ke halaman QR dalam 15 detik."
	}
	html := strings.ReplaceAll(htmlTemplate, "{{TITLE}}", title)
	html = strings.ReplaceAll(html, "{{MESSAGE}}", message)

	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(html))

	// Mulai ulang jalur QR Code baru di background tanpa crash server
	go func() {
		time.Sleep(2 * time.Second)
		startQRFlow()
	}()
}

func handleQR(w http.ResponseWriter, r *http.Request) {
	apiKey := getEnv("API_KEY")
	if apiKey != "" && r.URL.Query().Get("key") != apiKey {
		http.Error(w, "Unauthorized. Akses ditolak! Gunakan URL: /qr?key=API_KEY_ANDA", http.StatusUnauthorized)
		return
	}

	if client.Store.ID != nil {
		html := loadTemplate("qr_loggedin.html")
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

	qrURL := fmt.Sprintf("https://api.qrserver.com/v1/create-qr-code/?size=300x300&color=1d1d1f&bgcolor=ffffff&data=%s", url.QueryEscape(currentQR))
	html := strings.ReplaceAll(loadTemplate("qr.html"), "{{QR_URL}}", qrURL)

	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(html))
}
