package main

import (
	"encoding/json"
	"net/http"
)

func handlePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Length", "4")
	w.Header().Set("Connection", "close")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("pong"))
}

func handleVerify(w http.ResponseWriter, r *http.Request) {
	apiKey := getEnv("API_KEY")
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
	apiKey := getEnv("API_KEY")
	if apiKey != "" && r.URL.Query().Get("key") != apiKey {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	if r.Method == "GET" {
		urls := getWebhooks()
		if urls == nil {
			urls = []string{}
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"webhook_urls": urls})
		return
	}

	if r.Method == "POST" {
		var req struct {
			WebhookURLs []string `json:"webhook_urls"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		urlsBytes, _ := json.Marshal(req.WebhookURLs)
		if appDB != nil {
			_, err := appDB.Exec("INSERT INTO wa_settings (key, value) VALUES ('webhook_urls', $1) ON CONFLICT (key) DO UPDATE SET value = $1", string(urlsBytes))
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
