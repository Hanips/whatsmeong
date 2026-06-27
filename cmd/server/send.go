package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// SendRequest adalah struktur untuk request pengiriman pesan tunggal.
type SendRequest struct {
	Phone          string   `json:"phone"`
	Message        string   `json:"message"`
	ImageURL       string   `json:"image_url"`
	ImageBase64    string   `json:"image_base64"`
	DocumentURL    string   `json:"document_url"`
	DocumentBase64 string   `json:"document_base64"`
	FileName       string   `json:"file_name"`
	ContactName    string   `json:"contact_name"`
	ContactVcard   string   `json:"contact_vcard"`
	LocationLat    float64  `json:"location_lat"`
	LocationLng    float64  `json:"location_lng"`
	LocationName   string   `json:"location_name"`
	PollName       string   `json:"poll_name"`
	PollOptions    []string `json:"poll_options"`
	PollSelectable int      `json:"poll_selectable"`
}

// BroadcastRequest adalah struktur untuk request pengiriman ke banyak nomor.
type BroadcastRequest struct {
	Phones     []string    `json:"phones"`
	Payload    SendRequest `json:"payload"`
	DelayMs    int         `json:"delay_ms"`
	DelayMsMax int         `json:"delay_ms_max"`
}

// sendInternal adalah mesin pengirim utama untuk semua jenis pesan.
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
			if err != nil {
				return whatsmeow.SendResponse{}, fmt.Errorf("Invalid document URL: %v", err)
			}
			mediaReq.Header.Set("User-Agent", "Mozilla/5.0")
			mediaResp, err := httpClient.Do(mediaReq)
			if err != nil {
				return whatsmeow.SendResponse{}, fmt.Errorf("Failed to download document: %v", err)
			}
			defer mediaResp.Body.Close()
			if mediaResp.StatusCode != 200 {
				return whatsmeow.SendResponse{}, fmt.Errorf("Document URL returned %d", mediaResp.StatusCode)
			}
			mediaBytes, err = io.ReadAll(io.LimitReader(mediaResp.Body, 50*1024*1024))
			if err != nil {
				return whatsmeow.SendResponse{}, err
			}
		} else {
			var err error
			mediaBytes, err = base64.StdEncoding.DecodeString(req.DocumentBase64)
			if err != nil {
				return whatsmeow.SendResponse{}, err
			}
		}
	} else if req.ImageURL != "" || req.ImageBase64 != "" {
		if req.ImageURL != "" {
			httpClient := &http.Client{Timeout: 30 * time.Second}
			mediaReq, err := http.NewRequest("GET", req.ImageURL, nil)
			if err != nil {
				return whatsmeow.SendResponse{}, fmt.Errorf("Invalid image URL: %v", err)
			}
			mediaReq.Header.Set("User-Agent", "Mozilla/5.0")
			mediaResp, err := httpClient.Do(mediaReq)
			if err != nil {
				return whatsmeow.SendResponse{}, fmt.Errorf("Failed to download image: %v", err)
			}
			defer mediaResp.Body.Close()
			if mediaResp.StatusCode != 200 {
				return whatsmeow.SendResponse{}, fmt.Errorf("Image URL returned %d", mediaResp.StatusCode)
			}
			mediaBytes, err = io.ReadAll(io.LimitReader(mediaResp.Body, 15*1024*1024))
			if err != nil {
				return whatsmeow.SendResponse{}, err
			}
		} else {
			var err error
			mediaBytes, err = base64.StdEncoding.DecodeString(req.ImageBase64)
			if err != nil {
				return whatsmeow.SendResponse{}, err
			}
		}
	}

	var msg *waE2E.Message
	if req.PollName != "" && len(req.PollOptions) > 0 {
		selectable := req.PollSelectable
		if selectable <= 0 {
			selectable = 1
		}
		msg = client.BuildPollCreation(req.PollName, req.PollOptions, selectable)
	} else {
		msg = &waE2E.Message{}
	}

	if msg.PollCreationMessage == nil {
		if len(mediaBytes) > 0 {
			mimeType := http.DetectContentType(mediaBytes)
			if isDocument {
				uploaded, err := client.Upload(context.Background(), mediaBytes, whatsmeow.MediaDocument)
				if err != nil {
					return whatsmeow.SendResponse{}, err
				}
				fileName := req.FileName
				if fileName == "" {
					fileName = "document.pdf"
				}
				msg.DocumentMessage = &waE2E.DocumentMessage{
					Caption: proto.String(req.Message), Mimetype: proto.String(mimeType),
					FileName: proto.String(fileName), URL: &uploaded.URL,
					DirectPath: &uploaded.DirectPath, MediaKey: uploaded.MediaKey,
					FileEncSHA256: uploaded.FileEncSHA256, FileSHA256: uploaded.FileSHA256,
					FileLength: proto.Uint64(uploaded.FileLength),
				}
			} else {
				uploaded, err := client.Upload(context.Background(), mediaBytes, whatsmeow.MediaImage)
				if err != nil {
					return whatsmeow.SendResponse{}, err
				}
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

	apiKey := getEnv("API_KEY")
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

	apiKey := getEnv("API_KEY")
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

	go func(phones []string, payload SendRequest, delay int, delayMax int) {
		for _, phone := range phones {
			payload.Phone = phone
			sendInternal(payload)

			if delayMax > delay+1 {
				sleepTime := delay + rand.Intn(delayMax-delay)
				time.Sleep(time.Duration(sleepTime) * time.Millisecond)
			} else if delay > 0 {
				time.Sleep(time.Duration(delay) * time.Millisecond)
			}
		}
	}(req.Phones, req.Payload, req.DelayMs, req.DelayMsMax)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "processing",
		"message": fmt.Sprintf("Broadcasting to %d numbers in background", len(req.Phones)),
	})
}
