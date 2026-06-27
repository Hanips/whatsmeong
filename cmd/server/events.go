package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
)

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
				fmt.Printf("Reconnect gagal setelah StreamReplaced: %v. Memulai ulang QR flow...\n", err)
				time.Sleep(2 * time.Second)
				startQRFlow()
				return
			}
			fmt.Println("Reconnect berhasil!")
		}()
	case events.PermanentDisconnect:
		desc := v.PermanentDisconnectDescription()
		fmt.Printf("Permanent Disconnect: %s\n", desc)
		// Untuk semua jenis permanent disconnect (termasuk LoggedOut dari HP),
		// mulai ulang QR flow tanpa crash server.
		fmt.Println("Memulai ulang QR flow di background...")
		go func() {
			time.Sleep(3 * time.Second)
			startQRFlow()
		}()
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
					urls := getWebhooks()
					if len(urls) > 0 {
						firstHash := fmt.Sprintf("%X", pollVote.GetSelectedOptions()[0])
						var optionText string
						msgId := v.Message.GetPollUpdateMessage().GetPollCreationMessageKey().GetID()
						errDB := appDB.QueryRow("SELECT option_text FROM wa_poll_options WHERE msg_id = $1 AND option_hash = $2", msgId, firstHash).Scan(&optionText)
						if errDB != nil {
							optionText = "PollVote:" + firstHash
						}

						payload := map[string]string{
							"sender":    sender,
							"name":      name,
							"message":   optionText,
							"type":      "poll_vote",
							"timestamp": time.Now().Format(time.RFC3339),
						}
						body, _ := json.Marshal(payload)

						for _, u := range urls {
							go func(hookURL string) {
								httpClient := &http.Client{Timeout: 10 * time.Second}
								httpClient.Post(hookURL, "application/json", bytes.NewBuffer(body))
							}(u)
						}
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
		go func(sender string, chat string, name string, text string) {
			urls := getWebhooks()
			if len(urls) > 0 {
				payload := map[string]string{
					"sender":    sender,
					"chat":      chat,
					"name":      name,
					"message":   text,
					"timestamp": time.Now().Format(time.RFC3339),
				}
				body, _ := json.Marshal(payload)
				for _, u := range urls {
					go func(hookURL string) {
						httpClient := &http.Client{Timeout: 10 * time.Second}
						httpClient.Post(hookURL, "application/json", bytes.NewBuffer(body))
					}(u)
				}
			}
		}(v.Info.Sender.User, v.Info.Chat.String(), v.Info.PushName, msgText)

		// Jika ada yang chat "tes"
		if strings.ToLower(strings.TrimSpace(msgText)) == "tes" {
			replyText := "✅ WA Manager Status: AKTIF & SIAP!\nServer berjalan di Render Cloud."
			replyMsg := &waE2E.Message{
				Conversation: &replyText,
			}
			client.SendMessage(context.Background(), v.Info.Chat, replyMsg)
		}
	}
}

func getWebhooks() []string {
	if appDB == nil {
		return nil
	}
	var urlsStr string
	err := appDB.QueryRow("SELECT value FROM wa_settings WHERE key = 'webhook_urls'").Scan(&urlsStr)
	if err != nil || urlsStr == "" {
		var oldUrl string
		appDB.QueryRow("SELECT value FROM wa_settings WHERE key = 'webhook_url'").Scan(&oldUrl)
		if oldUrl != "" {
			return []string{oldUrl}
		}
		return nil
	}
	var urls []string
	json.Unmarshal([]byte(urlsStr), &urls)
	return urls
}
