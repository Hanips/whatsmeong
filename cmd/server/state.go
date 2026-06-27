package main

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
)

var client *whatsmeow.Client
var qrCode string
var qrCodeMu sync.RWMutex
var qrFlowMu sync.Mutex // Mencegah dua startQRFlow() berjalan bersamaan
var appDB *sql.DB

func startQRFlow() {
	// Guard: pastikan hanya 1 QR flow berjalan sekaligus
	if !qrFlowMu.TryLock() {
		fmt.Println("startQRFlow: already running, skipping duplicate call.")
		return
	}

	client.Disconnect()
	qrChan, _ := client.GetQRChannel(context.Background())
	err := client.Connect()
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		qrFlowMu.Unlock() // Release lock jika gagal
		return
	}
	
	// Jalankan di background
	go func() {
		// Pastikan lock selalu dilepas saat goroutine ini selesai
		defer qrFlowMu.Unlock() 
		
		for evt := range qrChan {
			if evt.Event == "code" {
				qrCodeMu.Lock()
				qrCode = evt.Code
				qrCodeMu.Unlock()
				fmt.Println("QR code available. Visit /qr to see it.")
			} else if evt.Event == "timeout" {
				fmt.Println("QR code expired (timeout). Getting a new one...")
				qrCodeMu.Lock()
				qrCode = ""
				qrCodeMu.Unlock()
				
				// Mulai ulang flow baru setelah jeda
				go func() {
					time.Sleep(2 * time.Second)
					startQRFlow()
				}()
				return // Keluar dari loop, trigger defer Unlock()
			} else if evt.Event == "success" {
				fmt.Println("Login successful!")
				qrCodeMu.Lock()
				qrCode = ""
				qrCodeMu.Unlock()
				return // Keluar dari loop, trigger defer Unlock()
			} else {
				fmt.Println("Login event:", evt.Event)
			}
		}
	}()
}
