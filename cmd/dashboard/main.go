// dashboard serves the static page in web/. The page talks to the SSE
// server directly; see web/index.html for how it finds it.
package main

import (
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func main() {
	webDir := os.Getenv("WEB_DIR")
	if webDir == "" {
		webDir = "./web"
	}
	absWebDir, err := filepath.Abs(webDir)
	if err != nil {
		log.Fatalf("Failed to resolve %s: %v", webDir, err)
	}
	if _, err := os.Stat(filepath.Join(absWebDir, "index.html")); err != nil {
		log.Fatalf("WEB_DIR must hold index.html: %v", err)
	}

	addr := os.Getenv("DASHBOARD_LISTEN_ADDR")
	if addr == "" {
		addr = ":8082"
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           http.FileServer(http.Dir(absWebDir)),
		ReadHeaderTimeout: 5 * time.Second,
	}
	slog.Info("Starting dashboard", "address", addr, "web_dir", absWebDir)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
