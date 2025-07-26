package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func main() {
	// Determine the web directory path
	webDir := os.Getenv("WEB_DIR")
	if webDir == "" {
		// Default to ./web relative to current directory
		webDir = "./web"
	}

	// Ensure web directory exists
	if _, err := os.Stat(webDir); os.IsNotExist(err) {
		log.Fatalf("Web directory does not exist: %s", webDir)
	}

	// Convert to absolute path for clarity
	absWebDir, err := filepath.Abs(webDir)
	if err != nil {
		log.Fatalf("Failed to get absolute path: %v", err)
	}

	// Serve static files from web directory
	fs := http.FileServer(http.Dir(absWebDir))
	http.Handle("/", fs)

	port := ":8082"
	fmt.Printf("Dashboard server starting on http://localhost%s\n", port)
	fmt.Printf("Serving files from: %s\n", absWebDir)
	fmt.Println("Make sure SSE server is running on :8081")

	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal(err)
	}
}
