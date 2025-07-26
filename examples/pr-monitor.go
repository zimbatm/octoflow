package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/pkg/events"
)

// Example: Monitor pull requests for a repository
func main() {
	// Get configuration from environment or use defaults
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	repoID := os.Getenv("REPO_ID")
	if repoID == "" {
		log.Fatal("REPO_ID environment variable required")
	}

	// Connect to NATS
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("Failed to create JetStream context: %v", err)
	}

	// Subscribe to all PR events for the repository
	subject := fmt.Sprintf("github.events.pr.%s.>", repoID)
	sub, err := js.Subscribe(subject, handlePREvent, 
		nats.Durable("pr-monitor"),
		nats.DeliverNew(),
	)
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	log.Printf("Monitoring PRs for repository %s", repoID)
	log.Printf("Subscribed to: %s", subject)

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
}

func handlePREvent(msg *nats.Msg) {
	// Parse the event
	var event events.GitHubEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		log.Printf("Error parsing event: %v", err)
		msg.Ack()
		return
	}

	// Extract PR information
	pr := event.PullRequest
	if pr == nil {
		log.Printf("No pull request data in event")
		msg.Ack()
		return
	}

	// Log PR activity
	switch event.Event {
	case "pull_request":
		switch event.Action {
		case "opened":
			fmt.Printf("✨ PR #%d opened: %s by @%s\n", 
				pr.Number, pr.Title, pr.User.Login)
		case "closed":
			if pr.Merged {
				fmt.Printf("🎉 PR #%d merged: %s\n", 
					pr.Number, pr.Title)
			} else {
				fmt.Printf("❌ PR #%d closed without merge: %s\n", 
					pr.Number, pr.Title)
			}
		case "reopened":
			fmt.Printf("♻️  PR #%d reopened: %s\n", 
				pr.Number, pr.Title)
		case "synchronize":
			fmt.Printf("🔄 PR #%d updated: %s\n", 
				pr.Number, pr.Title)
		}

	case "pull_request_review":
		review := event.Review
		if review != nil {
			switch review.State {
			case "approved":
				fmt.Printf("✅ PR #%d approved by @%s\n", 
					pr.Number, review.User.Login)
			case "changes_requested":
				fmt.Printf("🔧 PR #%d changes requested by @%s\n", 
					pr.Number, review.User.Login)
			case "commented":
				fmt.Printf("💬 PR #%d reviewed by @%s\n", 
					pr.Number, review.User.Login)
			}
		}

	case "pull_request_review_comment":
		fmt.Printf("💭 PR #%d comment by @%s\n", 
			pr.Number, event.Sender.Login)

	case "issue_comment":
		// This also fires for PR comments
		if pr.Number > 0 {
			fmt.Printf("💬 PR #%d comment by @%s\n", 
				pr.Number, event.Sender.Login)
		}
	}

	// Acknowledge the message
	msg.Ack()
}