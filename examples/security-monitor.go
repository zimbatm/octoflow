//go:build ignore

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/pkg/events"
)

// Example: Monitor security-relevant events across the organization
func main() {
	// Get configuration
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	orgID := os.Getenv("ORG_ID")
	if orgID == "" {
		log.Fatal("ORG_ID environment variable required")
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

	// Subscribe to all organization events
	subject := fmt.Sprintf("github.events.org.%s.>", orgID)
	sub, err := js.Subscribe(subject, handleSecurityEvent,
		nats.Durable("security-monitor"),
		nats.DeliverNew(),
	)
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	log.Printf("Security monitor started for organization %s", orgID)
	log.Printf("Watching for security-relevant events...")

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
}

func handleSecurityEvent(msg *nats.Msg) {
	// Parse event
	var event events.GitHubEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		log.Printf("Error parsing event: %v", err)
		msg.Ack()
		return
	}

	// Check for security-relevant events
	alert := checkSecurityEvent(&event)
	if alert != nil {
		logSecurityAlert(alert)
		// In production, you would send this to your alerting system
	}

	msg.Ack()
}

type SecurityAlert struct {
	Severity    string
	Event       string
	Action      string
	Actor       string
	Repository  string
	Description string
	Timestamp   time.Time
}

func checkSecurityEvent(event *events.GitHubEvent) *SecurityAlert {
	switch event.Event {
	case "repository":
		return checkRepositoryEvent(event)
	case "member":
		return checkMemberEvent(event)
	case "team":
		return checkTeamEvent(event)
	case "organization":
		return checkOrgEvent(event)
	case "security_advisory":
		return checkSecurityAdvisory(event)
	case "secret_scanning_alert":
		return checkSecretScanning(event)
	case "code_scanning_alert":
		return checkCodeScanning(event)
	case "dependabot_alert":
		return checkDependabot(event)
	}

	// Check for suspicious patterns in any event
	if isSuspiciousActivity(event) {
		return &SecurityAlert{
			Severity:    "LOW",
			Event:       event.Event,
			Action:      event.Action,
			Actor:       event.Sender.Login,
			Repository:  getRepoName(event),
			Description: "Unusual activity pattern detected",
			Timestamp:   event.ReceivedAt,
		}
	}

	return nil
}

func checkRepositoryEvent(event *events.GitHubEvent) *SecurityAlert {
	repo := event.Repository
	if repo == nil {
		return nil
	}

	switch event.Action {
	case "publicized":
		return &SecurityAlert{
			Severity:    "HIGH",
			Event:       event.Event,
			Action:      event.Action,
			Actor:       event.Sender.Login,
			Repository:  repo.FullName,
			Description: fmt.Sprintf("Repository made PUBLIC: %s", repo.FullName),
			Timestamp:   event.ReceivedAt,
		}

	case "archived":
		return &SecurityAlert{
			Severity:    "LOW",
			Event:       event.Event,
			Action:      event.Action,
			Actor:       event.Sender.Login,
			Repository:  repo.FullName,
			Description: fmt.Sprintf("Repository archived: %s", repo.FullName),
			Timestamp:   event.ReceivedAt,
		}

	case "transferred":
		return &SecurityAlert{
			Severity:    "MEDIUM",
			Event:       event.Event,
			Action:      event.Action,
			Actor:       event.Sender.Login,
			Repository:  repo.FullName,
			Description: "Repository ownership transferred",
			Timestamp:   event.ReceivedAt,
		}
	}

	return nil
}

func checkMemberEvent(event *events.GitHubEvent) *SecurityAlert {
	switch event.Action {
	case "added":
		member := event.Member
		if member != nil && member.SiteAdmin {
			return &SecurityAlert{
				Severity:    "CRITICAL",
				Event:       event.Event,
				Action:      event.Action,
				Actor:       event.Sender.Login,
				Repository:  getRepoName(event),
				Description: fmt.Sprintf("Site admin added: @%s", member.Login),
				Timestamp:   event.ReceivedAt,
			}
		}
		return &SecurityAlert{
			Severity:    "MEDIUM",
			Event:       event.Event,
			Action:      event.Action,
			Actor:       event.Sender.Login,
			Repository:  getRepoName(event),
			Description: fmt.Sprintf("Member added: @%s", event.Member.Login),
			Timestamp:   event.ReceivedAt,
		}

	case "removed":
		return &SecurityAlert{
			Severity:    "LOW",
			Event:       event.Event,
			Action:      event.Action,
			Actor:       event.Sender.Login,
			Repository:  getRepoName(event),
			Description: fmt.Sprintf("Member removed: @%s", event.Member.Login),
			Timestamp:   event.ReceivedAt,
		}
	}

	return nil
}

func checkTeamEvent(event *events.GitHubEvent) *SecurityAlert {
	if event.Action == "added_to_repository" || event.Action == "removed_from_repository" {
		return &SecurityAlert{
			Severity:    "MEDIUM",
			Event:       event.Event,
			Action:      event.Action,
			Actor:       event.Sender.Login,
			Repository:  getRepoName(event),
			Description: fmt.Sprintf("Team permissions changed: %s", event.Action),
			Timestamp:   event.ReceivedAt,
		}
	}
	return nil
}

func checkOrgEvent(event *events.GitHubEvent) *SecurityAlert {
	// All organization-level changes are important
	return &SecurityAlert{
		Severity:    "HIGH",
		Event:       event.Event,
		Action:      event.Action,
		Actor:       event.Sender.Login,
		Repository:  "",
		Description: fmt.Sprintf("Organization setting changed: %s", event.Action),
		Timestamp:   event.ReceivedAt,
	}
}

func checkSecurityAdvisory(event *events.GitHubEvent) *SecurityAlert {
	return &SecurityAlert{
		Severity:    "CRITICAL",
		Event:       event.Event,
		Action:      event.Action,
		Actor:       event.Sender.Login,
		Repository:  getRepoName(event),
		Description: fmt.Sprintf("Security advisory %s", event.Action),
		Timestamp:   event.ReceivedAt,
	}
}

func checkSecretScanning(event *events.GitHubEvent) *SecurityAlert {
	return &SecurityAlert{
		Severity:    "CRITICAL",
		Event:       event.Event,
		Action:      event.Action,
		Actor:       event.Sender.Login,
		Repository:  getRepoName(event),
		Description: "Secret detected in repository",
		Timestamp:   event.ReceivedAt,
	}
}

func checkCodeScanning(event *events.GitHubEvent) *SecurityAlert {
	if event.Action == "created" || event.Action == "reopened" {
		return &SecurityAlert{
			Severity:    "HIGH",
			Event:       event.Event,
			Action:      event.Action,
			Actor:       event.Sender.Login,
			Repository:  getRepoName(event),
			Description: "Code scanning alert",
			Timestamp:   event.ReceivedAt,
		}
	}
	return nil
}

func checkDependabot(event *events.GitHubEvent) *SecurityAlert {
	if event.Action == "created" {
		return &SecurityAlert{
			Severity:    "MEDIUM",
			Event:       event.Event,
			Action:      event.Action,
			Actor:       "dependabot",
			Repository:  getRepoName(event),
			Description: "Vulnerable dependency detected",
			Timestamp:   event.ReceivedAt,
		}
	}
	return nil
}

func isSuspiciousActivity(event *events.GitHubEvent) bool {
	// Check for rapid-fire events from same user
	// In production, you'd track this across multiple events

	// Check for events at unusual hours
	hour := event.ReceivedAt.Hour()
	if hour >= 2 && hour <= 5 {
		// Activity between 2-5 AM might be suspicious
		return true
	}

	// Check for bot-like patterns
	if strings.Contains(event.Sender.Login, "bot") {
		return false // Bots are expected
	}

	return false
}

func getRepoName(event *events.GitHubEvent) string {
	if event.Repository != nil {
		return event.Repository.FullName
	}
	return ""
}

func logSecurityAlert(alert *SecurityAlert) {
	// Color code by severity
	var prefix string
	switch alert.Severity {
	case "CRITICAL":
		prefix = "🚨 CRITICAL"
	case "HIGH":
		prefix = "⚠️  HIGH"
	case "MEDIUM":
		prefix = "⚡ MEDIUM"
	case "LOW":
		prefix = "ℹ️  LOW"
	}

	fmt.Printf("\n%s Security Alert\n", prefix)
	fmt.Printf("Time: %s\n", alert.Timestamp.Format(time.RFC3339))
	fmt.Printf("Event: %s.%s\n", alert.Event, alert.Action)
	fmt.Printf("Actor: @%s\n", alert.Actor)
	if alert.Repository != "" {
		fmt.Printf("Repository: %s\n", alert.Repository)
	}
	fmt.Printf("Description: %s\n", alert.Description)
	fmt.Println(strings.Repeat("-", 60))
}
