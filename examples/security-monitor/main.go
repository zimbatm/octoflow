// security-monitor prints the events of one organization that change who
// can see or change what.
//
//	ORG_ID=487568 go run ./examples/security-monitor
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

type alert struct {
	severity, what string
}

// payload is the part of a webhook payload this program reads.
type payload struct {
	Member struct {
		Login string `json:"login"`
	} `json:"member"`
	Team struct {
		Name string `json:"name"`
	} `json:"team"`
}

func main() {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	orgID := os.Getenv("ORG_ID")
	if orgID == "" {
		log.Fatal("ORG_ID is required: the numeric id of the organization")
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	subject := fmt.Sprintf("github.events.org.%s.>", orgID)
	sub, err := js.Subscribe(subject, handle, nats.Durable("security-monitor"), nats.DeliverNew())
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	log.Printf("watching %s", subject)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}

func handle(msg *nats.Msg) {
	var event events.GitHubEvent
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		log.Printf("not an event: %v", err)
		_ = msg.Term()
		return
	}
	var p payload
	_ = json.Unmarshal(event.Payload, &p)

	if a := classify(&event, &p); a != nil {
		fmt.Printf("%s %s %s.%s by @%s %s\n",
			event.ReceivedAt.Format("2006-01-02 15:04:05"), a.severity, event.Event, event.Action, event.Sender, a.what)
	}
	_ = msg.Ack()
}

func classify(e *events.GitHubEvent, p *payload) *alert {
	switch e.Event {
	case "repository":
		switch e.Action {
		case "publicized":
			return &alert{"HIGH", "repository made public: " + e.Repository}
		case "transferred", "deleted", "archived":
			return &alert{"MEDIUM", "repository " + e.Action + ": " + e.Repository}
		}
	case "member":
		if e.Action == "added" || e.Action == "removed" {
			return &alert{"MEDIUM", "collaborator @" + p.Member.Login + " " + e.Action + " on " + e.Repository}
		}
	case "organization":
		return &alert{"HIGH", "organization membership change"}
	case "team":
		if e.Action == "added_to_repository" || e.Action == "removed_from_repository" {
			return &alert{"MEDIUM", "team " + p.Team.Name + " " + e.Action + " " + e.Repository}
		}
	case "security_advisory", "secret_scanning_alert":
		return &alert{"CRITICAL", e.Repository}
	case "code_scanning_alert":
		if e.Action == "created" || e.Action == "reopened" {
			return &alert{"HIGH", e.Repository}
		}
	case "dependabot_alert":
		if e.Action == "created" {
			return &alert{"MEDIUM", e.Repository}
		}
	}
	return nil
}
