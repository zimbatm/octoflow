// pr-monitor prints the pull request activity of one repository.
//
//	REPO_ID=4542716 go run ./examples/pr-monitor
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

// prPayload is the part of a pull request webhook payload this program reads.
type prPayload struct {
	PullRequest struct {
		Number int    `json:"number"`
		Title  string `json:"title"`
		Merged bool   `json:"merged"`
		User   struct {
			Login string `json:"login"`
		} `json:"user"`
	} `json:"pull_request"`
	Review *struct {
		State string `json:"state"`
		User  struct {
			Login string `json:"login"`
		} `json:"user"`
	} `json:"review"`
}

func main() {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	repoID := os.Getenv("REPO_ID")
	if repoID == "" {
		log.Fatal("REPO_ID is required: the numeric id of the repository")
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

	// pr.{repo}.{number} carries one copy per event; pr.{repo}.{number}.{type}
	// carries another, so pr.{repo}.> would print everything twice.
	subject := fmt.Sprintf("github.events.pr.%s.*", repoID)
	sub, err := js.Subscribe(subject, handle, nats.Durable("pr-monitor"), nats.DeliverNew())
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
	var p prPayload
	if err := json.Unmarshal(event.Payload, &p); err != nil {
		log.Printf("event %s: bad payload: %v", event.ID, err)
		_ = msg.Term()
		return
	}
	pr := p.PullRequest

	switch event.Event {
	case "pull_request":
		switch event.Action {
		case "opened":
			fmt.Printf("PR #%d opened: %s by @%s\n", pr.Number, pr.Title, pr.User.Login)
		case "closed":
			if pr.Merged {
				fmt.Printf("PR #%d merged: %s\n", pr.Number, pr.Title)
			} else {
				fmt.Printf("PR #%d closed without merge: %s\n", pr.Number, pr.Title)
			}
		default:
			fmt.Printf("PR #%d %s: %s\n", pr.Number, event.Action, pr.Title)
		}
	case "pull_request_review":
		if p.Review != nil {
			fmt.Printf("PR #%d review %s by @%s\n", pr.Number, p.Review.State, p.Review.User.Login)
		}
	case "pull_request_review_comment":
		fmt.Printf("PR #%d comment by @%s\n", pr.Number, event.Sender)
	default:
		fmt.Printf("PR #%d %s %s by @%s\n", pr.Number, event.Event, event.Action, event.Sender)
	}
	_ = msg.Ack()
}
