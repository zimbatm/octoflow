package nats

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
	"github.com/zimbatm/octoflow/internal/natstest"
	"github.com/zimbatm/octoflow/pkg/events"
)

func testConfig() *config.NATSConfig {
	return &config.NATSConfig{
		SubjectPrefix:  "t.events",
		StreamName:     "T_EVENTS",
		StreamReplicas: 1,
		StreamMaxAge:   time.Hour,
	}
}

func TestEnsureStreamCreatesThenUpdates(t *testing.T) {
	_, js := natstest.Start(t)
	cfg := testConfig()

	if err := EnsureStream(js, cfg); err != nil {
		t.Fatal(err)
	}
	info, err := js.StreamInfo(cfg.StreamName)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != "t.events.>" {
		t.Errorf("subjects = %v, want [t.events.>]", info.Config.Subjects)
	}
	if info.Config.Duplicates != time.Hour {
		t.Errorf("duplicates = %v, want the 1h max age: the server rejects a longer window", info.Config.Duplicates)
	}
	if info.Config.MaxAge != time.Hour {
		t.Errorf("max age = %v, want 1h", info.Config.MaxAge)
	}

	cfg.StreamMaxAge = 48 * time.Hour
	if err := EnsureStream(js, cfg); err != nil {
		t.Fatal(err)
	}
	info, err = js.StreamInfo(cfg.StreamName)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.MaxAge != 48*time.Hour {
		t.Errorf("max age after update = %v, want 48h", info.Config.MaxAge)
	}
	if info.Config.Duplicates != DuplicateWindow {
		t.Errorf("duplicates after update = %v, want %v", info.Config.Duplicates, DuplicateWindow)
	}
}

func TestPublishEventDeduplicatesByID(t *testing.T) {
	_, js := natstest.Start(t)
	cfg := testConfig()
	if err := EnsureStream(js, cfg); err != nil {
		t.Fatal(err)
	}
	p := NewPublisher(js, cfg)
	ctx := context.Background()

	org := &events.GitHubEvent{ID: "d1", Event: "push", OrganizationID: "42", Payload: json.RawMessage(`{}`)}
	for i := 0; i < 2; i++ {
		if err := p.PublishEvent(ctx, org); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	global := &events.GitHubEvent{ID: "d2", Event: "sponsorship", Payload: json.RawMessage(`{}`)}
	if err := p.PublishEvent(ctx, global); err != nil {
		t.Fatal(err)
	}

	info, err := js.StreamInfo(cfg.StreamName)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 2 {
		t.Fatalf("stream holds %d messages, want 2 (second publish of d1 is a duplicate)", info.State.Msgs)
	}
	for seq, want := range map[uint64]string{1: "t.events.org.42.push", 2: "t.events.global.sponsorship"} {
		m, err := js.GetMsg(cfg.StreamName, seq)
		if err != nil {
			t.Fatal(err)
		}
		if m.Subject != want {
			t.Errorf("seq %d subject = %s, want %s", seq, m.Subject, want)
		}
	}
}

func TestPublishToSubjectStoresEachID(t *testing.T) {
	_, js := natstest.Start(t)
	cfg := testConfig()
	if err := EnsureStream(js, cfg); err != nil {
		t.Fatal(err)
	}
	p := NewPublisher(js, cfg)
	ctx := context.Background()
	data := []byte(`{"id":"d1"}`)

	for _, subject := range []string{"t.events.repo.7.push", "t.events.user.9.push"} {
		if err := p.PublishToSubject(ctx, subject, "push", data, "d1@"+subject); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.PublishToSubject(ctx, "t.events.repo.7.push", "push", data, "d1@t.events.repo.7.push"); err != nil {
		t.Fatal(err)
	}

	info, err := js.StreamInfo(cfg.StreamName)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 2 {
		t.Errorf("stream holds %d messages, want 2", info.State.Msgs)
	}
}

func TestEnsureConsumerCreatesThenUpdates(t *testing.T) {
	_, js := natstest.Start(t)
	cfg := testConfig()
	if err := EnsureStream(js, cfg); err != nil {
		t.Fatal(err)
	}
	cc := &nats.ConsumerConfig{
		Durable:        "worker",
		AckPolicy:      nats.AckExplicitPolicy,
		FilterSubjects: PrimarySubjects(cfg.SubjectPrefix),
		MaxAckPending:  10,
	}
	if err := EnsureConsumer(js, cfg.StreamName, cc); err != nil {
		t.Fatal(err)
	}
	info, err := js.ConsumerInfo(cfg.StreamName, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Config.FilterSubjects) != 2 {
		t.Errorf("filter subjects = %v, want org and global", info.Config.FilterSubjects)
	}

	cc.MaxAckPending = 5
	if err := EnsureConsumer(js, cfg.StreamName, cc); err != nil {
		t.Fatal(err)
	}
	info, err = js.ConsumerInfo(cfg.StreamName, "worker")
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.MaxAckPending != 5 {
		t.Errorf("max ack pending = %d, want 5 after update", info.Config.MaxAckPending)
	}
}

func TestEnsureConsumerWaitsForStream(t *testing.T) {
	_, js := natstest.Start(t)
	cfg := testConfig()
	go func() {
		time.Sleep(500 * time.Millisecond)
		_ = EnsureStream(js, cfg)
	}()
	cc := &nats.ConsumerConfig{Durable: "late", AckPolicy: nats.AckExplicitPolicy}
	if err := EnsureConsumer(js, cfg.StreamName, cc); err != nil {
		t.Fatal(err)
	}
}

func TestAckOrNak(t *testing.T) {
	_, js := natstest.Start(t)
	cfg := testConfig()
	if err := EnsureStream(js, cfg); err != nil {
		t.Fatal(err)
	}
	cc := &nats.ConsumerConfig{Durable: "w", AckPolicy: nats.AckExplicitPolicy, AckWait: time.Second}
	if err := EnsureConsumer(js, cfg.StreamName, cc); err != nil {
		t.Fatal(err)
	}
	sub, err := js.PullSubscribe("", "w", nats.Bind(cfg.StreamName, "w"))
	if err != nil {
		t.Fatal(err)
	}
	p := NewPublisher(js, cfg)
	ctx := context.Background()
	if err := p.PublishToSubject(ctx, "t.events.global.x", "x", []byte("a"), "a"); err != nil {
		t.Fatal(err)
	}

	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	AckOrNak(msgs[0], errors.New("not now"))

	msgs, err = sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("expected redelivery after nak: %v", err)
	}
	meta, err := msgs[0].Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if meta.NumDelivered != 2 {
		t.Errorf("num delivered = %d, want 2", meta.NumDelivered)
	}
	AckOrNak(msgs[0], nil)

	// acks are fire-and-forget; give the server a moment to apply it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		info, err := js.ConsumerInfo(cfg.StreamName, "w")
		if err != nil {
			t.Fatal(err)
		}
		if info.NumAckPending == 0 && info.NumPending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ack pending = %d, pending = %d, want 0 and 0", info.NumAckPending, info.NumPending)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
