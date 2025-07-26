package events

import (
	"testing"
	"time"
)

func TestEventReorderBuffer(t *testing.T) {
	buffer := NewEventReorderBuffer(5 * time.Second)

	now := time.Now()

	// Create events with different timestamps
	event1 := &GitHubEvent{
		ID:              "1",
		Event:           "push",
		GithubTimestamp: now,
	}

	event2 := &GitHubEvent{
		ID:              "2",
		Event:           "pull_request",
		GithubTimestamp: now.Add(-2 * time.Second),
	}

	event3 := &GitHubEvent{
		ID:              "3",
		Event:           "issues",
		GithubTimestamp: now.Add(-1 * time.Second),
	}

	// Add events out of order
	buffer.Add(event1)
	buffer.Add(event3)
	buffer.Add(event2)

	// Should not flush yet (within window)
	if buffer.ShouldFlush() {
		t.Error("Buffer should not flush within time window")
	}

	// Flush and check order
	events := buffer.Flush()
	if len(events) != 3 {
		t.Errorf("Expected 3 events, got %d", len(events))
	}

	// Verify events are in chronological order
	if events[0].ID != "2" {
		t.Errorf("Expected first event ID to be 2, got %s", events[0].ID)
	}
	if events[1].ID != "3" {
		t.Errorf("Expected second event ID to be 3, got %s", events[1].ID)
	}
	if events[2].ID != "1" {
		t.Errorf("Expected third event ID to be 1, got %s", events[2].ID)
	}
}

func TestEventReorderBufferTimeWindow(t *testing.T) {
	buffer := NewEventReorderBuffer(100 * time.Millisecond)

	now := time.Now()

	event1 := &GitHubEvent{
		ID:              "1",
		Event:           "push",
		GithubTimestamp: now.Add(-200 * time.Millisecond),
	}

	event2 := &GitHubEvent{
		ID:              "2",
		Event:           "pull_request",
		GithubTimestamp: now,
	}

	buffer.Add(event1)

	// Wait for window to expire
	time.Sleep(150 * time.Millisecond)

	// Should flush now
	if !buffer.ShouldFlush() {
		t.Error("Buffer should flush after time window")
	}

	// Flush first batch
	events1 := buffer.Flush()

	// Add new event after flush
	buffer.Add(event2)

	// Flush second batch
	events2 := buffer.Flush()

	if len(events1) != 1 || events1[0].ID != "1" {
		t.Error("First flush should contain event 1")
	}

	if len(events2) != 1 || events2[0].ID != "2" {
		t.Error("Second flush should contain event 2")
	}
}

func TestEventReorderBufferEmpty(t *testing.T) {
	buffer := NewEventReorderBuffer(5 * time.Second)

	// Empty buffer should not indicate flush needed
	if buffer.ShouldFlush() {
		t.Error("Empty buffer should not need flush")
	}

	// Flushing empty buffer should return empty slice
	events := buffer.Flush()
	if len(events) != 0 {
		t.Errorf("Expected empty slice, got %d events", len(events))
	}
}
