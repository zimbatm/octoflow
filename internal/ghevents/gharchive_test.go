package ghevents

import (
	"strings"
	"testing"
	"time"
)

func TestHourURL(t *testing.T) {
	got := HourURL("https://data.gharchive.org", time.Date(2026, 10, 14, 9, 30, 0, 0, time.UTC))
	if want := "https://data.gharchive.org/2026-10-14-9.json.gz"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestScanEventsFilters(t *testing.T) {
	in := strings.Join([]string{
		`{"id":"1","type":"PushEvent","repo":{"id":4542716,"name":"NixOS/nixpkgs"},"payload":{}}`,
		`{"id":"2","type":"PushEvent","repo":{"id":7,"name":"other/repo"},"payload":{}}`,
		`not json`,
		`{"id":"3","type":"IssuesEvent","repo":{"id":4542716,"name":"NixOS/nixpkgs"},"payload":{"action":"opened"}}`,
	}, "\n")

	var ids []string
	n, err := scanEvents(strings.NewReader(in), map[int64]bool{4542716: true}, func(e *Event) error {
		ids = append(ids, e.ID)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 || strings.Join(ids, ",") != "1,3" {
		t.Errorf("got n=%d ids=%v", n, ids)
	}
}
