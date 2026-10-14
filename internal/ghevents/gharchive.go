package ghevents

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ErrHourNotFound means GH Archive has no file for the hour, either not
// yet published or one of its known gaps.
var ErrHourNotFound = errors.New("gharchive: hour not found")

// HourURL returns the GH Archive file for the hour that starts at t.
// The hour has no leading zero: 2026-10-14-9.json.gz.
func HourURL(base string, t time.Time) string {
	t = t.UTC()
	return fmt.Sprintf("%s/%s-%d.json.gz", base, t.Format("2006-01-02"), t.Hour())
}

// ReadHour streams one hourly file and calls fn for each event of a
// watched repository. The files hold every public event on GitHub, so the
// filter runs before the full decode.
func ReadHour(ctx context.Context, hc *http.Client, url string, repoIDs map[int64]bool, fn func(*Event) error) (seen int, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, ErrHourNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("GET %s: %s", url, resp.Status)
	}

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return 0, err
	}
	defer gz.Close()
	return scanEvents(gz, repoIDs, fn)
}

func scanEvents(r io.Reader, repoIDs map[int64]bool, fn func(*Event) error) (int, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	var head struct {
		Repo Repo `json:"repo"`
	}
	n := 0
	for sc.Scan() {
		line := sc.Bytes()
		head.Repo = Repo{}
		if err := json.Unmarshal(line, &head); err != nil || !repoIDs[head.Repo.ID] {
			continue
		}
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			continue
		}
		n++
		if err := fn(&e); err != nil {
			return n, err
		}
	}
	return n, sc.Err()
}
