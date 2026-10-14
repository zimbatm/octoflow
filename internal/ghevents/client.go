package ghevents

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Client reads the GitHub REST API. A token is optional; without one the
// rate limit is 60 requests per hour.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

func NewClient(token string) *Client {
	return &Client{
		BaseURL: "https://api.github.com",
		Token:   token,
		HTTP:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) get(ctx context.Context, path, etag string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	return c.HTTP.Do(req)
}

// RepoInfo is what the converter needs to know about a watched repository.
type RepoInfo struct {
	ID       int64   `json:"id"`
	FullName string  `json:"full_name"`
	Owner    Account `json:"owner"`
}

func (c *Client) Repo(ctx context.Context, fullName string) (*RepoInfo, error) {
	resp, err := c.get(ctx, "/repos/"+fullName, "")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET /repos/%s: %s", fullName, resp.Status)
	}
	var info RepoInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, err
	}
	return &info, nil
}

// EventsPage is one page of GET /repos/{repo}/events.
type EventsPage struct {
	Events       []Event
	ETag         string
	NotModified  bool
	PollInterval time.Duration
}

// RepoEvents fetches one page. A 304 for a matching etag does not count
// against the rate limit.
func (c *Client) RepoEvents(ctx context.Context, fullName string, page int, etag string) (*EventsPage, error) {
	path := fmt.Sprintf("/repos/%s/events?per_page=100&page=%d", fullName, page)
	resp, err := c.get(ctx, path, etag)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	out := &EventsPage{ETag: resp.Header.Get("ETag")}
	if s, err := strconv.Atoi(resp.Header.Get("X-Poll-Interval")); err == nil {
		out.PollInterval = time.Duration(s) * time.Second
	}
	switch resp.StatusCode {
	case http.StatusNotModified:
		out.NotModified = true
		out.ETag = etag
		return out, nil
	case http.StatusOK:
	default:
		return nil, fmt.Errorf("GET %s: %s", path, resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(&out.Events); err != nil {
		return nil, err
	}
	return out, nil
}
