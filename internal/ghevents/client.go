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

// RateLimitError means GitHub refused the request until Reset.
type RateLimitError struct {
	Reset time.Time
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("github rate limit exceeded until %s", e.Reset.UTC().Format(time.RFC3339))
}

// Quota is what the last response said about the remaining rate limit.
// Remaining is -1 when the response carried no quota headers.
type Quota struct {
	Remaining int
	Reset     time.Time
}

func quotaOf(resp *http.Response) Quota {
	q := Quota{Remaining: -1}
	if n, err := strconv.Atoi(resp.Header.Get("X-RateLimit-Remaining")); err == nil {
		q.Remaining = n
	}
	if s, err := strconv.ParseInt(resp.Header.Get("X-RateLimit-Reset"), 10, 64); err == nil {
		q.Reset = time.Unix(s, 0)
	}
	return q
}

// rateLimited recognises GitHub's two ways of saying "too many requests":
// 429, or 403 with the quota at zero.
func rateLimited(resp *http.Response, q Quota) error {
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
	case resp.StatusCode == http.StatusForbidden && q.Remaining == 0:
	default:
		return nil
	}
	reset := q.Reset
	if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
		reset = time.Now().Add(time.Duration(s) * time.Second)
	}
	if reset.IsZero() {
		reset = time.Now().Add(time.Minute)
	}
	return &RateLimitError{Reset: reset}
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
	q := quotaOf(resp)
	if err := rateLimited(resp, q); err != nil {
		return nil, err
	}
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
	Quota        Quota
}

// RepoEvents fetches one page. A 304 for a matching etag does not count
// against the rate limit. A refused request is a *RateLimitError.
func (c *Client) RepoEvents(ctx context.Context, fullName string, page int, etag string) (*EventsPage, error) {
	path := fmt.Sprintf("/repos/%s/events?per_page=100&page=%d", fullName, page)
	resp, err := c.get(ctx, path, etag)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	q := quotaOf(resp)
	if err := rateLimited(resp, q); err != nil {
		return nil, err
	}

	out := &EventsPage{ETag: resp.Header.Get("ETag"), Quota: q}
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
