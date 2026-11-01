package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// id is a GitHub id as a string, whether the payload carries it as a number
// or as a string.
type id string

func (i *id) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	// GitHub ids are integers; anything else is kept out of subjects and labels
	for _, c := range s {
		if c < '0' || c > '9' {
			s = ""
			break
		}
	}
	*i = id(s)
	return nil
}

type account struct {
	ID    id     `json:"id"`
	Login string `json:"login"`
}

// payload is the part of a GitHub payload that routing reads.
type payload struct {
	Action     string `json:"action"`
	Repository *struct {
		ID       id       `json:"id"`
		FullName string   `json:"full_name"`
		Owner    *account `json:"owner"`
	} `json:"repository"`
	Organization *account `json:"organization"`
	Sender       *account `json:"sender"`
	PullRequest  *struct {
		Number             json.Number `json:"number"`
		User               *account    `json:"user"`
		Assignees          []account   `json:"assignees"`
		RequestedReviewers []account   `json:"requested_reviewers"`
	} `json:"pull_request"`
	Issue *struct {
		Number    json.Number `json:"number"`
		User      *account    `json:"user"`
		Assignees []account   `json:"assignees"`
	} `json:"issue"`
	Comment *struct {
		User *account `json:"user"`
	} `json:"comment"`
	Review *struct {
		User *account `json:"user"`
	} `json:"review"`
	Team *struct {
		Members []account `json:"members"`
	} `json:"team"`
}

// decode is lenient about shape: a field of an unexpected type is skipped
// and the rest is kept, as encoding/json does. Only invalid JSON is an error.
func decode(raw json.RawMessage) (*payload, error) {
	var p payload
	err := json.Unmarshal(raw, &p)
	var typeErr *json.UnmarshalTypeError
	if err != nil && !errors.As(err, &typeErr) {
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}
	return &p, nil
}

// ParseCommonFields fills the routing fields of event from its payload.
// The organization is the payload's organization, or else the repository
// owner, which is what webhooks on user-owned repositories carry.
func ParseCommonFields(event *GitHubEvent) error {
	p, err := decode(event.Payload)
	if err != nil {
		return err
	}
	event.Action = p.Action
	if r := p.Repository; r != nil {
		event.Repository = r.FullName
		event.RepositoryID = string(r.ID)
		if r.Owner != nil {
			event.Organization = r.Owner.Login
			event.OrganizationID = string(r.Owner.ID)
		}
	}
	if s := p.Sender; s != nil {
		event.Sender = s.Login
		event.SenderID = string(s.ID)
	}
	if o := p.Organization; o != nil {
		event.Organization = o.Login
		event.OrganizationID = string(o.ID)
	}
	return nil
}

// ExtractEventEntities lists what the dispatcher routes an event to: its
// repository, every involved user (sender, author, assignees, requested
// reviewers, comment and review authors, team members) and its pull request
// or issue number.
func ExtractEventEntities(event *GitHubEvent) (*EventEntities, error) {
	p, err := decode(event.Payload)
	if err != nil {
		return nil, err
	}
	e := &EventEntities{RepositoryID: event.RepositoryID, UserIDs: []string{}}

	addUser := func(a *account) {
		if a == nil || a.ID == "" {
			return
		}
		if !slices.Contains(e.UserIDs, string(a.ID)) {
			e.UserIDs = append(e.UserIDs, string(a.ID))
		}
	}
	addUsers := func(as []account) {
		for i := range as {
			addUser(&as[i])
		}
	}

	if event.SenderID != "" {
		e.UserIDs = append(e.UserIDs, event.SenderID)
	}
	switch event.Event {
	case "pull_request", "pull_request_review", "pull_request_review_comment":
		if pr := p.PullRequest; pr != nil {
			e.PRNumber = toInt(pr.Number)
			addUser(pr.User)
			addUsers(pr.Assignees)
			addUsers(pr.RequestedReviewers)
		}
	case "issues", "issue_comment":
		if is := p.Issue; is != nil {
			e.IssueNumber = toInt(is.Number)
			addUser(is.User)
			addUsers(is.Assignees)
		}
	case "team_add", "team":
		if p.Team != nil {
			addUsers(p.Team.Members)
		}
	}
	if p.Comment != nil {
		addUser(p.Comment.User)
	}
	if p.Review != nil {
		addUser(p.Review.User)
	}
	return e, nil
}

func toInt(n json.Number) int {
	i, err := n.Int64()
	if err != nil {
		return 0
	}
	return int(i)
}
