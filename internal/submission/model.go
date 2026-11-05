package submission

import (
	"context"
	"encoding/json"
	"time"
)

// Submission is a stored form submission.
type Submission struct {
	ID             int64           `json:"id"`
	FormID         string          `json:"form_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Email          string          `json:"email"`
	Data           json.RawMessage `json:"data"`
	CreatedAt      time.Time       `json:"created_at"`
}

// CreateRequest is the POST /submissions body.
type CreateRequest struct {
	FormID         string          `json:"form_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Email          string          `json:"email"`
	Data           json.RawMessage `json:"data"`
}

// Page is one page of submissions for a form.
type Page struct {
	Items      []Submission `json:"data"`
	Page       int          `json:"page"`
	PerPage    int          `json:"per_page"`
	Total      int          `json:"total"`
	TotalPages int          `json:"total_pages"`
}

// Store persists submissions. The Postgres implementation lives in
// repository.go; tests use an in-memory fake.
type Store interface {
	// Create inserts the submission, or returns the existing one if the
	// idempotency key was already used. created reports which happened.
	Create(ctx context.Context, req CreateRequest) (s Submission, created bool, err error)
	ListByForm(ctx context.Context, formID string, limit, offset int) ([]Submission, int, error)
}
