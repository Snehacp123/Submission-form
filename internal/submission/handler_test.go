package submission

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeStore mimics the Postgres semantics: unique idempotency keys.
type fakeStore struct {
	mu    sync.Mutex
	byKey map[string]Submission
	rows  []Submission
}

func newFakeStore() *fakeStore { return &fakeStore{byKey: map[string]Submission{}} }

func (f *fakeStore) Create(_ context.Context, req CreateRequest) (Submission, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.byKey[req.IdempotencyKey]; ok {
		return s, false, nil
	}
	s := Submission{
		ID: int64(len(f.rows) + 1), FormID: req.FormID, IdempotencyKey: req.IdempotencyKey,
		Email: req.Email, Data: req.Data, CreatedAt: time.Now(),
	}
	f.byKey[req.IdempotencyKey] = s
	f.rows = append(f.rows, s)
	return s, true, nil
}

func (f *fakeStore) ListByForm(_ context.Context, formID string, limit, offset int) ([]Submission, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var all []Submission
	for _, s := range f.rows {
		if s.FormID == formID {
			all = append(all, s)
		}
	}
	end := min(offset+limit, len(all))
	if offset > len(all) {
		offset = len(all)
	}
	return all[offset:end], len(all), nil
}

func newServer(store Store) *httptest.Server {
	mux := http.NewServeMux()
	NewHandler(store, slog.New(slog.NewTextHandler(io.Discard, nil))).Register(mux)
	return httptest.NewServer(mux)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url+"/submissions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestCreateValidation(t *testing.T) {
	srv := newServer(newFakeStore())
	defer srv.Close()

	cases := []struct {
		name, body string
		want       int
	}{
		{"valid", `{"form_id":"f","idempotency_key":"k1","email":"a@b.com","data":{"x":1}}`, 201},
		{"missing form_id", `{"idempotency_key":"k2","email":"a@b.com"}`, 400},
		{"blank form_id", `{"form_id":"  ","idempotency_key":"k3","email":"a@b.com"}`, 400},
		{"invalid email", `{"form_id":"f","idempotency_key":"k4","email":"not-an-email"}`, 400},
		{"display-name email", `{"form_id":"f","idempotency_key":"k5","email":"John <a@b.com>"}`, 400},
		{"missing key", `{"form_id":"f","email":"a@b.com"}`, 400},
		{"data not object", `{"form_id":"f","idempotency_key":"k6","email":"a@b.com","data":[1]}`, 400},
		{"malformed json", `{"form_id":`, 400},
		{"data omitted", `{"form_id":"f","idempotency_key":"k7","email":"a@b.com"}`, 201},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := post(t, srv.URL, tc.body).StatusCode; got != tc.want {
				t.Fatalf("status = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCreateIdempotent(t *testing.T) {
	store := newFakeStore()
	srv := newServer(store)
	defer srv.Close()

	body := `{"form_id":"f","idempotency_key":"same","email":"a@b.com","data":{}}`
	first := post(t, srv.URL, body)
	second := post(t, srv.URL, body)

	if first.StatusCode != 201 || second.StatusCode != 200 {
		t.Fatalf("statuses = %d, %d; want 201, 200", first.StatusCode, second.StatusCode)
	}
	if second.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatal("missing Idempotent-Replayed header on duplicate")
	}
	var a, b Submission
	json.NewDecoder(first.Body).Decode(&a)
	json.NewDecoder(second.Body).Decode(&b)
	if a.ID != b.ID {
		t.Fatalf("duplicate returned id %d, want %d", b.ID, a.ID)
	}
	if len(store.rows) != 1 {
		t.Fatalf("stored %d rows, want 1", len(store.rows))
	}
}

func TestListPagination(t *testing.T) {
	store := newFakeStore()
	srv := newServer(store)
	defer srv.Close()

	for i := range 23 {
		post(t, srv.URL, fmt.Sprintf(`{"form_id":"f","idempotency_key":"k%d","email":"a@b.com"}`, i))
	}
	post(t, srv.URL, `{"form_id":"other","idempotency_key":"o1","email":"a@b.com"}`)

	get := func(q string) (int, Page) {
		resp, err := http.Get(srv.URL + "/forms/f/submissions" + q)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var p Page
		json.NewDecoder(resp.Body).Decode(&p)
		return resp.StatusCode, p
	}

	if code, p := get(""); code != 200 || len(p.Items) != 10 || p.Total != 23 || p.TotalPages != 3 || p.Page != 1 {
		t.Fatalf("page 1: code=%d items=%d total=%d pages=%d", code, len(p.Items), p.Total, p.TotalPages)
	}
	if _, p := get("?page=3"); len(p.Items) != 3 {
		t.Fatalf("page 3: items=%d, want 3", len(p.Items))
	}
	if _, p := get("?page=4"); len(p.Items) != 0 {
		t.Fatalf("page 4: items=%d, want 0", len(p.Items))
	}
	if code, _ := get("?page=0"); code != 400 {
		t.Fatalf("page=0: code=%d, want 400", code)
	}
}
