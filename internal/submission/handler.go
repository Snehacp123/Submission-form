package submission

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
)

const (
	PerPage      = 10
	maxBodyBytes = 1 << 20 // 1 MiB
)

type Handler struct {
	store Store
	log   *slog.Logger
}

func NewHandler(store Store, log *slog.Logger) *Handler {
	return &Handler{store: store, log: log}
}

// Register mounts the routes using Go 1.22+ method/path patterns.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /submissions", h.create)
	mux.HandleFunc("GET /forms/{form_id}/submissions", h.list)
}

// create returns 201 for a new submission and 200 (with the original record)
// when the idempotency key was already used, so clients can tell them apart.
func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large", nil)
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON body", nil)
		return
	}

	if errs := Validate(&req); len(errs) > 0 {
		writeError(w, http.StatusBadRequest, "validation failed", errs)
		return
	}

	sub, created, err := h.store.Create(r.Context(), req)
	if err != nil {
		h.log.Error("create submission", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error", nil)
		return
	}

	status := http.StatusCreated
	if !created {
		status = http.StatusOK
		w.Header().Set("Idempotent-Replayed", "true")
	}
	writeJSON(w, status, sub)
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	formID := r.PathValue("form_id")

	page := 1
	if p := r.URL.Query().Get("page"); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "page must be a positive integer", nil)
			return
		}
		page = n
	}

	items, total, err := h.store.ListByForm(r.Context(), formID, PerPage, (page-1)*PerPage)
	if err != nil {
		h.log.Error("list submissions", "err", err, "form_id", formID)
		writeError(w, http.StatusInternalServerError, "internal error", nil)
		return
	}

	writeJSON(w, http.StatusOK, Page{
		Items:      items,
		Page:       page,
		PerPage:    PerPage,
		Total:      total,
		TotalPages: (total + PerPage - 1) / PerPage,
	})
}

type errorBody struct {
	Error  string            `json:"error"`
	Fields map[string]string `json:"fields,omitempty"`
}

func writeError(w http.ResponseWriter, status int, msg string, fields map[string]string) {
	writeJSON(w, status, errorBody{Error: msg, Fields: fields})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
