// Package stats serves the per-hour submission counts computed by the Flink
// job (Part 3). The API only reads them; Flink is the sole writer.
package stats

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// maxHours caps the response at one week of hourly buckets.
const maxHours = 168

type HourlyCount struct {
	HourStart time.Time `json:"hour_start"`
	Count     int64     `json:"count"`
}

type response struct {
	FormID string        `json:"form_id"`
	Total  int64         `json:"total"`
	Hours  []HourlyCount `json:"hours"` // newest first
}

type Handler struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewHandler(pool *pgxpool.Pool, log *slog.Logger) *Handler {
	return &Handler{pool: pool, log: log}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /forms/{form_id}/hourly-counts", h.hourly)
}

func (h *Handler) hourly(w http.ResponseWriter, r *http.Request) {
	formID := r.PathValue("form_id")

	rows, err := h.pool.Query(r.Context(), `
		SELECT hour_start, submission_count
		FROM form_hourly_counts
		WHERE form_id = $1
		ORDER BY hour_start DESC
		LIMIT $2`, formID, maxHours)
	if err != nil {
		h.fail(w, err, formID)
		return
	}
	defer rows.Close()

	resp := response{FormID: formID, Hours: []HourlyCount{}}
	for rows.Next() {
		var hc HourlyCount
		if err := rows.Scan(&hc.HourStart, &hc.Count); err != nil {
			h.fail(w, err, formID)
			return
		}
		hc.HourStart = hc.HourStart.UTC()
		resp.Hours = append(resp.Hours, hc)
		resp.Total += hc.Count
	}
	if err := rows.Err(); err != nil {
		h.fail(w, err, formID)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *Handler) fail(w http.ResponseWriter, err error, formID string) {
	h.log.Error("hourly counts", "err", err, "form_id", formID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = w.Write([]byte(`{"error":"internal error"}` + "\n"))
}
