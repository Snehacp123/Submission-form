package submission

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore is the Store backed by PostgreSQL.
type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{pool: pool}
}

const columns = `id, form_id, idempotency_key, email, data, created_at`

// Create relies on the UNIQUE(idempotency_key) constraint for duplicate
// protection, so it is correct under concurrent requests with the same key:
//
//  1. INSERT ... ON CONFLICT DO NOTHING RETURNING — if we won, we get the row.
//  2. Otherwise the key already exists; SELECT it.
//
// The two steps are deliberately separate statements. A single-statement CTE
// (INSERT ... UNION SELECT) can return zero rows when a concurrent request
// commits the same key mid-statement, because the SELECT runs on a snapshot
// taken before that commit. Under READ COMMITTED, step 2 takes a fresh
// snapshot and is guaranteed to see the committed row.
func (s *PostgresStore) Create(ctx context.Context, req CreateRequest) (Submission, bool, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO submissions (form_id, idempotency_key, email, data)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+columns,
		req.FormID, req.IdempotencyKey, req.Email, []byte(req.Data),
	)
	sub, err := scan(row)
	if err == nil {
		return sub, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Submission{}, false, fmt.Errorf("insert submission: %w", err)
	}

	row = s.pool.QueryRow(ctx,
		`SELECT `+columns+` FROM submissions WHERE idempotency_key = $1`,
		req.IdempotencyKey,
	)
	sub, err = scan(row)
	if err != nil {
		return Submission{}, false, fmt.Errorf("fetch existing submission: %w", err)
	}
	return sub, false, nil
}

func (s *PostgresStore) ListByForm(ctx context.Context, formID string, limit, offset int) ([]Submission, int, error) {
	var total int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM submissions WHERE form_id = $1`, formID,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count submissions: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT `+columns+`
		FROM submissions
		WHERE form_id = $1
		ORDER BY id
		LIMIT $2 OFFSET $3`,
		formID, limit, offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("list submissions: %w", err)
	}
	defer rows.Close()

	items := make([]Submission, 0, limit)
	for rows.Next() {
		sub, err := scan(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan submission: %w", err)
		}
		items = append(items, sub)
	}
	return items, total, rows.Err()
}

func scan(row pgx.Row) (Submission, error) {
	var sub Submission
	var data []byte
	err := row.Scan(&sub.ID, &sub.FormID, &sub.IdempotencyKey, &sub.Email, &data, &sub.CreatedAt)
	sub.Data = data
	return sub, err
}
