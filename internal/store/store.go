package store

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) runTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

type queryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func loadThreadRow(ctx context.Context, q queryer, id uuid.UUID, forUpdate bool) (Thread, error) {
	query := "SELECT id, status, created_at, updated_at, organization_id, message_count FROM threads WHERE id = $1"
	if forUpdate {
		query += " FOR UPDATE"
	}
	thread, err := scanThreadRow(q.QueryRow(ctx, query, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Thread{}, ErrThreadNotFound
		}
		return Thread{}, err
	}
	return thread, nil
}

func loadThread(ctx context.Context, q queryer, id uuid.UUID) (Thread, error) {
	thread, err := loadThreadRow(ctx, q, id, false)
	if err != nil {
		return Thread{}, err
	}
	participants, err := loadParticipants(ctx, q, id)
	if err != nil {
		return Thread{}, err
	}
	thread.Participants = participants
	return thread, nil
}

func loadParticipants(ctx context.Context, q queryer, threadID uuid.UUID) ([]Participant, error) {
	rows, err := q.Query(ctx, `SELECT participant_id, joined_at, passive FROM thread_participants WHERE thread_id = $1 ORDER BY joined_at ASC, participant_id ASC`, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	participants := []Participant{}
	for rows.Next() {
		var participant Participant
		if err := rows.Scan(&participant.ID, &participant.JoinedAt, &participant.Passive); err != nil {
			return nil, err
		}
		participants = append(participants, participant)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return participants, nil
}

func ensureThreadExists(ctx context.Context, q queryer, threadID uuid.UUID) error {
	var exists bool
	if err := q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM threads WHERE id = $1)`, threadID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrThreadNotFound
	}
	return nil
}

func uuidsToStrings(ids []uuid.UUID) []string {
	values := make([]string, len(ids))
	for i, id := range ids {
		values[i] = id.String()
	}
	return values
}

func stringsToUUIDs(values []string) ([]uuid.UUID, error) {
	ids := make([]uuid.UUID, len(values))
	for i, raw := range values {
		id, err := uuid.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("parse uuid: %w", err)
		}
		ids[i] = id
	}
	return ids, nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanThreadRow(scanner rowScanner) (Thread, error) {
	var thread Thread
	var statusValue int16
	var organizationID pgtype.UUID
	if err := scanner.Scan(&thread.ID, &statusValue, &thread.CreatedAt, &thread.UpdatedAt, &organizationID, &thread.MessageCount); err != nil {
		return Thread{}, err
	}
	status, err := ParseThreadStatus(statusValue)
	if err != nil {
		return Thread{}, fmt.Errorf("invalid thread status: %w", err)
	}
	thread.Status = status
	if organizationID.Valid {
		orgID := uuid.UUID(organizationID.Bytes)
		thread.OrganizationID = &orgID
	}
	return thread, nil
}
