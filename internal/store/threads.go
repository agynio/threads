package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Store) CreateThread(ctx context.Context, participantIDs []uuid.UUID) (Thread, error) {
	var thread Thread
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		threadID := uuid.New()
		now := time.Now().UTC()
		if _, err := tx.Exec(ctx, `INSERT INTO threads (id, status, created_at, updated_at) VALUES ($1, $2, $3, $3)`, threadID, int16(ThreadStatusActive), now); err != nil {
			return err
		}
		participants := make([]Participant, len(participantIDs))
		for i, participantID := range participantIDs {
			if _, err := tx.Exec(ctx, `INSERT INTO thread_participants (thread_id, participant_id, joined_at) VALUES ($1, $2, $3)`, threadID, participantID, now); err != nil {
				return err
			}
			participants[i] = Participant{ID: participantID, JoinedAt: now}
		}
		thread = Thread{
			ID:           threadID,
			Status:       ThreadStatusActive,
			CreatedAt:    now,
			UpdatedAt:    now,
			Participants: participants,
		}
		return nil
	})
	if err != nil {
		return Thread{}, err
	}
	return thread, nil
}

func (s *Store) ArchiveThread(ctx context.Context, threadID uuid.UUID) (Thread, error) {
	var thread Thread
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		now := time.Now().UTC()
		var createdAt time.Time
		var updatedAt time.Time
		if err := tx.QueryRow(ctx, `UPDATE threads SET status = $2, updated_at = $3 WHERE id = $1 RETURNING created_at, updated_at`, threadID, int16(ThreadStatusArchived), now).Scan(&createdAt, &updatedAt); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrThreadNotFound
			}
			return err
		}
		participants, err := loadParticipants(ctx, tx, threadID)
		if err != nil {
			return err
		}
		thread = Thread{
			ID:           threadID,
			Status:       ThreadStatusArchived,
			CreatedAt:    createdAt,
			UpdatedAt:    updatedAt,
			Participants: participants,
		}
		return nil
	})
	if err != nil {
		return Thread{}, err
	}
	return thread, nil
}

func (s *Store) AddParticipant(ctx context.Context, threadID, participantID uuid.UUID) (Thread, error) {
	var thread Thread
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		status, createdAt, updatedAt, err := loadThreadRow(ctx, tx, threadID, true)
		if err != nil {
			return err
		}
		if status == ThreadStatusArchived {
			return ErrThreadArchived
		}
		now := time.Now().UTC()
		cmd, err := tx.Exec(ctx, `INSERT INTO thread_participants (thread_id, participant_id, joined_at) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`, threadID, participantID, now)
		if err != nil {
			return err
		}
		if cmd.RowsAffected() > 0 {
			if _, err := tx.Exec(ctx, `UPDATE threads SET updated_at = $2 WHERE id = $1`, threadID, now); err != nil {
				return err
			}
			updatedAt = now
		}
		participants, err := loadParticipants(ctx, tx, threadID)
		if err != nil {
			return err
		}
		thread = Thread{
			ID:           threadID,
			Status:       status,
			CreatedAt:    createdAt,
			UpdatedAt:    updatedAt,
			Participants: participants,
		}
		return nil
	})
	if err != nil {
		return Thread{}, err
	}
	return thread, nil
}

func (s *Store) ListThreads(ctx context.Context, participantID uuid.UUID, pageSize int32, cursor *ThreadCursor) (ThreadListResult, error) {
	limit := normalizePageSize(pageSize)
	query := strings.Builder{}
	query.WriteString(`SELECT t.id, t.status, t.created_at, t.updated_at
        FROM threads t
        JOIN thread_participants tp ON tp.thread_id = t.id
        WHERE tp.participant_id = $1`)
	args := []any{participantID}
	paramIndex := 2
	if cursor != nil {
		query.WriteString(fmt.Sprintf(" AND (t.updated_at, t.id) < ($%d, $%d)", paramIndex, paramIndex+1))
		args = append(args, cursor.UpdatedAt, cursor.ThreadID)
		paramIndex += 2
	}
	query.WriteString(fmt.Sprintf(" ORDER BY t.updated_at DESC, t.id DESC LIMIT $%d", paramIndex))
	args = append(args, int(limit)+1)

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return ThreadListResult{}, err
	}
	defer rows.Close()

	threads := make([]Thread, 0, limit)
	var (
		nextCursor *ThreadCursor
		lastID     uuid.UUID
		lastTime   time.Time
		hasMore    bool
	)
	for rows.Next() {
		var thread Thread
		var statusValue int16
		if err := rows.Scan(&thread.ID, &statusValue, &thread.CreatedAt, &thread.UpdatedAt); err != nil {
			return ThreadListResult{}, err
		}
		if int32(len(threads)) == limit {
			hasMore = true
			break
		}
		status, err := ParseThreadStatus(statusValue)
		if err != nil {
			return ThreadListResult{}, fmt.Errorf("invalid thread status: %w", err)
		}
		thread.Status = status
		threads = append(threads, thread)
		lastID = thread.ID
		lastTime = thread.UpdatedAt
	}
	if err := rows.Err(); err != nil {
		return ThreadListResult{}, err
	}
	if hasMore {
		nextCursor = &ThreadCursor{UpdatedAt: lastTime, ThreadID: lastID}
	}

	for i := range threads {
		participants, err := loadParticipants(ctx, s.pool, threads[i].ID)
		if err != nil {
			return ThreadListResult{}, err
		}
		threads[i].Participants = participants
	}

	return ThreadListResult{Threads: threads, NextCursor: nextCursor}, nil
}
