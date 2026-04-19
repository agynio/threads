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

func (s *Store) CreateThread(ctx context.Context, organizationID uuid.UUID, participantInputs []ParticipantInput) (Thread, error) {
	var thread Thread
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		threadID := uuid.New()
		now := time.Now().UTC()
		if _, err := tx.Exec(ctx, `INSERT INTO threads (id, organization_id, status, created_at, updated_at, message_count) VALUES ($1, $2, $3, $4, $4, 0)`, threadID, organizationID, int16(ThreadStatusActive), now); err != nil {
			return err
		}
		participants := make([]Participant, len(participantInputs))
		for i, participant := range participantInputs {
			if _, err := tx.Exec(ctx, `INSERT INTO thread_participants (thread_id, participant_id, joined_at, passive) VALUES ($1, $2, $3, $4)`, threadID, participant.ID, now, participant.Passive); err != nil {
				return err
			}
			participants[i] = Participant{ID: participant.ID, JoinedAt: now, Passive: participant.Passive}
		}
		thread = Thread{
			ID:             threadID,
			OrganizationID: &organizationID,
			MessageCount:   0,
			Status:         ThreadStatusActive,
			CreatedAt:      now,
			UpdatedAt:      now,
			Participants:   participants,
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
		row := tx.QueryRow(ctx, `UPDATE threads SET status = $2, updated_at = $3 WHERE id = $1 RETURNING id, status, created_at, updated_at, organization_id, message_count`, threadID, int16(ThreadStatusArchived), now)
		threadRow, err := scanThreadRow(row)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrThreadNotFound
			}
			return err
		}
		participants, err := loadParticipants(ctx, tx, threadID)
		if err != nil {
			return err
		}
		thread = threadRow
		thread.Participants = participants
		return nil
	})
	if err != nil {
		return Thread{}, err
	}
	return thread, nil
}

func (s *Store) AddParticipant(ctx context.Context, threadID, participantID uuid.UUID, passive bool) (Thread, error) {
	var thread Thread
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		threadRow, err := loadThreadRow(ctx, tx, threadID, true)
		if err != nil {
			return err
		}
		if threadRow.Status == ThreadStatusArchived {
			return ErrThreadArchived
		}
		now := time.Now().UTC()
		cmd, err := tx.Exec(ctx, `INSERT INTO thread_participants (thread_id, participant_id, joined_at, passive) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, threadID, participantID, now, passive)
		if err != nil {
			return err
		}
		if cmd.RowsAffected() > 0 {
			if _, err := tx.Exec(ctx, `UPDATE threads SET updated_at = $2 WHERE id = $1`, threadID, now); err != nil {
				return err
			}
			threadRow.UpdatedAt = now
		}
		participants, err := loadParticipants(ctx, tx, threadID)
		if err != nil {
			return err
		}
		thread = threadRow
		thread.Participants = participants
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
	query.WriteString(`SELECT t.id, t.status, t.created_at, t.updated_at, t.organization_id, t.message_count
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
		thread, err := scanThreadRow(rows)
		if err != nil {
			return ThreadListResult{}, err
		}
		if int32(len(threads)) == limit {
			hasMore = true
			break
		}
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

func (s *Store) ListOrganizationThreads(ctx context.Context, organizationID uuid.UUID, status *ThreadStatus, pageSize int32, cursor *OrganizationThreadCursor) (OrganizationThreadListResult, error) {
	limit := normalizePageSize(pageSize)
	query := strings.Builder{}
	query.WriteString(`SELECT id, status, created_at, updated_at, organization_id, message_count
        FROM threads
        WHERE organization_id = $1`)
	args := []any{organizationID}
	paramIndex := 2
	if status != nil {
		query.WriteString(fmt.Sprintf(" AND status = $%d", paramIndex))
		args = append(args, int16(*status))
		paramIndex++
	}
	if cursor != nil {
		query.WriteString(fmt.Sprintf(" AND (created_at, id) < ($%d, $%d)", paramIndex, paramIndex+1))
		args = append(args, cursor.CreatedAt, cursor.ThreadID)
		paramIndex += 2
	}
	query.WriteString(fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", paramIndex))
	args = append(args, int(limit)+1)

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return OrganizationThreadListResult{}, err
	}
	defer rows.Close()

	threads := make([]Thread, 0, limit)
	var (
		nextCursor *OrganizationThreadCursor
		lastID     uuid.UUID
		lastTime   time.Time
		hasMore    bool
	)
	for rows.Next() {
		thread, err := scanThreadRow(rows)
		if err != nil {
			return OrganizationThreadListResult{}, err
		}
		if int32(len(threads)) == limit {
			hasMore = true
			break
		}
		threads = append(threads, thread)
		lastID = thread.ID
		lastTime = thread.CreatedAt
	}
	if err := rows.Err(); err != nil {
		return OrganizationThreadListResult{}, err
	}
	if hasMore {
		nextCursor = &OrganizationThreadCursor{CreatedAt: lastTime, ThreadID: lastID}
	}

	for i := range threads {
		participants, err := loadParticipants(ctx, s.pool, threads[i].ID)
		if err != nil {
			return OrganizationThreadListResult{}, err
		}
		threads[i].Participants = participants
	}

	return OrganizationThreadListResult{Threads: threads, NextCursor: nextCursor}, nil
}

func (s *Store) GetThread(ctx context.Context, threadID uuid.UUID) (Thread, error) {
	thread, err := loadThread(ctx, s.pool, threadID)
	if err != nil {
		return Thread{}, err
	}
	return thread, nil
}
