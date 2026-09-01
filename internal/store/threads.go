package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// CreateThread takes the thread's id rather than inventing one, because
// participants are finalized before the row exists: creating an agent instance
// has to name the thread that is adding it, and that has to be the same id the
// thread ends up with.
func (s *Store) CreateThread(ctx context.Context, threadID uuid.UUID, organizationID uuid.UUID, participantInputs []ParticipantInput) (Thread, error) {
	var thread Thread
	err := s.runTx(ctx, func(tx pgx.Tx) error {
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

func (s *Store) DegradeThread(ctx context.Context, threadID uuid.UUID) (Thread, error) {
	var thread Thread
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		threadRow, err := loadThreadRow(ctx, tx, threadID, true)
		if err != nil {
			return err
		}
		if threadRow.Status != ThreadStatusArchived && threadRow.Status != ThreadStatusDegraded {
			now := time.Now().UTC()
			row := tx.QueryRow(ctx, `UPDATE threads SET status = $2, updated_at = $3 WHERE id = $1 RETURNING id, status, created_at, updated_at, organization_id, message_count`, threadID, int16(ThreadStatusDegraded), now)
			threadRow, err = scanThreadRow(row)
			if err != nil {
				return err
			}
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

func (s *Store) ListOrganizationThreads(ctx context.Context, organizationID uuid.UUID, filter OrganizationThreadFilter, sort OrganizationThreadSort, pageSize int32, cursor *OrganizationThreadCursor) (OrganizationThreadListResult, error) {
	limit := normalizePageSize(pageSize)
	query, args, err := buildOrganizationThreadsQuery(organizationID, filter, sort, cursor, limit)
	if err != nil {
		return OrganizationThreadListResult{}, err
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return OrganizationThreadListResult{}, err
	}
	defer rows.Close()

	threads := make([]Thread, 0, limit)
	var (
		nextCursor *OrganizationThreadCursor
		lastThread Thread
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
		lastThread = thread
	}
	if err := rows.Err(); err != nil {
		return OrganizationThreadListResult{}, err
	}
	if hasMore {
		nextCursorValue, err := organizationThreadCursorFromThread(lastThread, sort.Field)
		if err != nil {
			return OrganizationThreadListResult{}, err
		}
		nextCursor = &nextCursorValue
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

func buildOrganizationThreadsQuery(organizationID uuid.UUID, filter OrganizationThreadFilter, sort OrganizationThreadSort, cursor *OrganizationThreadCursor, limit int32) (string, []any, error) {
	sortColumn, err := organizationThreadSortColumn(sort.Field)
	if err != nil {
		return "", nil, err
	}
	orderDirection, cursorOperator, err := organizationThreadSortDirection(sort.Direction)
	if err != nil {
		return "", nil, err
	}

	query := strings.Builder{}
	query.WriteString(`SELECT t.id, t.status, t.created_at, t.updated_at, t.organization_id, t.message_count
        FROM threads t
        WHERE t.organization_id = $1`)
	args := []any{organizationID}
	paramIndex := 2
	if len(filter.StatusIn) > 0 {
		statusValues := make([]int16, len(filter.StatusIn))
		for i, status := range filter.StatusIn {
			statusValues[i] = int16(status)
		}
		query.WriteString(fmt.Sprintf(" AND t.status = ANY($%d)", paramIndex))
		args = append(args, pgtype.FlatArray[int16](statusValues))
		paramIndex++
	}
	if len(filter.ParticipantIDs) > 0 {
		query.WriteString(fmt.Sprintf(" AND EXISTS (SELECT 1 FROM thread_participants tp WHERE tp.thread_id = t.id AND tp.participant_id = ANY($%d))", paramIndex))
		args = append(args, pgtype.FlatArray[uuid.UUID](filter.ParticipantIDs))
		paramIndex++
	}
	if filter.CreatedAfter != nil {
		query.WriteString(fmt.Sprintf(" AND t.created_at >= $%d", paramIndex))
		args = append(args, *filter.CreatedAfter)
		paramIndex++
	}
	if filter.CreatedBefore != nil {
		query.WriteString(fmt.Sprintf(" AND t.created_at < $%d", paramIndex))
		args = append(args, *filter.CreatedBefore)
		paramIndex++
	}
	if cursor != nil {
		cursorValue, err := organizationThreadCursorValue(sort.Field, *cursor)
		if err != nil {
			return "", nil, err
		}
		query.WriteString(fmt.Sprintf(" AND (%s %s $%d OR (%s = $%d AND t.id > $%d))", sortColumn, cursorOperator, paramIndex, sortColumn, paramIndex, paramIndex+1))
		args = append(args, cursorValue, cursor.ThreadID)
		paramIndex += 2
	}
	query.WriteString(fmt.Sprintf(" ORDER BY %s %s, t.id ASC LIMIT $%d", sortColumn, orderDirection, paramIndex))
	args = append(args, int(limit)+1)
	return query.String(), args, nil
}

func organizationThreadSortColumn(field OrganizationThreadSortField) (string, error) {
	switch field {
	case OrganizationThreadSortFieldCreated:
		return "t.created_at", nil
	case OrganizationThreadSortFieldUpdated:
		return "t.updated_at", nil
	case OrganizationThreadSortFieldMessageCount:
		return "t.message_count", nil
	case OrganizationThreadSortFieldStatus:
		return "t.status", nil
	default:
		return "", fmt.Errorf("unsupported sort field: %d", field)
	}
}

func organizationThreadSortDirection(direction SortDirection) (string, string, error) {
	switch direction {
	case SortDirectionAsc:
		return "ASC", ">", nil
	case SortDirectionDesc:
		return "DESC", "<", nil
	default:
		return "", "", fmt.Errorf("unsupported sort direction: %d", direction)
	}
}

func organizationThreadCursorValue(field OrganizationThreadSortField, cursor OrganizationThreadCursor) (any, error) {
	switch field {
	case OrganizationThreadSortFieldCreated:
		return cursor.CreatedAt, nil
	case OrganizationThreadSortFieldUpdated:
		return cursor.UpdatedAt, nil
	case OrganizationThreadSortFieldMessageCount:
		return cursor.MessageCount, nil
	case OrganizationThreadSortFieldStatus:
		return int16(cursor.Status), nil
	default:
		return nil, fmt.Errorf("unsupported sort field: %d", field)
	}
}

func organizationThreadCursorFromThread(thread Thread, field OrganizationThreadSortField) (OrganizationThreadCursor, error) {
	cursor := OrganizationThreadCursor{ThreadID: thread.ID}
	switch field {
	case OrganizationThreadSortFieldCreated:
		cursor.CreatedAt = thread.CreatedAt
	case OrganizationThreadSortFieldUpdated:
		cursor.UpdatedAt = thread.UpdatedAt
	case OrganizationThreadSortFieldMessageCount:
		cursor.MessageCount = thread.MessageCount
	case OrganizationThreadSortFieldStatus:
		cursor.Status = thread.Status
	default:
		return OrganizationThreadCursor{}, fmt.Errorf("unsupported sort field: %d", field)
	}
	return cursor, nil
}

func (s *Store) GetThread(ctx context.Context, threadID uuid.UUID) (Thread, error) {
	thread, err := loadThread(ctx, s.pool, threadID)
	if err != nil {
		return Thread{}, err
	}
	return thread, nil
}

// OrganizationThreadTuples is one thread's identity in the authorization model:
// the thread itself, and the participants holding a relation on it. Both come
// off before the row does.
type OrganizationThreadTuples struct {
	ThreadID       uuid.UUID
	ParticipantIDs []uuid.UUID
}

// ListOrganizationThreadTuples returns every thread the organization carries,
// with its participants, unpaginated. The teardown needs all of them.
func (s *Store) ListOrganizationThreadTuples(ctx context.Context, organizationID uuid.UUID) ([]OrganizationThreadTuples, error) {
	rows, err := s.pool.Query(ctx, `
        SELECT t.id, COALESCE(ARRAY_AGG(p.participant_id) FILTER (WHERE p.participant_id IS NOT NULL), '{}')
        FROM threads t
        LEFT JOIN thread_participants p ON p.thread_id = t.id
        WHERE t.organization_id = $1
        GROUP BY t.id`, organizationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	threads := []OrganizationThreadTuples{}
	for rows.Next() {
		var entry OrganizationThreadTuples
		var participants pgtype.FlatArray[uuid.UUID]
		if err := rows.Scan(&entry.ThreadID, &participants); err != nil {
			return nil, err
		}
		entry.ParticipantIDs = participants
		threads = append(threads, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return threads, nil
}

// DeleteOrganizationThreads removes the organization's threads. Messages,
// participants, recipients, and inbox deliveries follow through ON DELETE
// CASCADE.
func (s *Store) DeleteOrganizationThreads(ctx context.Context, organizationID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM threads WHERE organization_id = $1`, organizationID)
	return err
}
