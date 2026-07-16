package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type SendMessageResult struct {
	Message              Message
	OrganizationID       uuid.UUID
	Recipients           []uuid.UUID
	AgentInboxDeliveries []AgentInboxDelivery
}

func (s *Store) SendMessage(ctx context.Context, threadID, senderID uuid.UUID, body string, fileIDs []uuid.UUID, recipients []uuid.UUID, agentInstanceRecipients []uuid.UUID) (SendMessageResult, error) {
	var result SendMessageResult
	err := s.runTx(ctx, func(tx pgx.Tx) error {
		thread, err := loadThreadRow(ctx, tx, threadID, true)
		if err != nil {
			return err
		}
		if thread.Status == ThreadStatusArchived {
			return ErrThreadArchived
		}
		if thread.Status == ThreadStatusDegraded {
			return ErrThreadDegraded
		}
		if thread.OrganizationID == nil {
			return ErrThreadOrganizationMissing
		}
		organizationID := *thread.OrganizationID
		now := time.Now().UTC()
		messageID := uuid.New()
		fileIDArray := pgtype.FlatArray[string](uuidsToStrings(fileIDs))
		if _, err := tx.Exec(ctx, `INSERT INTO messages (id, thread_id, sender_id, body, file_ids, created_at) VALUES ($1, $2, $3, $4, $5, $6)`, messageID, threadID, senderID, body, fileIDArray, now); err != nil {
			return err
		}
		if len(recipients) > 0 {
			rows := make([][]any, len(recipients))
			for i, recipientID := range recipients {
				rows[i] = []any{messageID, threadID, recipientID}
			}
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"message_recipients"}, []string{"message_id", "thread_id", "participant_id"}, pgx.CopyFromRows(rows)); err != nil {
				return err
			}
		}
		agentInboxDeliveries := make([]AgentInboxDelivery, len(agentInstanceRecipients))
		if len(agentInstanceRecipients) > 0 {
			rows := make([][]any, len(agentInstanceRecipients))
			for i, agentInstanceID := range agentInstanceRecipients {
				rows[i] = []any{messageID, agentInstanceID, threadID, senderID, body, fileIDArray, now, now}
				agentInboxDeliveries[i] = AgentInboxDelivery{
					MessageID:       messageID,
					AgentInstanceID: agentInstanceID,
					ThreadID:        threadID,
					SenderID:        senderID,
					Body:            body,
					FileIDs:         fileIDs,
					CreatedAt:       now,
					UpdatedAt:       now,
				}
			}
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"agent_inbox_deliveries"}, []string{"message_id", "agent_instance_id", "thread_id", "sender_id", "body", "file_ids", "created_at", "updated_at"}, pgx.CopyFromRows(rows)); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE threads SET updated_at = $2, message_count = message_count + 1 WHERE id = $1`, threadID, now); err != nil {
			return err
		}
		result = SendMessageResult{
			Message: Message{
				ID:        messageID,
				ThreadID:  threadID,
				SenderID:  senderID,
				Body:      body,
				FileIDs:   fileIDs,
				CreatedAt: now,
			},
			OrganizationID:       organizationID,
			Recipients:           recipients,
			AgentInboxDeliveries: agentInboxDeliveries,
		}
		return nil
	})
	if err != nil {
		return SendMessageResult{}, err
	}
	return result, nil
}

func (s *Store) ListMessages(ctx context.Context, threadID uuid.UUID, pageSize int32, cursor *MessageCursor, order MessageOrder) (MessageListResult, error) {
	if err := ensureThreadExists(ctx, s.pool, threadID); err != nil {
		return MessageListResult{}, err
	}
	limit := normalizePageSize(pageSize)
	query := strings.Builder{}
	query.WriteString(`SELECT id, thread_id, sender_id, body, file_ids, created_at
        FROM messages
        WHERE thread_id = $1`)
	args := []any{threadID}
	paramIndex := 2
	if cursor != nil {
		operator := ">"
		if order == MessageOrderNewestFirst {
			operator = "<"
		}
		query.WriteString(fmt.Sprintf(" AND (created_at, id) %s ($%d, $%d)", operator, paramIndex, paramIndex+1))
		args = append(args, cursor.CreatedAt, cursor.MessageID)
		paramIndex += 2
	}
	orderClause := "ORDER BY created_at ASC, id ASC"
	if order == MessageOrderNewestFirst {
		orderClause = "ORDER BY created_at DESC, id DESC"
	}
	query.WriteString(fmt.Sprintf(" %s LIMIT $%d", orderClause, paramIndex))
	args = append(args, int(limit)+1)

	rows, err := s.pool.Query(ctx, query.String(), args...)
	if err != nil {
		return MessageListResult{}, err
	}
	defer rows.Close()

	messages := make([]Message, 0, limit)
	var (
		nextCursor *MessageCursor
		lastID     uuid.UUID
		lastTime   time.Time
		hasMore    bool
	)
	for rows.Next() {
		var msg Message
		var fileIDs []string
		if err := rows.Scan(&msg.ID, &msg.ThreadID, &msg.SenderID, &msg.Body, &fileIDs, &msg.CreatedAt); err != nil {
			return MessageListResult{}, err
		}
		if int32(len(messages)) == limit {
			hasMore = true
			break
		}
		parsedIDs, err := stringsToUUIDs(fileIDs)
		if err != nil {
			return MessageListResult{}, fmt.Errorf("parse file ids: %w", err)
		}
		msg.FileIDs = parsedIDs
		messages = append(messages, msg)
		lastID = msg.ID
		lastTime = msg.CreatedAt
	}
	if err := rows.Err(); err != nil {
		return MessageListResult{}, err
	}
	if hasMore {
		nextCursor = &MessageCursor{CreatedAt: lastTime, MessageID: lastID}
	}
	return MessageListResult{Messages: messages, NextCursor: nextCursor}, nil
}

func (s *Store) ListUnackedMessages(ctx context.Context, participantID uuid.UUID, threadID *uuid.UUID, pageSize int32, cursor *MessageCursor) (MessageListResult, error) {
	limit := normalizePageSize(pageSize)
	query, args := buildUnackedMessagesQuery(participantID, threadID, cursor, limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return MessageListResult{}, err
	}
	defer rows.Close()

	messages := make([]Message, 0, limit)
	var (
		nextCursor *MessageCursor
		lastID     uuid.UUID
		lastTime   time.Time
		hasMore    bool
	)
	for rows.Next() {
		var msg Message
		var fileIDs []string
		if err := rows.Scan(&msg.ID, &msg.ThreadID, &msg.SenderID, &msg.Body, &fileIDs, &msg.CreatedAt); err != nil {
			return MessageListResult{}, err
		}
		if int32(len(messages)) == limit {
			hasMore = true
			break
		}
		parsedIDs, err := stringsToUUIDs(fileIDs)
		if err != nil {
			return MessageListResult{}, fmt.Errorf("parse file ids: %w", err)
		}
		msg.FileIDs = parsedIDs
		messages = append(messages, msg)
		lastID = msg.ID
		lastTime = msg.CreatedAt
	}
	if err := rows.Err(); err != nil {
		return MessageListResult{}, err
	}
	if hasMore {
		nextCursor = &MessageCursor{CreatedAt: lastTime, MessageID: lastID}
	}
	return MessageListResult{Messages: messages, NextCursor: nextCursor}, nil
}

func (s *Store) GetUnackedMessageCounts(ctx context.Context, participantID uuid.UUID) (map[uuid.UUID]int32, error) {
	rows, err := s.pool.Query(ctx, `SELECT thread_id, COUNT(*) FROM message_recipients WHERE participant_id = $1 AND acked_at IS NULL GROUP BY thread_id`, participantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[uuid.UUID]int32)
	for rows.Next() {
		var threadID uuid.UUID
		var count int64
		if err := rows.Scan(&threadID, &count); err != nil {
			return nil, err
		}
		if count > int64(^uint32(0)>>1) {
			return nil, fmt.Errorf("unacked count overflow: %d", count)
		}
		counts[threadID] = int32(count)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return counts, nil
}

func buildUnackedMessagesQuery(participantID uuid.UUID, threadID *uuid.UUID, cursor *MessageCursor, limit int32) (string, []any) {
	query := strings.Builder{}
	query.WriteString(`SELECT m.id, m.thread_id, m.sender_id, m.body, m.file_ids, m.created_at
        FROM message_recipients mr
        JOIN messages m ON m.id = mr.message_id
        WHERE mr.participant_id = $1 AND mr.acked_at IS NULL`)
	args := []any{participantID}
	paramIndex := 2
	if threadID != nil {
		query.WriteString(fmt.Sprintf(" AND m.thread_id = $%d", paramIndex))
		args = append(args, *threadID)
		paramIndex++
	}
	if cursor != nil {
		query.WriteString(fmt.Sprintf(" AND (m.created_at, m.id) > ($%d, $%d)", paramIndex, paramIndex+1))
		args = append(args, cursor.CreatedAt, cursor.MessageID)
		paramIndex += 2
	}
	query.WriteString(fmt.Sprintf(" ORDER BY m.created_at ASC, m.id ASC LIMIT $%d", paramIndex))
	args = append(args, int(limit)+1)
	return query.String(), args
}

func (s *Store) AckMessages(ctx context.Context, participantID uuid.UUID, messageIDs []uuid.UUID) (int32, error) {
	now := time.Now().UTC()
	messageIDArray := pgtype.FlatArray[uuid.UUID](messageIDs)
	cmd, err := s.pool.Exec(ctx, `UPDATE message_recipients SET acked_at = $1 WHERE participant_id = $2 AND message_id = ANY($3) AND acked_at IS NULL`, now, participantID, messageIDArray)
	if err != nil {
		return 0, err
	}
	count := cmd.RowsAffected()
	if count > int64(^uint32(0)>>1) {
		return 0, fmt.Errorf("acked count overflow: %d", count)
	}
	return int32(count), nil
}
