package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type AgentInboxDelivery struct {
	MessageID       uuid.UUID
	AgentInstanceID uuid.UUID
	ThreadID        uuid.UUID
	SenderID        uuid.UUID
	Body            string
	FileIDs         []uuid.UUID
	CreatedAt       time.Time
	DeliveredAt     *time.Time
	Attempts        int32
	LastError       *string
	UpdatedAt       time.Time
}

func (s *Store) ListPendingAgentInboxDeliveries(ctx context.Context, limit int32) ([]AgentInboxDelivery, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT message_id, agent_instance_id, thread_id, sender_id, body, file_ids, created_at, delivered_at, attempts, last_error, updated_at
		FROM agent_inbox_deliveries
		WHERE delivered_at IS NULL
		ORDER BY created_at ASC, message_id ASC, agent_instance_id ASC
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	deliveries := make([]AgentInboxDelivery, 0)
	for rows.Next() {
		delivery, err := scanAgentInboxDelivery(rows)
		if err != nil {
			return nil, err
		}
		deliveries = append(deliveries, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return deliveries, nil
}

func (s *Store) MarkAgentInboxDeliveryDelivered(ctx context.Context, messageID, agentInstanceID uuid.UUID) error {
	now := time.Now().UTC()
	_, err := s.pool.Exec(ctx, `UPDATE agent_inbox_deliveries SET delivered_at = $3, updated_at = $3, last_error = NULL WHERE message_id = $1 AND agent_instance_id = $2`, messageID, agentInstanceID, now)
	return err
}

func (s *Store) MarkAgentInboxDeliveryFailed(ctx context.Context, messageID, agentInstanceID uuid.UUID, deliveryError string) error {
	now := time.Now().UTC()
	_, err := s.pool.Exec(ctx, `UPDATE agent_inbox_deliveries SET attempts = attempts + 1, last_error = $3, updated_at = $4 WHERE message_id = $1 AND agent_instance_id = $2 AND delivered_at IS NULL`, messageID, agentInstanceID, deliveryError, now)
	return err
}

func scanAgentInboxDelivery(row pgx.Row) (AgentInboxDelivery, error) {
	var delivery AgentInboxDelivery
	var fileIDs []string
	if err := row.Scan(&delivery.MessageID, &delivery.AgentInstanceID, &delivery.ThreadID, &delivery.SenderID, &delivery.Body, &fileIDs, &delivery.CreatedAt, &delivery.DeliveredAt, &delivery.Attempts, &delivery.LastError, &delivery.UpdatedAt); err != nil {
		return AgentInboxDelivery{}, err
	}
	parsedFileIDs, err := stringsToUUIDs(fileIDs)
	if err != nil {
		return AgentInboxDelivery{}, fmt.Errorf("parse file ids: %w", err)
	}
	delivery.FileIDs = parsedFileIDs
	return delivery, nil
}
