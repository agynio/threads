package store

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	agentInboxDeliveryClaimTimeout = time.Minute
	agentInboxDeliveryRetryDelay   = time.Minute
)

const claimPendingAgentInboxDeliveriesSQL = `WITH claimed AS (
		SELECT message_id, agent_instance_id
		FROM agent_inbox_deliveries
		WHERE delivered_at IS NULL
			AND (claimed_at IS NULL OR claimed_at < $2)
			AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())
		ORDER BY created_at ASC, message_id ASC, agent_instance_id ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	)
	UPDATE agent_inbox_deliveries d
	SET claimed_at = NOW(), claim_id = $3, updated_at = NOW()
	FROM claimed
	WHERE d.message_id = claimed.message_id AND d.agent_instance_id = claimed.agent_instance_id
	RETURNING d.message_id, d.agent_instance_id, d.thread_id, d.sender_id, d.body, d.file_ids, d.created_at, d.delivered_at, d.claimed_at, d.claim_id, d.next_attempt_at, d.attempts, d.last_error, d.updated_at`

const markAgentInboxDeliveryDeliveredSQL = `UPDATE agent_inbox_deliveries SET delivered_at = $4, claimed_at = NULL, claim_id = NULL, next_attempt_at = NULL, updated_at = $4, last_error = NULL WHERE message_id = $1 AND agent_instance_id = $2 AND claim_id = $3 AND delivered_at IS NULL`

const markAgentInboxDeliveryFailedSQL = `UPDATE agent_inbox_deliveries SET attempts = attempts + 1, claimed_at = NULL, claim_id = NULL, next_attempt_at = $5, last_error = $4, updated_at = $6 WHERE message_id = $1 AND agent_instance_id = $2 AND claim_id = $3 AND delivered_at IS NULL`

type AgentInboxDelivery struct {
	MessageID       uuid.UUID
	AgentInstanceID uuid.UUID
	ThreadID        uuid.UUID
	SenderID        uuid.UUID
	Body            string
	FileIDs         []uuid.UUID
	CreatedAt       time.Time
	DeliveredAt     *time.Time
	ClaimedAt       *time.Time
	ClaimID         uuid.UUID
	NextAttemptAt   *time.Time
	Attempts        int32
	LastError       *string
	UpdatedAt       time.Time
}

func (s *Store) ClaimPendingAgentInboxDeliveries(ctx context.Context, limit int32) ([]AgentInboxDelivery, error) {
	if limit <= 0 {
		limit = 100
	}
	claimID := uuid.New()
	claimBefore := time.Now().UTC().Add(-agentInboxDeliveryClaimTimeout)
	rows, err := s.pool.Query(ctx, claimPendingAgentInboxDeliveriesSQL, limit, claimBefore, claimID)
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

func (s *Store) MarkAgentInboxDeliveryDelivered(ctx context.Context, messageID, agentInstanceID, claimID uuid.UUID) error {
	now := time.Now().UTC()
	cmd, err := s.pool.Exec(ctx, markAgentInboxDeliveryDeliveredSQL, messageID, agentInstanceID, claimID, now)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return ErrAgentInboxDeliveryNotClaimed
	}
	return nil
}

func (s *Store) MarkAgentInboxDeliveryFailed(ctx context.Context, messageID, agentInstanceID, claimID uuid.UUID, deliveryError string) error {
	now := time.Now().UTC()
	nextAttemptAt := now.Add(agentInboxDeliveryRetryDelay)
	cmd, err := s.pool.Exec(ctx, markAgentInboxDeliveryFailedSQL, messageID, agentInstanceID, claimID, deliveryError, nextAttemptAt, now)
	if err != nil {
		return err
	}
	if cmd.RowsAffected() == 0 {
		return ErrAgentInboxDeliveryNotClaimed
	}
	return nil
}

func scanAgentInboxDelivery(row pgx.Row) (AgentInboxDelivery, error) {
	var delivery AgentInboxDelivery
	var fileIDs []string
	if err := row.Scan(&delivery.MessageID, &delivery.AgentInstanceID, &delivery.ThreadID, &delivery.SenderID, &delivery.Body, &fileIDs, &delivery.CreatedAt, &delivery.DeliveredAt, &delivery.ClaimedAt, &delivery.ClaimID, &delivery.NextAttemptAt, &delivery.Attempts, &delivery.LastError, &delivery.UpdatedAt); err != nil {
		return AgentInboxDelivery{}, err
	}
	parsedFileIDs, err := stringsToUUIDs(fileIDs)
	if err != nil {
		return AgentInboxDelivery{}, fmt.Errorf("parse file ids: %w", err)
	}
	delivery.FileIDs = parsedFileIDs
	return delivery, nil
}
