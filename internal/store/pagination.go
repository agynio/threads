package store

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	defaultPageSize int32 = 50
	maxPageSize     int32 = 100
)

func normalizePageSize(size int32) int32 {
	if size <= 0 {
		return defaultPageSize
	}
	if size > maxPageSize {
		return maxPageSize
	}
	return size
}

type threadPageToken struct {
	ParticipantID  string `json:"participant_id"`
	UpdatedAtNanos int64  `json:"updated_at_nanos"`
	ThreadID       string `json:"thread_id"`
}

func EncodeThreadPageToken(participantID uuid.UUID, cursor ThreadCursor) (string, error) {
	payload := threadPageToken{
		ParticipantID:  participantID.String(),
		UpdatedAtNanos: cursor.UpdatedAt.UnixNano(),
		ThreadID:       cursor.ThreadID.String(),
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func DecodeThreadPageToken(token string) (uuid.UUID, ThreadCursor, error) {
	if token == "" {
		return uuid.UUID{}, ThreadCursor{}, errors.New("empty token")
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return uuid.UUID{}, ThreadCursor{}, fmt.Errorf("decode token: %w", err)
	}
	var payload threadPageToken
	if err := json.Unmarshal(data, &payload); err != nil {
		return uuid.UUID{}, ThreadCursor{}, fmt.Errorf("unmarshal token: %w", err)
	}
	participantID, err := uuid.Parse(payload.ParticipantID)
	if err != nil {
		return uuid.UUID{}, ThreadCursor{}, fmt.Errorf("parse participant id: %w", err)
	}
	threadID, err := uuid.Parse(payload.ThreadID)
	if err != nil {
		return uuid.UUID{}, ThreadCursor{}, fmt.Errorf("parse thread id: %w", err)
	}
	return participantID, ThreadCursor{
		UpdatedAt: time.Unix(0, payload.UpdatedAtNanos).UTC(),
		ThreadID:  threadID,
	}, nil
}

type messagePageToken struct {
	OwnerID        string `json:"owner_id"`
	CreatedAtNanos int64  `json:"created_at_nanos"`
	MessageID      string `json:"message_id"`
}

func EncodeMessagePageToken(ownerID uuid.UUID, cursor MessageCursor) (string, error) {
	payload := messagePageToken{
		OwnerID:        ownerID.String(),
		CreatedAtNanos: cursor.CreatedAt.UnixNano(),
		MessageID:      cursor.MessageID.String(),
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func DecodeMessagePageToken(token string) (uuid.UUID, MessageCursor, error) {
	if token == "" {
		return uuid.UUID{}, MessageCursor{}, errors.New("empty token")
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return uuid.UUID{}, MessageCursor{}, fmt.Errorf("decode token: %w", err)
	}
	var payload messagePageToken
	if err := json.Unmarshal(data, &payload); err != nil {
		return uuid.UUID{}, MessageCursor{}, fmt.Errorf("unmarshal token: %w", err)
	}
	ownerID, err := uuid.Parse(payload.OwnerID)
	if err != nil {
		return uuid.UUID{}, MessageCursor{}, fmt.Errorf("parse owner id: %w", err)
	}
	messageID, err := uuid.Parse(payload.MessageID)
	if err != nil {
		return uuid.UUID{}, MessageCursor{}, fmt.Errorf("parse message id: %w", err)
	}
	return ownerID, MessageCursor{
		CreatedAt: time.Unix(0, payload.CreatedAtNanos).UTC(),
		MessageID: messageID,
	}, nil
}
