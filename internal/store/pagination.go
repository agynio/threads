package store

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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

type organizationThreadPageToken struct {
	OrganizationID     string   `json:"organization_id"`
	SortField          int      `json:"sort_field,omitempty"`
	SortDirection      int      `json:"sort_direction,omitempty"`
	StatusIn           []int16  `json:"status_in,omitempty"`
	ParticipantIDIn    []string `json:"participant_id_in,omitempty"`
	CreatedAfterNanos  *int64   `json:"created_after_nanos,omitempty"`
	CreatedBeforeNanos *int64   `json:"created_before_nanos,omitempty"`
	CreatedAtNanos     *int64   `json:"created_at_nanos,omitempty"`
	UpdatedAtNanos     *int64   `json:"updated_at_nanos,omitempty"`
	MessageCount       *int32   `json:"message_count,omitempty"`
	Status             *int16   `json:"status,omitempty"`
	ThreadID           string   `json:"thread_id"`
}

func EncodeOrganizationThreadPageToken(organizationID uuid.UUID, filter OrganizationThreadFilter, sort OrganizationThreadSort, cursor OrganizationThreadCursor) (string, error) {
	payload := organizationThreadPageToken{
		OrganizationID: organizationID.String(),
		SortField:      int(sort.Field),
		SortDirection:  int(sort.Direction),
		ThreadID:       cursor.ThreadID.String(),
	}
	if len(filter.StatusIn) > 0 {
		statusValues := make([]int16, len(filter.StatusIn))
		for i, status := range filter.StatusIn {
			statusValues[i] = int16(status)
		}
		payload.StatusIn = statusValues
	}
	if len(filter.ParticipantIDs) > 0 {
		payload.ParticipantIDIn = uuidsToStrings(filter.ParticipantIDs)
	}
	if filter.CreatedAfter != nil {
		createdAfter := filter.CreatedAfter.UnixNano()
		payload.CreatedAfterNanos = &createdAfter
	}
	if filter.CreatedBefore != nil {
		createdBefore := filter.CreatedBefore.UnixNano()
		payload.CreatedBeforeNanos = &createdBefore
	}
	switch sort.Field {
	case OrganizationThreadSortFieldCreated:
		createdAt := cursor.CreatedAt.UnixNano()
		payload.CreatedAtNanos = &createdAt
	case OrganizationThreadSortFieldUpdated:
		updatedAt := cursor.UpdatedAt.UnixNano()
		payload.UpdatedAtNanos = &updatedAt
	case OrganizationThreadSortFieldMessageCount:
		messageCount := cursor.MessageCount
		payload.MessageCount = &messageCount
	case OrganizationThreadSortFieldStatus:
		status := int16(cursor.Status)
		payload.Status = &status
	default:
		return "", fmt.Errorf("unsupported sort field: %d", sort.Field)
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func DecodeOrganizationThreadPageToken(token string) (uuid.UUID, OrganizationThreadFilter, OrganizationThreadSort, OrganizationThreadCursor, error) {
	if token == "" {
		return uuid.UUID{}, OrganizationThreadFilter{}, OrganizationThreadSort{}, OrganizationThreadCursor{}, errors.New("empty token")
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return uuid.UUID{}, OrganizationThreadFilter{}, OrganizationThreadSort{}, OrganizationThreadCursor{}, fmt.Errorf("decode token: %w", err)
	}
	var payload organizationThreadPageToken
	if err := json.Unmarshal(data, &payload); err != nil {
		return uuid.UUID{}, OrganizationThreadFilter{}, OrganizationThreadSort{}, OrganizationThreadCursor{}, fmt.Errorf("unmarshal token: %w", err)
	}
	organizationID, err := uuid.Parse(payload.OrganizationID)
	if err != nil {
		return uuid.UUID{}, OrganizationThreadFilter{}, OrganizationThreadSort{}, OrganizationThreadCursor{}, fmt.Errorf("parse organization id: %w", err)
	}
	sortField := OrganizationThreadSortField(payload.SortField)
	if sortField == OrganizationThreadSortFieldUnspecified {
		sortField = OrganizationThreadSortFieldCreated
	}
	if err := validateOrganizationThreadSortField(sortField); err != nil {
		return uuid.UUID{}, OrganizationThreadFilter{}, OrganizationThreadSort{}, OrganizationThreadCursor{}, err
	}
	sortDirection := SortDirection(payload.SortDirection)
	if sortDirection == SortDirectionUnspecified {
		sortDirection = SortDirectionDesc
	}
	if err := validateSortDirection(sortDirection); err != nil {
		return uuid.UUID{}, OrganizationThreadFilter{}, OrganizationThreadSort{}, OrganizationThreadCursor{}, err
	}

	filter := OrganizationThreadFilter{}
	if len(payload.StatusIn) > 0 {
		statusValues := make([]ThreadStatus, 0, len(payload.StatusIn))
		for _, raw := range payload.StatusIn {
			status, err := ParseThreadStatus(raw)
			if err != nil {
				return uuid.UUID{}, OrganizationThreadFilter{}, OrganizationThreadSort{}, OrganizationThreadCursor{}, fmt.Errorf("parse status filter: %w", err)
			}
			statusValues = append(statusValues, status)
		}
		sort.Slice(statusValues, func(i, j int) bool { return statusValues[i] < statusValues[j] })
		filter.StatusIn = statusValues
	}
	if len(payload.ParticipantIDIn) > 0 {
		participantIDs := make([]uuid.UUID, 0, len(payload.ParticipantIDIn))
		for _, raw := range payload.ParticipantIDIn {
			participantID, err := uuid.Parse(raw)
			if err != nil {
				return uuid.UUID{}, OrganizationThreadFilter{}, OrganizationThreadSort{}, OrganizationThreadCursor{}, fmt.Errorf("parse participant id: %w", err)
			}
			participantIDs = append(participantIDs, participantID)
		}
		sort.Slice(participantIDs, func(i, j int) bool { return participantIDs[i].String() < participantIDs[j].String() })
		filter.ParticipantIDs = participantIDs
	}
	if payload.CreatedAfterNanos != nil {
		createdAfter := time.Unix(0, *payload.CreatedAfterNanos).UTC()
		filter.CreatedAfter = &createdAfter
	}
	if payload.CreatedBeforeNanos != nil {
		createdBefore := time.Unix(0, *payload.CreatedBeforeNanos).UTC()
		filter.CreatedBefore = &createdBefore
	}

	threadID, err := uuid.Parse(payload.ThreadID)
	if err != nil {
		return uuid.UUID{}, OrganizationThreadFilter{}, OrganizationThreadSort{}, OrganizationThreadCursor{}, fmt.Errorf("parse thread id: %w", err)
	}
	cursor := OrganizationThreadCursor{ThreadID: threadID}
	switch sortField {
	case OrganizationThreadSortFieldCreated:
		if payload.CreatedAtNanos == nil {
			return uuid.UUID{}, OrganizationThreadFilter{}, OrganizationThreadSort{}, OrganizationThreadCursor{}, errors.New("missing created_at cursor")
		}
		cursor.CreatedAt = time.Unix(0, *payload.CreatedAtNanos).UTC()
	case OrganizationThreadSortFieldUpdated:
		if payload.UpdatedAtNanos == nil {
			return uuid.UUID{}, OrganizationThreadFilter{}, OrganizationThreadSort{}, OrganizationThreadCursor{}, errors.New("missing updated_at cursor")
		}
		cursor.UpdatedAt = time.Unix(0, *payload.UpdatedAtNanos).UTC()
	case OrganizationThreadSortFieldMessageCount:
		if payload.MessageCount == nil {
			return uuid.UUID{}, OrganizationThreadFilter{}, OrganizationThreadSort{}, OrganizationThreadCursor{}, errors.New("missing message_count cursor")
		}
		cursor.MessageCount = *payload.MessageCount
	case OrganizationThreadSortFieldStatus:
		if payload.Status == nil {
			return uuid.UUID{}, OrganizationThreadFilter{}, OrganizationThreadSort{}, OrganizationThreadCursor{}, errors.New("missing status cursor")
		}
		status, err := ParseThreadStatus(*payload.Status)
		if err != nil {
			return uuid.UUID{}, OrganizationThreadFilter{}, OrganizationThreadSort{}, OrganizationThreadCursor{}, fmt.Errorf("parse status cursor: %w", err)
		}
		cursor.Status = status
	}

	return organizationID, filter, OrganizationThreadSort{Field: sortField, Direction: sortDirection}, cursor, nil
}

func validateOrganizationThreadSortField(field OrganizationThreadSortField) error {
	switch field {
	case OrganizationThreadSortFieldCreated, OrganizationThreadSortFieldUpdated, OrganizationThreadSortFieldMessageCount, OrganizationThreadSortFieldStatus:
		return nil
	default:
		return fmt.Errorf("invalid sort field: %d", field)
	}
}

func validateSortDirection(direction SortDirection) error {
	switch direction {
	case SortDirectionAsc, SortDirectionDesc:
		return nil
	default:
		return fmt.Errorf("invalid sort direction: %d", direction)
	}
}

type messagePageToken struct {
	OwnerID        string `json:"owner_id"`
	CreatedAtNanos int64  `json:"created_at_nanos"`
	MessageID      string `json:"message_id"`
}

func EncodeThreadMessagePageToken(threadID uuid.UUID, cursor MessageCursor) (string, error) {
	payload := messagePageToken{
		OwnerID:        threadID.String(),
		CreatedAtNanos: cursor.CreatedAt.UnixNano(),
		MessageID:      cursor.MessageID.String(),
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func DecodeThreadMessagePageToken(token string) (uuid.UUID, MessageCursor, error) {
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

func EncodeUnackedMessagePageToken(participantID uuid.UUID, cursor MessageCursor) (string, error) {
	payload := messagePageToken{
		OwnerID:        participantID.String(),
		CreatedAtNanos: cursor.CreatedAt.UnixNano(),
		MessageID:      cursor.MessageID.String(),
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func DecodeUnackedMessagePageToken(token string) (uuid.UUID, MessageCursor, error) {
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
