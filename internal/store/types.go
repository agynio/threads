package store

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrThreadNotFound            = errors.New("thread not found")
	ErrThreadArchived            = errors.New("thread is archived")
	ErrThreadDegraded            = errors.New("thread is degraded")
	ErrThreadOrganizationMissing = errors.New("thread organization_id missing")
	ErrParticipantNotInThread    = errors.New("participant not in thread")
)

type ThreadStatus int16

const (
	ThreadStatusUnspecified ThreadStatus = 0
	ThreadStatusActive      ThreadStatus = 1
	ThreadStatusArchived    ThreadStatus = 2
	ThreadStatusDegraded    ThreadStatus = 3
)

func ParseThreadStatus(value int16) (ThreadStatus, error) {
	switch ThreadStatus(value) {
	case ThreadStatusActive, ThreadStatusArchived, ThreadStatusDegraded:
		return ThreadStatus(value), nil
	default:
		return ThreadStatusUnspecified, errors.New("invalid thread status")
	}
}

type Thread struct {
	ID             uuid.UUID
	OrganizationID *uuid.UUID
	MessageCount   int32
	Participants   []Participant
	Status         ThreadStatus
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type Participant struct {
	ID       uuid.UUID
	JoinedAt time.Time
	Passive  bool
}

type ParticipantInput struct {
	ID      uuid.UUID
	Passive bool
}

type Message struct {
	ID        uuid.UUID
	ThreadID  uuid.UUID
	SenderID  uuid.UUID
	Body      string
	FileIDs   []uuid.UUID
	CreatedAt time.Time
}

type MessageOrder int

const (
	MessageOrderOldestFirst MessageOrder = iota
	MessageOrderNewestFirst
)

type SortDirection int

const (
	SortDirectionUnspecified SortDirection = iota
	SortDirectionAsc
	SortDirectionDesc
)

type OrganizationThreadSortField int

const (
	OrganizationThreadSortFieldUnspecified OrganizationThreadSortField = iota
	OrganizationThreadSortFieldCreated
	OrganizationThreadSortFieldUpdated
	OrganizationThreadSortFieldMessageCount
	OrganizationThreadSortFieldStatus
)

type OrganizationThreadSort struct {
	Field     OrganizationThreadSortField
	Direction SortDirection
}

type OrganizationThreadFilter struct {
	StatusIn       []ThreadStatus
	ParticipantIDs []uuid.UUID
	CreatedAfter   *time.Time
	CreatedBefore  *time.Time
}

type ThreadCursor struct {
	UpdatedAt time.Time
	ThreadID  uuid.UUID
}

type OrganizationThreadCursor struct {
	ThreadID     uuid.UUID
	CreatedAt    time.Time
	UpdatedAt    time.Time
	MessageCount int32
	Status       ThreadStatus
}

type MessageCursor struct {
	CreatedAt time.Time
	MessageID uuid.UUID
}

type ThreadListResult struct {
	Threads    []Thread
	NextCursor *ThreadCursor
}

type OrganizationThreadListResult struct {
	Threads    []Thread
	NextCursor *OrganizationThreadCursor
}

type MessageListResult struct {
	Messages   []Message
	NextCursor *MessageCursor
}
