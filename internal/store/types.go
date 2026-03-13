package store

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrThreadNotFound         = errors.New("thread not found")
	ErrThreadArchived         = errors.New("thread is archived")
	ErrParticipantNotInThread = errors.New("participant not in thread")
)

type ThreadStatus int16

const (
	ThreadStatusUnspecified ThreadStatus = 0
	ThreadStatusActive      ThreadStatus = 1
	ThreadStatusArchived    ThreadStatus = 2
)

func ParseThreadStatus(value int16) (ThreadStatus, error) {
	switch ThreadStatus(value) {
	case ThreadStatusActive, ThreadStatusArchived:
		return ThreadStatus(value), nil
	default:
		return ThreadStatusUnspecified, errors.New("invalid thread status")
	}
}

type Thread struct {
	ID           uuid.UUID
	Participants []Participant
	Status       ThreadStatus
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type Participant struct {
	ID       uuid.UUID
	JoinedAt time.Time
}

type Message struct {
	ID        uuid.UUID
	ThreadID  uuid.UUID
	SenderID  uuid.UUID
	Body      string
	FileIDs   []uuid.UUID
	CreatedAt time.Time
}

type ThreadCursor struct {
	UpdatedAt time.Time
	ThreadID  uuid.UUID
}

type MessageCursor struct {
	CreatedAt time.Time
	MessageID uuid.UUID
}

type ThreadListResult struct {
	Threads    []Thread
	NextCursor *ThreadCursor
}

type MessageListResult struct {
	Messages   []Message
	NextCursor *MessageCursor
}
