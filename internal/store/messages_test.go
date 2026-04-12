package store

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestBuildUnackedMessagesQueryIncludesThreadFilter(t *testing.T) {
	participantID := uuid.New()
	threadID := uuid.New()

	query, args := buildUnackedMessagesQuery(participantID, &threadID, nil, 10)
	if !strings.Contains(query, "m.thread_id = $2") {
		t.Fatalf("expected thread filter in query, got %s", query)
	}
	if len(args) != 3 {
		t.Fatalf("expected 3 args, got %d", len(args))
	}
	if args[0] != participantID {
		t.Fatalf("expected participant ID arg %s, got %v", participantID, args[0])
	}
	if args[1] != threadID {
		t.Fatalf("expected thread ID arg %s, got %v", threadID, args[1])
	}
	if args[2] != 11 {
		t.Fatalf("expected limit arg 11, got %v", args[2])
	}
}

func TestBuildUnackedMessagesQueryWithoutThreadFilter(t *testing.T) {
	participantID := uuid.New()

	query, args := buildUnackedMessagesQuery(participantID, nil, nil, 10)
	if strings.Contains(query, "AND m.thread_id") {
		t.Fatalf("expected no thread filter, got %s", query)
	}
	if len(args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(args))
	}
	if args[0] != participantID {
		t.Fatalf("expected participant ID arg %s, got %v", participantID, args[0])
	}
	if args[1] != 11 {
		t.Fatalf("expected limit arg 11, got %v", args[1])
	}
}

func TestBuildUnackedMessagesQueryWithThreadAndCursor(t *testing.T) {
	participantID := uuid.New()
	threadID := uuid.New()
	cursor := &MessageCursor{CreatedAt: time.Unix(0, 123).UTC(), MessageID: uuid.New()}

	query, args := buildUnackedMessagesQuery(participantID, &threadID, cursor, 5)
	if !strings.Contains(query, "m.thread_id = $2") {
		t.Fatalf("expected thread filter in query, got %s", query)
	}
	if !strings.Contains(query, "(m.created_at, m.id) > ($3, $4)") {
		t.Fatalf("expected cursor filter in query, got %s", query)
	}
	if len(args) != 5 {
		t.Fatalf("expected 5 args, got %d", len(args))
	}
	if args[0] != participantID {
		t.Fatalf("expected participant ID arg %s, got %v", participantID, args[0])
	}
	if args[1] != threadID {
		t.Fatalf("expected thread ID arg %s, got %v", threadID, args[1])
	}
	if args[2] != cursor.CreatedAt {
		t.Fatalf("expected cursor time arg %v, got %v", cursor.CreatedAt, args[2])
	}
	if args[3] != cursor.MessageID {
		t.Fatalf("expected cursor message ID arg %s, got %v", cursor.MessageID, args[3])
	}
	if args[4] != 6 {
		t.Fatalf("expected limit arg 6, got %v", args[4])
	}
}
