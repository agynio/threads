package store

import (
	"strings"
	"testing"

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
