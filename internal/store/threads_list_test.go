package store

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestBuildOrganizationThreadsQueryFilters(t *testing.T) {
	organizationID := uuid.New()
	participantA := uuid.New()
	participantB := uuid.New()
	createdAfter := time.Now().UTC().Add(-48 * time.Hour)
	createdBefore := time.Now().UTC().Add(-24 * time.Hour)

	filter := OrganizationThreadFilter{
		StatusIn:       []ThreadStatus{ThreadStatusArchived, ThreadStatusActive},
		ParticipantIDs: []uuid.UUID{participantA, participantB},
		CreatedAfter:   &createdAfter,
		CreatedBefore:  &createdBefore,
	}
	sort := OrganizationThreadSort{Field: OrganizationThreadSortFieldCreated, Direction: SortDirectionDesc}

	query, args, err := buildOrganizationThreadsQuery(organizationID, filter, sort, nil, 5)
	if err != nil {
		t.Fatalf("build query: %v", err)
	}
	if !strings.Contains(query, "t.status = ANY($2)") {
		t.Fatalf("expected status filter, got %s", query)
	}
	if !strings.Contains(query, "tp.participant_id = ANY($3)") {
		t.Fatalf("expected participant filter, got %s", query)
	}
	if !strings.Contains(query, "t.created_at >= $4") {
		t.Fatalf("expected created_after filter, got %s", query)
	}
	if !strings.Contains(query, "t.created_at < $5") {
		t.Fatalf("expected created_before filter, got %s", query)
	}
	if !strings.Contains(query, "ORDER BY t.created_at DESC, t.id ASC") {
		t.Fatalf("expected deterministic order, got %s", query)
	}
	if len(args) != 6 {
		t.Fatalf("expected 6 args, got %d", len(args))
	}
	if args[0] != organizationID {
		t.Fatalf("expected organization arg %s, got %v", organizationID, args[0])
	}
	statusArg, ok := args[1].(pgtype.FlatArray[int16])
	if !ok {
		t.Fatalf("expected status array arg, got %T", args[1])
	}
	if !reflect.DeepEqual([]int16(statusArg), []int16{int16(ThreadStatusArchived), int16(ThreadStatusActive)}) {
		t.Fatalf("unexpected status args %v", statusArg)
	}
	participantArg, ok := args[2].(pgtype.FlatArray[uuid.UUID])
	if !ok {
		t.Fatalf("expected participant array arg, got %T", args[2])
	}
	if !reflect.DeepEqual([]uuid.UUID(participantArg), []uuid.UUID{participantA, participantB}) {
		t.Fatalf("unexpected participant args %v", participantArg)
	}
	if args[3] != createdAfter {
		t.Fatalf("expected created_after arg %v, got %v", createdAfter, args[3])
	}
	if args[4] != createdBefore {
		t.Fatalf("expected created_before arg %v, got %v", createdBefore, args[4])
	}
	if args[5] != 6 {
		t.Fatalf("expected limit arg 6, got %v", args[5])
	}
}

func TestBuildOrganizationThreadsQueryCreatedAscCursor(t *testing.T) {
	organizationID := uuid.New()
	cursor := OrganizationThreadCursor{CreatedAt: time.Now().UTC(), ThreadID: uuid.New()}
	sort := OrganizationThreadSort{Field: OrganizationThreadSortFieldCreated, Direction: SortDirectionAsc}

	query, args, err := buildOrganizationThreadsQuery(organizationID, OrganizationThreadFilter{}, sort, &cursor, 3)
	if err != nil {
		t.Fatalf("build query: %v", err)
	}
	if !strings.Contains(query, "AND (t.created_at > $2 OR (t.created_at = $2 AND t.id > $3))") {
		t.Fatalf("expected cursor filter, got %s", query)
	}
	if !strings.Contains(query, "ORDER BY t.created_at ASC, t.id ASC") {
		t.Fatalf("expected asc order, got %s", query)
	}
	if len(args) != 4 {
		t.Fatalf("expected 4 args, got %d", len(args))
	}
	if args[1] != cursor.CreatedAt {
		t.Fatalf("expected cursor time arg %v, got %v", cursor.CreatedAt, args[1])
	}
	if args[2] != cursor.ThreadID {
		t.Fatalf("expected cursor id arg %s, got %v", cursor.ThreadID, args[2])
	}
	if args[3] != 4 {
		t.Fatalf("expected limit arg 4, got %v", args[3])
	}
}

func TestBuildOrganizationThreadsQueryStatusDescCursor(t *testing.T) {
	organizationID := uuid.New()
	cursor := OrganizationThreadCursor{Status: ThreadStatusArchived, ThreadID: uuid.New()}
	sort := OrganizationThreadSort{Field: OrganizationThreadSortFieldStatus, Direction: SortDirectionDesc}

	query, args, err := buildOrganizationThreadsQuery(organizationID, OrganizationThreadFilter{}, sort, &cursor, 2)
	if err != nil {
		t.Fatalf("build query: %v", err)
	}
	if !strings.Contains(query, "AND (t.status < $2 OR (t.status = $2 AND t.id > $3))") {
		t.Fatalf("expected cursor filter, got %s", query)
	}
	if !strings.Contains(query, "ORDER BY t.status DESC, t.id ASC") {
		t.Fatalf("expected desc order, got %s", query)
	}
	if len(args) != 4 {
		t.Fatalf("expected 4 args, got %d", len(args))
	}
	if args[1] != int16(ThreadStatusArchived) {
		t.Fatalf("expected status arg %v, got %v", ThreadStatusArchived, args[1])
	}
	if args[2] != cursor.ThreadID {
		t.Fatalf("expected cursor id arg %s, got %v", cursor.ThreadID, args[2])
	}
	if args[3] != 3 {
		t.Fatalf("expected limit arg 3, got %v", args[3])
	}
}
