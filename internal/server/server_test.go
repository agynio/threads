package server

import (
	"context"
	"testing"
	"time"

	identityv1 "github.com/agynio/threads/.gen/go/agynio/api/identity/v1"
	threadsv1 "github.com/agynio/threads/.gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"

	"github.com/agynio/threads/internal/store"
)

type fakeThreadStore struct {
	t                     *testing.T
	expectedThreadID      uuid.UUID
	expectedParticipantID uuid.UUID
	expectedPassive       bool
	called                bool
	thread                store.Thread
}

func (f *fakeThreadStore) CreateThread(context.Context, []uuid.UUID) (store.Thread, error) {
	panic("unexpected CreateThread call")
}

func (f *fakeThreadStore) ArchiveThread(context.Context, uuid.UUID) (store.Thread, error) {
	panic("unexpected ArchiveThread call")
}

func (f *fakeThreadStore) AddParticipant(ctx context.Context, threadID, participantID uuid.UUID, passive bool) (store.Thread, error) {
	f.t.Helper()
	f.called = true
	if threadID != f.expectedThreadID {
		f.t.Fatalf("expected thread ID %s, got %s", f.expectedThreadID, threadID)
	}
	if participantID != f.expectedParticipantID {
		f.t.Fatalf("expected participant ID %s, got %s", f.expectedParticipantID, participantID)
	}
	if passive != f.expectedPassive {
		f.t.Fatalf("expected passive %v, got %v", f.expectedPassive, passive)
	}
	return f.thread, nil
}

func (f *fakeThreadStore) SendMessage(context.Context, uuid.UUID, uuid.UUID, string, []uuid.UUID) (store.SendMessageResult, error) {
	panic("unexpected SendMessage call")
}

func (f *fakeThreadStore) ListThreads(context.Context, uuid.UUID, int32, *store.ThreadCursor) (store.ThreadListResult, error) {
	panic("unexpected ListThreads call")
}

func (f *fakeThreadStore) ListMessages(context.Context, uuid.UUID, int32, *store.MessageCursor) (store.MessageListResult, error) {
	panic("unexpected ListMessages call")
}

func (f *fakeThreadStore) ListUnackedMessages(context.Context, uuid.UUID, *uuid.UUID, int32, *store.MessageCursor) (store.MessageListResult, error) {
	panic("unexpected ListUnackedMessages call")
}

func (f *fakeThreadStore) AckMessages(context.Context, uuid.UUID, []uuid.UUID) (int32, error) {
	panic("unexpected AckMessages call")
}

type fakeIdentityResolver struct {
	t                *testing.T
	expectedOrgID    string
	expectedNickname string
	responseID       string
	called           bool
}

func (f *fakeIdentityResolver) ResolveNickname(ctx context.Context, req *identityv1.ResolveNicknameRequest, opts ...grpc.CallOption) (*identityv1.ResolveNicknameResponse, error) {
	f.t.Helper()
	f.called = true
	if req.GetOrganizationId() != f.expectedOrgID {
		f.t.Fatalf("expected organization ID %s, got %s", f.expectedOrgID, req.GetOrganizationId())
	}
	if req.GetNickname() != f.expectedNickname {
		f.t.Fatalf("expected nickname %s, got %s", f.expectedNickname, req.GetNickname())
	}
	return &identityv1.ResolveNicknameResponse{IdentityId: f.responseID}, nil
}

func TestAddParticipantWithNicknamePassesPassive(t *testing.T) {
	threadID := uuid.New()
	organizationID := uuid.New()
	participantID := uuid.New()
	now := time.Now().UTC()

	storeStub := &fakeThreadStore{
		t:                     t,
		expectedThreadID:      threadID,
		expectedParticipantID: participantID,
		expectedPassive:       true,
		thread: store.Thread{
			ID:        threadID,
			Status:    store.ThreadStatusActive,
			CreatedAt: now,
			UpdatedAt: now,
			Participants: []store.Participant{
				{ID: participantID, JoinedAt: now, Passive: true},
			},
		},
	}
	identityStub := &fakeIdentityResolver{
		t:                t,
		expectedOrgID:    organizationID.String(),
		expectedNickname: "agent-alpha",
		responseID:       participantID.String(),
	}

	srv := New(storeStub, nil, identityStub)
	orgIDValue := organizationID.String()
	resp, err := srv.AddParticipant(context.Background(), &threadsv1.AddParticipantRequest{
		ThreadId:       threadID.String(),
		OrganizationId: &orgIDValue,
		Passive:        true,
		Participant: &threadsv1.ParticipantIdentifier{
			Identifier: &threadsv1.ParticipantIdentifier_ParticipantNickname{ParticipantNickname: "agent-alpha"},
		},
	})
	if err != nil {
		t.Fatalf("AddParticipant returned error: %v", err)
	}
	if !storeStub.called {
		t.Fatal("expected AddParticipant to be called")
	}
	if !identityStub.called {
		t.Fatal("expected ResolveNickname to be called")
	}
	if resp.GetThread() == nil || len(resp.GetThread().GetParticipants()) != 1 {
		t.Fatalf("expected 1 participant, got %v", resp.GetThread().GetParticipants())
	}
	if !resp.GetThread().GetParticipants()[0].GetPassive() {
		t.Fatal("expected passive participant to be true")
	}
}
