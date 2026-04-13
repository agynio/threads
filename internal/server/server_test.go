package server

import (
	"context"
	"testing"
	"time"

	identityv1 "github.com/agynio/threads/.gen/go/agynio/api/identity/v1"
	threadsv1 "github.com/agynio/threads/.gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agynio/threads/internal/store"
)

type stubThreadStore struct {
	t                *testing.T
	addParticipantFn func(ctx context.Context, threadID, participantID uuid.UUID, passive bool) (store.Thread, error)
}

func (s *stubThreadStore) unexpectedCall(method string) {
	s.t.Helper()
	s.t.Fatalf("unexpected %s call", method)
}

func (s *stubThreadStore) CreateThread(context.Context, []uuid.UUID) (store.Thread, error) {
	s.unexpectedCall("CreateThread")
	return store.Thread{}, nil
}

func (s *stubThreadStore) ArchiveThread(context.Context, uuid.UUID) (store.Thread, error) {
	s.unexpectedCall("ArchiveThread")
	return store.Thread{}, nil
}

func (s *stubThreadStore) AddParticipant(ctx context.Context, threadID, participantID uuid.UUID, passive bool) (store.Thread, error) {
	s.t.Helper()
	if s.addParticipantFn == nil {
		s.t.Fatalf("unexpected AddParticipant call")
	}
	return s.addParticipantFn(ctx, threadID, participantID, passive)
}

func (s *stubThreadStore) SendMessage(context.Context, uuid.UUID, uuid.UUID, string, []uuid.UUID) (store.SendMessageResult, error) {
	s.unexpectedCall("SendMessage")
	return store.SendMessageResult{}, nil
}

func (s *stubThreadStore) ListThreads(context.Context, uuid.UUID, int32, *store.ThreadCursor) (store.ThreadListResult, error) {
	s.unexpectedCall("ListThreads")
	return store.ThreadListResult{}, nil
}

func (s *stubThreadStore) ListMessages(context.Context, uuid.UUID, int32, *store.MessageCursor) (store.MessageListResult, error) {
	s.unexpectedCall("ListMessages")
	return store.MessageListResult{}, nil
}

func (s *stubThreadStore) ListUnackedMessages(context.Context, uuid.UUID, *uuid.UUID, int32, *store.MessageCursor) (store.MessageListResult, error) {
	s.unexpectedCall("ListUnackedMessages")
	return store.MessageListResult{}, nil
}

func (s *stubThreadStore) AckMessages(context.Context, uuid.UUID, []uuid.UUID) (int32, error) {
	s.unexpectedCall("AckMessages")
	return 0, nil
}

type stubIdentityResolver struct {
	t         *testing.T
	resolveFn func(ctx context.Context, req *identityv1.ResolveNicknameRequest, opts ...grpc.CallOption) (*identityv1.ResolveNicknameResponse, error)
}

func (s *stubIdentityResolver) ResolveNickname(ctx context.Context, req *identityv1.ResolveNicknameRequest, opts ...grpc.CallOption) (*identityv1.ResolveNicknameResponse, error) {
	s.t.Helper()
	if s.resolveFn == nil {
		s.t.Fatalf("unexpected ResolveNickname call")
	}
	return s.resolveFn(ctx, req, opts...)
}

func TestAddParticipantWithNicknamePassesPassive(t *testing.T) {
	threadID := uuid.New()
	organizationID := uuid.New()
	participantID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false
	identityCalled := false

	storeStub := &stubThreadStore{
		t: t,
		addParticipantFn: func(ctx context.Context, threadArg, participantArg uuid.UUID, passive bool) (store.Thread, error) {
			storeCalled = true
			if threadArg != threadID {
				t.Fatalf("expected thread ID %s, got %s", threadID, threadArg)
			}
			if participantArg != participantID {
				t.Fatalf("expected participant ID %s, got %s", participantID, participantArg)
			}
			if !passive {
				t.Fatalf("expected passive true, got %v", passive)
			}
			return store.Thread{
				ID:        threadID,
				Status:    store.ThreadStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
				Participants: []store.Participant{
					{ID: participantID, JoinedAt: now, Passive: true},
				},
			}, nil
		},
	}
	identityStub := &stubIdentityResolver{
		t: t,
		resolveFn: func(ctx context.Context, req *identityv1.ResolveNicknameRequest, opts ...grpc.CallOption) (*identityv1.ResolveNicknameResponse, error) {
			identityCalled = true
			if req.GetOrganizationId() != organizationID.String() {
				t.Fatalf("expected organization ID %s, got %s", organizationID, req.GetOrganizationId())
			}
			if req.GetNickname() != "agent-alpha" {
				t.Fatalf("expected nickname agent-alpha, got %s", req.GetNickname())
			}
			return &identityv1.ResolveNicknameResponse{IdentityId: participantID.String()}, nil
		},
	}

	srv := New(storeStub, nil, identityStub, nil)
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
	if !storeCalled {
		t.Fatal("expected AddParticipant to be called")
	}
	if !identityCalled {
		t.Fatal("expected ResolveNickname to be called")
	}
	if resp.GetThread() == nil || len(resp.GetThread().GetParticipants()) != 1 {
		t.Fatalf("expected 1 participant, got %v", resp.GetThread().GetParticipants())
	}
	if !resp.GetThread().GetParticipants()[0].GetPassive() {
		t.Fatal("expected passive participant to be true")
	}
}

func TestAddParticipantWithParticipantIDOneof(t *testing.T) {
	threadID := uuid.New()
	participantID := uuid.New()
	storeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		addParticipantFn: func(ctx context.Context, threadArg, participantArg uuid.UUID, passive bool) (store.Thread, error) {
			storeCalled = true
			if threadArg != threadID {
				t.Fatalf("expected thread ID %s, got %s", threadID, threadArg)
			}
			if participantArg != participantID {
				t.Fatalf("expected participant ID %s, got %s", participantID, participantArg)
			}
			if passive {
				t.Fatalf("expected passive false, got %v", passive)
			}
			return store.Thread{ID: threadID}, nil
		},
	}

	srv := New(storeStub, nil, nil, nil)
	_, err := srv.AddParticipant(context.Background(), &threadsv1.AddParticipantRequest{
		ThreadId: threadID.String(),
		Participant: &threadsv1.ParticipantIdentifier{
			Identifier: &threadsv1.ParticipantIdentifier_ParticipantId{ParticipantId: participantID.String()},
		},
	})
	if err != nil {
		t.Fatalf("AddParticipant returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected AddParticipant to be called")
	}
}

func TestAddParticipantWithLegacyParticipantID(t *testing.T) {
	threadID := uuid.New()
	participantID := uuid.New()
	storeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		addParticipantFn: func(ctx context.Context, threadArg, participantArg uuid.UUID, passive bool) (store.Thread, error) {
			storeCalled = true
			if threadArg != threadID {
				t.Fatalf("expected thread ID %s, got %s", threadID, threadArg)
			}
			if participantArg != participantID {
				t.Fatalf("expected participant ID %s, got %s", participantID, participantArg)
			}
			return store.Thread{ID: threadID}, nil
		},
	}

	srv := New(storeStub, nil, nil, nil)
	_, err := srv.AddParticipant(context.Background(), &threadsv1.AddParticipantRequest{
		ThreadId:      threadID.String(),
		ParticipantId: participantID.String(),
	})
	if err != nil {
		t.Fatalf("AddParticipant returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected AddParticipant to be called")
	}
}

func TestAddParticipantNicknameRequiresOrganizationID(t *testing.T) {
	threadID := uuid.New()

	srv := New(&stubThreadStore{t: t}, nil, &stubIdentityResolver{t: t}, nil)
	_, err := srv.AddParticipant(context.Background(), &threadsv1.AddParticipantRequest{
		ThreadId: threadID.String(),
		Participant: &threadsv1.ParticipantIdentifier{
			Identifier: &threadsv1.ParticipantIdentifier_ParticipantNickname{ParticipantNickname: "agent-alpha"},
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %s: %s", st.Code(), st.Message())
	}
}
