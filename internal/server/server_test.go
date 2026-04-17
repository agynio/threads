package server

import (
	"context"
	"testing"
	"time"

	agentsv1 "github.com/agynio/threads/.gen/go/agynio/api/agents/v1"
	identityv1 "github.com/agynio/threads/.gen/go/agynio/api/identity/v1"
	threadsv1 "github.com/agynio/threads/.gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/agynio/threads/internal/store"
)

type stubThreadStore struct {
	t                *testing.T
	createThreadFn   func(ctx context.Context, participants []store.ParticipantInput) (store.Thread, error)
	addParticipantFn func(ctx context.Context, threadID, participantID uuid.UUID, passive bool) (store.Thread, error)
}

func (s *stubThreadStore) unexpectedCall(method string) {
	s.t.Helper()
	s.t.Fatalf("unexpected %s call", method)
}

func (s *stubThreadStore) CreateThread(ctx context.Context, participants []store.ParticipantInput) (store.Thread, error) {
	s.t.Helper()
	if s.createThreadFn == nil {
		s.t.Fatalf("unexpected CreateThread call")
	}
	return s.createThreadFn(ctx, participants)
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

type stubAgentsService struct {
	t          *testing.T
	getAgentFn func(ctx context.Context, req *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error)
}

func (s *stubAgentsService) GetAgent(ctx context.Context, req *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
	s.t.Helper()
	if s.getAgentFn == nil {
		s.t.Fatalf("unexpected GetAgent call")
	}
	return s.getAgentFn(ctx, req, opts...)
}

func TestCreateThreadAgentInitiatorPassive(t *testing.T) {
	threadID := uuid.New()
	agentID := uuid.New()
	participantID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, participants []store.ParticipantInput) (store.Thread, error) {
			storeCalled = true
			if len(participants) != 2 {
				t.Fatalf("expected 2 participants, got %d", len(participants))
			}
			if participants[0].ID != agentID {
				t.Fatalf("expected initiator %s first, got %s", agentID, participants[0].ID)
			}
			if !participants[0].Passive {
				t.Fatalf("expected agent passive true")
			}
			if participants[1].ID != participantID {
				t.Fatalf("expected participant %s second, got %s", participantID, participants[1].ID)
			}
			if participants[1].Passive {
				t.Fatalf("expected participant passive false")
			}
			return store.Thread{
				ID:        threadID,
				Status:    store.ThreadStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
				Participants: []store.Participant{
					{ID: agentID, JoinedAt: now, Passive: true},
					{ID: participantID, JoinedAt: now, Passive: false},
				},
			}, nil
		},
	}

	srv := New(storeStub, nil, nil, nil, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", agentID.String(), "x-identity-type", "agent"),
	)
	resp, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{ParticipantIds: []string{participantID.String()}})
	if err != nil {
		t.Fatalf("CreateThread returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected CreateThread to be called")
	}
	agentParticipant := findProtoParticipant(resp.GetThread(), agentID)
	if agentParticipant == nil {
		t.Fatal("expected agent participant in response")
	}
	if !agentParticipant.GetPassive() {
		t.Fatal("expected agent participant passive true")
	}
}

func TestCreateThreadEmptyParticipantsWithAgentInitiator(t *testing.T) {
	threadID := uuid.New()
	agentID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, participants []store.ParticipantInput) (store.Thread, error) {
			storeCalled = true
			if len(participants) != 1 {
				t.Fatalf("expected 1 participant, got %d", len(participants))
			}
			if participants[0].ID != agentID {
				t.Fatalf("expected initiator %s, got %s", agentID, participants[0].ID)
			}
			if !participants[0].Passive {
				t.Fatalf("expected initiator passive true")
			}
			return store.Thread{
				ID:        threadID,
				Status:    store.ThreadStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
				Participants: []store.Participant{
					{ID: agentID, JoinedAt: now, Passive: true},
				},
			}, nil
		},
	}

	srv := New(storeStub, nil, nil, nil, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", agentID.String(), "x-identity-type", "agent"),
	)
	resp, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{})
	if err != nil {
		t.Fatalf("CreateThread returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected CreateThread to be called")
	}
	agentParticipant := findProtoParticipant(resp.GetThread(), agentID)
	if agentParticipant == nil {
		t.Fatal("expected agent participant in response")
	}
	if !agentParticipant.GetPassive() {
		t.Fatal("expected agent participant passive true")
	}
}

func TestCreateThreadUserInitiatorActive(t *testing.T) {
	threadID := uuid.New()
	userID := uuid.New()
	participantID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, participants []store.ParticipantInput) (store.Thread, error) {
			storeCalled = true
			if len(participants) != 2 {
				t.Fatalf("expected 2 participants, got %d", len(participants))
			}
			if participants[0].ID != userID {
				t.Fatalf("expected initiator %s first, got %s", userID, participants[0].ID)
			}
			if participants[0].Passive {
				t.Fatalf("expected initiator passive false")
			}
			if participants[1].ID != participantID {
				t.Fatalf("expected participant %s second, got %s", participantID, participants[1].ID)
			}
			if participants[1].Passive {
				t.Fatalf("expected participant passive false")
			}
			return store.Thread{
				ID:        threadID,
				Status:    store.ThreadStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
				Participants: []store.Participant{
					{ID: userID, JoinedAt: now, Passive: false},
					{ID: participantID, JoinedAt: now, Passive: false},
				},
			}, nil
		},
	}

	srv := New(storeStub, nil, nil, nil, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", userID.String(), "x-identity-type", "user"),
	)
	resp, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{ParticipantIds: []string{participantID.String()}})
	if err != nil {
		t.Fatalf("CreateThread returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected CreateThread to be called")
	}
	userParticipant := findProtoParticipant(resp.GetThread(), userID)
	if userParticipant == nil {
		t.Fatal("expected user participant in response")
	}
	if userParticipant.GetPassive() {
		t.Fatal("expected user participant passive false")
	}
}

func TestCreateThreadMissingIdentityMetadataDefaultsActive(t *testing.T) {
	threadID := uuid.New()
	participantID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, participants []store.ParticipantInput) (store.Thread, error) {
			storeCalled = true
			if len(participants) != 1 {
				t.Fatalf("expected 1 participant, got %d", len(participants))
			}
			if participants[0].ID != participantID {
				t.Fatalf("expected participant %s, got %s", participantID, participants[0].ID)
			}
			if participants[0].Passive {
				t.Fatalf("expected participant passive false")
			}
			return store.Thread{
				ID:        threadID,
				Status:    store.ThreadStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
				Participants: []store.Participant{
					{ID: participantID, JoinedAt: now, Passive: false},
				},
			}, nil
		},
	}

	srv := New(storeStub, nil, nil, nil, nil)
	resp, err := srv.CreateThread(context.Background(), &threadsv1.CreateThreadRequest{ParticipantIds: []string{participantID.String()}})
	if err != nil {
		t.Fatalf("CreateThread returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected CreateThread to be called")
	}
	participant := findProtoParticipant(resp.GetThread(), participantID)
	if participant == nil {
		t.Fatal("expected participant in response")
	}
	if participant.GetPassive() {
		t.Fatal("expected participant passive false")
	}
}

func TestCreateThreadMissingIdentityMetadataRejectsEmpty(t *testing.T) {
	srv := New(&stubThreadStore{t: t}, nil, nil, nil, nil)
	_, err := srv.CreateThread(context.Background(), &threadsv1.CreateThreadRequest{})
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

func TestCreateThreadRejectsInitiatorInParticipants(t *testing.T) {
	initiatorID := uuid.New()
	participantID := uuid.New()

	srv := New(&stubThreadStore{t: t}, nil, nil, nil, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", initiatorID.String(), "x-identity-type", "agent"),
	)
	_, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{ParticipantIds: []string{initiatorID.String(), participantID.String()}})
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

	srv := New(storeStub, nil, identityStub, nil, nil)
	orgIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-organization-id", uuid.New().String()))
	resp, err := srv.AddParticipant(ctx, &threadsv1.AddParticipantRequest{
		ThreadId:       threadID.String(),
		OrganizationId: &orgIDValue,
		Passive:        true,
		Participant: &threadsv1.ParticipantIdentifier{
			Identifier: &threadsv1.ParticipantIdentifier_ParticipantNickname{ParticipantNickname: "@agent-alpha"},
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

	srv := New(storeStub, nil, nil, nil, nil)
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

	srv := New(storeStub, nil, nil, nil, nil)
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

	srv := New(&stubThreadStore{t: t}, nil, &stubIdentityResolver{t: t}, nil, nil)
	_, err := srv.AddParticipant(context.Background(), &threadsv1.AddParticipantRequest{
		ThreadId: threadID.String(),
		Participant: &threadsv1.ParticipantIdentifier{
			Identifier: &threadsv1.ParticipantIdentifier_ParticipantNickname{ParticipantNickname: "@agent-alpha"},
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
	if st.Message() != "organization_id must be provided for participant_nickname" {
		t.Fatalf("expected organization_id error, got %s", st.Message())
	}
}

func TestAddParticipantNicknameUsesOrganizationIDFromMetadata(t *testing.T) {
	threadID := uuid.New()
	organizationID := uuid.New()
	participantID := uuid.New()
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
			return store.Thread{ID: threadID}, nil
		},
	}
	identityStub := &stubIdentityResolver{
		t: t,
		resolveFn: func(ctx context.Context, req *identityv1.ResolveNicknameRequest, opts ...grpc.CallOption) (*identityv1.ResolveNicknameResponse, error) {
			identityCalled = true
			if req.GetOrganizationId() != organizationID.String() {
				t.Fatalf("expected organization ID %s, got %s", organizationID, req.GetOrganizationId())
			}
			if req.GetNickname() != "agent-beta" {
				t.Fatalf("expected nickname agent-beta, got %s", req.GetNickname())
			}
			return &identityv1.ResolveNicknameResponse{IdentityId: participantID.String()}, nil
		},
	}

	srv := New(storeStub, nil, identityStub, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-organization-id", organizationID.String()))
	_, err := srv.AddParticipant(ctx, &threadsv1.AddParticipantRequest{
		ThreadId: threadID.String(),
		Participant: &threadsv1.ParticipantIdentifier{
			Identifier: &threadsv1.ParticipantIdentifier_ParticipantNickname{ParticipantNickname: "@agent-beta"},
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
}

func TestAddParticipantNicknameUsesOrganizationIDFromAgentIdentity(t *testing.T) {
	threadID := uuid.New()
	organizationID := uuid.New()
	agentID := uuid.New()
	participantID := uuid.New()
	storeCalled := false
	identityCalled := false
	agentCalled := false

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
	identityStub := &stubIdentityResolver{
		t: t,
		resolveFn: func(ctx context.Context, req *identityv1.ResolveNicknameRequest, opts ...grpc.CallOption) (*identityv1.ResolveNicknameResponse, error) {
			identityCalled = true
			if req.GetOrganizationId() != organizationID.String() {
				t.Fatalf("expected organization ID %s, got %s", organizationID, req.GetOrganizationId())
			}
			if req.GetNickname() != "agent-gamma" {
				t.Fatalf("expected nickname agent-gamma, got %s", req.GetNickname())
			}
			return &identityv1.ResolveNicknameResponse{IdentityId: participantID.String()}, nil
		},
	}
	agentsStub := &stubAgentsService{
		t: t,
		getAgentFn: func(ctx context.Context, req *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			agentCalled = true
			if req.GetId() != agentID.String() {
				t.Fatalf("expected agent ID %s, got %s", agentID, req.GetId())
			}
			return &agentsv1.GetAgentResponse{
				Agent: &agentsv1.Agent{OrganizationId: organizationID.String()},
			}, nil
		},
	}

	srv := New(storeStub, nil, identityStub, agentsStub, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", agentID.String(), "x-identity-type", "agent"),
	)
	_, err := srv.AddParticipant(ctx, &threadsv1.AddParticipantRequest{
		ThreadId: threadID.String(),
		Participant: &threadsv1.ParticipantIdentifier{
			Identifier: &threadsv1.ParticipantIdentifier_ParticipantNickname{ParticipantNickname: "@agent-gamma"},
		},
	})
	if err != nil {
		t.Fatalf("AddParticipant returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected AddParticipant to be called")
	}
	if !agentCalled {
		t.Fatal("expected GetAgent to be called")
	}
	if !identityCalled {
		t.Fatal("expected ResolveNickname to be called")
	}
}

func TestAddParticipantNicknameRejectsNonAgentIdentityTypes(t *testing.T) {
	threadID := uuid.New()
	identityID := uuid.New()
	identityTypes := []string{"user", "runner", "app"}

	for _, identityType := range identityTypes {
		t.Run(identityType, func(t *testing.T) {
			srv := New(&stubThreadStore{t: t}, nil, &stubIdentityResolver{t: t}, nil, nil)
			ctx := metadata.NewIncomingContext(
				context.Background(),
				metadata.Pairs("x-identity-id", identityID.String(), "x-identity-type", identityType),
			)
			_, err := srv.AddParticipant(ctx, &threadsv1.AddParticipantRequest{
				ThreadId: threadID.String(),
				Participant: &threadsv1.ParticipantIdentifier{
					Identifier: &threadsv1.ParticipantIdentifier_ParticipantNickname{ParticipantNickname: "@agent"},
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
			if st.Message() != "organization_id must be provided for participant_nickname" {
				t.Fatalf("expected organization_id error, got %s", st.Message())
			}
		})
	}
}

func TestAddParticipantNicknameAgentLookupError(t *testing.T) {
	threadID := uuid.New()
	agentID := uuid.New()

	agentsStub := &stubAgentsService{
		t: t,
		getAgentFn: func(ctx context.Context, req *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return nil, status.Error(codes.Internal, "agent down")
		},
	}

	srv := New(&stubThreadStore{t: t}, nil, &stubIdentityResolver{t: t}, agentsStub, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", agentID.String(), "x-identity-type", "agent"),
	)
	_, err := srv.AddParticipant(ctx, &threadsv1.AddParticipantRequest{
		ThreadId: threadID.String(),
		Participant: &threadsv1.ParticipantIdentifier{
			Identifier: &threadsv1.ParticipantIdentifier_ParticipantNickname{ParticipantNickname: "@agent"},
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.Internal {
		t.Fatalf("expected Internal, got %s: %s", st.Code(), st.Message())
	}
}

func TestAddParticipantNicknameAgentMissingOrganization(t *testing.T) {
	threadID := uuid.New()
	agentID := uuid.New()

	agentsStub := &stubAgentsService{
		t: t,
		getAgentFn: func(ctx context.Context, req *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{}}, nil
		},
	}

	srv := New(&stubThreadStore{t: t}, nil, &stubIdentityResolver{t: t}, agentsStub, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", agentID.String(), "x-identity-type", "agent"),
	)
	_, err := srv.AddParticipant(ctx, &threadsv1.AddParticipantRequest{
		ThreadId: threadID.String(),
		Participant: &threadsv1.ParticipantIdentifier{
			Identifier: &threadsv1.ParticipantIdentifier_ParticipantNickname{ParticipantNickname: "@agent"},
		},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.Internal {
		t.Fatalf("expected Internal, got %s: %s", st.Code(), st.Message())
	}
}

func findProtoParticipant(thread *threadsv1.Thread, id uuid.UUID) *threadsv1.Participant {
	if thread == nil {
		return nil
	}
	value := id.String()
	for _, participant := range thread.GetParticipants() {
		if participant.GetId() == value {
			return participant
		}
	}
	return nil
}
