package server

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	agentsv1 "github.com/agynio/threads/.gen/go/agynio/api/agents/v1"
	authorizationv1 "github.com/agynio/threads/.gen/go/agynio/api/authorization/v1"
	identityv1 "github.com/agynio/threads/.gen/go/agynio/api/identity/v1"
	threadsv1 "github.com/agynio/threads/.gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/agynio/threads/internal/store"
)

type stubThreadStore struct {
	t                                  *testing.T
	createThreadFn                     func(ctx context.Context, organizationID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error)
	archiveThreadFn                    func(ctx context.Context, threadID uuid.UUID) (store.Thread, error)
	degradeThreadFn                    func(ctx context.Context, threadID uuid.UUID) (store.Thread, error)
	addParticipantFn                   func(ctx context.Context, threadID, participantID uuid.UUID, passive bool) (store.Thread, error)
	sendMessageFn                      func(ctx context.Context, threadID, senderID uuid.UUID, body string, fileIDs []uuid.UUID, messageRecipientIDs []uuid.UUID, agentInstanceRecipientIDs []uuid.UUID) (store.SendMessageResult, error)
	claimPendingAgentInboxDeliveriesFn func(ctx context.Context, limit int32) ([]store.AgentInboxDelivery, error)
	markAgentInboxDeliveryDeliveredFn  func(ctx context.Context, messageID, agentInstanceID, claimID uuid.UUID) error
	markAgentInboxDeliveryFailedFn     func(ctx context.Context, messageID, agentInstanceID, claimID uuid.UUID, deliveryError string) error
	getThreadFn                        func(ctx context.Context, threadID uuid.UUID) (store.Thread, error)
	listOrgThreadsFn                   func(ctx context.Context, organizationID uuid.UUID, filter store.OrganizationThreadFilter, sort store.OrganizationThreadSort, pageSize int32, cursor *store.OrganizationThreadCursor) (store.OrganizationThreadListResult, error)
	listMessagesFn                     func(ctx context.Context, threadID uuid.UUID, pageSize int32, cursor *store.MessageCursor, order store.MessageOrder) (store.MessageListResult, error)
	listUnackedFn                      func(ctx context.Context, participantID uuid.UUID, threadID *uuid.UUID, pageSize int32, cursor *store.MessageCursor) (store.MessageListResult, error)
	unackedCountsFn                    func(ctx context.Context, participantID uuid.UUID) (map[uuid.UUID]int32, error)
	ackMessagesFn                      func(ctx context.Context, participantID uuid.UUID, messageIDs []uuid.UUID) (int32, error)
}

func (s *stubThreadStore) unexpectedCall(method string) {
	s.t.Helper()
	s.t.Fatalf("unexpected %s call", method)
}

func (s *stubThreadStore) CreateThread(ctx context.Context, organizationID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error) {
	s.t.Helper()
	if s.createThreadFn == nil {
		s.t.Fatalf("unexpected CreateThread call")
	}
	return s.createThreadFn(ctx, organizationID, participants)
}

func (s *stubThreadStore) ArchiveThread(ctx context.Context, threadID uuid.UUID) (store.Thread, error) {
	if s.archiveThreadFn == nil {
		s.unexpectedCall("ArchiveThread")
		return store.Thread{}, nil
	}
	return s.archiveThreadFn(ctx, threadID)
}

func (s *stubThreadStore) DegradeThread(ctx context.Context, threadID uuid.UUID) (store.Thread, error) {
	if s.degradeThreadFn == nil {
		s.unexpectedCall("DegradeThread")
		return store.Thread{}, nil
	}
	return s.degradeThreadFn(ctx, threadID)
}

func (s *stubThreadStore) AddParticipant(ctx context.Context, threadID, participantID uuid.UUID, passive bool) (store.Thread, error) {
	s.t.Helper()
	if s.addParticipantFn == nil {
		s.t.Fatalf("unexpected AddParticipant call")
	}
	return s.addParticipantFn(ctx, threadID, participantID, passive)
}

func (s *stubThreadStore) SendMessage(ctx context.Context, threadID uuid.UUID, senderID uuid.UUID, body string, fileIDs []uuid.UUID, messageRecipientIDs []uuid.UUID, agentInstanceRecipientIDs []uuid.UUID) (store.SendMessageResult, error) {
	if s.sendMessageFn == nil {
		s.unexpectedCall("SendMessage")
		return store.SendMessageResult{}, nil
	}
	return s.sendMessageFn(ctx, threadID, senderID, body, fileIDs, messageRecipientIDs, agentInstanceRecipientIDs)
}

func (s *stubThreadStore) ClaimPendingAgentInboxDeliveries(ctx context.Context, limit int32) ([]store.AgentInboxDelivery, error) {
	s.t.Helper()
	if s.claimPendingAgentInboxDeliveriesFn == nil {
		return nil, nil
	}
	return s.claimPendingAgentInboxDeliveriesFn(ctx, limit)
}

func (s *stubThreadStore) MarkAgentInboxDeliveryDelivered(ctx context.Context, messageID, agentInstanceID, claimID uuid.UUID) error {
	s.t.Helper()
	if s.markAgentInboxDeliveryDeliveredFn == nil {
		return nil
	}
	return s.markAgentInboxDeliveryDeliveredFn(ctx, messageID, agentInstanceID, claimID)
}

func (s *stubThreadStore) MarkAgentInboxDeliveryFailed(ctx context.Context, messageID, agentInstanceID, claimID uuid.UUID, deliveryError string) error {
	s.t.Helper()
	if s.markAgentInboxDeliveryFailedFn == nil {
		return nil
	}
	return s.markAgentInboxDeliveryFailedFn(ctx, messageID, agentInstanceID, claimID, deliveryError)
}

func (s *stubThreadStore) GetThread(ctx context.Context, threadID uuid.UUID) (store.Thread, error) {
	s.t.Helper()
	if s.getThreadFn == nil {
		s.t.Fatalf("unexpected GetThread call")
	}
	return s.getThreadFn(ctx, threadID)
}

func (s *stubThreadStore) ListThreads(context.Context, uuid.UUID, int32, *store.ThreadCursor) (store.ThreadListResult, error) {
	s.unexpectedCall("ListThreads")
	return store.ThreadListResult{}, nil
}

func (s *stubThreadStore) ListOrganizationThreads(ctx context.Context, organizationID uuid.UUID, filter store.OrganizationThreadFilter, sort store.OrganizationThreadSort, pageSize int32, cursor *store.OrganizationThreadCursor) (store.OrganizationThreadListResult, error) {
	s.t.Helper()
	if s.listOrgThreadsFn == nil {
		s.t.Fatalf("unexpected ListOrganizationThreads call")
	}
	return s.listOrgThreadsFn(ctx, organizationID, filter, sort, pageSize, cursor)
}

func (s *stubThreadStore) ListMessages(ctx context.Context, threadID uuid.UUID, pageSize int32, cursor *store.MessageCursor, order store.MessageOrder) (store.MessageListResult, error) {
	s.t.Helper()
	if s.listMessagesFn == nil {
		s.t.Fatalf("unexpected ListMessages call")
	}
	return s.listMessagesFn(ctx, threadID, pageSize, cursor, order)
}

func (s *stubThreadStore) ListUnackedMessages(ctx context.Context, participantID uuid.UUID, threadID *uuid.UUID, pageSize int32, cursor *store.MessageCursor) (store.MessageListResult, error) {
	if s.listUnackedFn == nil {
		s.unexpectedCall("ListUnackedMessages")
		return store.MessageListResult{}, nil
	}
	return s.listUnackedFn(ctx, participantID, threadID, pageSize, cursor)
}

func (s *stubThreadStore) GetUnackedMessageCounts(ctx context.Context, participantID uuid.UUID) (map[uuid.UUID]int32, error) {
	if s.unackedCountsFn == nil {
		s.unexpectedCall("GetUnackedMessageCounts")
		return nil, nil
	}
	return s.unackedCountsFn(ctx, participantID)
}

func (s *stubThreadStore) AckMessages(ctx context.Context, participantID uuid.UUID, messageIDs []uuid.UUID) (int32, error) {
	if s.ackMessagesFn == nil {
		s.unexpectedCall("AckMessages")
		return 0, nil
	}
	return s.ackMessagesFn(ctx, participantID, messageIDs)
}

type stubIdentityResolver struct {
	t           *testing.T
	resolveFn   func(ctx context.Context, req *identityv1.ResolveNicknameRequest, opts ...grpc.CallOption) (*identityv1.ResolveNicknameResponse, error)
	batchFn     func(ctx context.Context, req *identityv1.BatchGetNicknamesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetNicknamesResponse, error)
	typeBatchFn func(ctx context.Context, req *identityv1.BatchGetIdentityTypesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetIdentityTypesResponse, error)
}

func (s *stubIdentityResolver) ResolveNickname(ctx context.Context, req *identityv1.ResolveNicknameRequest, opts ...grpc.CallOption) (*identityv1.ResolveNicknameResponse, error) {
	s.t.Helper()
	if s.resolveFn == nil {
		s.t.Fatalf("unexpected ResolveNickname call")
	}
	return s.resolveFn(ctx, req, opts...)
}

func (s *stubIdentityResolver) BatchGetNicknames(ctx context.Context, req *identityv1.BatchGetNicknamesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetNicknamesResponse, error) {
	s.t.Helper()
	if s.batchFn == nil {
		s.t.Fatalf("unexpected BatchGetNicknames call")
	}
	return s.batchFn(ctx, req, opts...)
}

func (s *stubIdentityResolver) BatchGetIdentityTypes(ctx context.Context, req *identityv1.BatchGetIdentityTypesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetIdentityTypesResponse, error) {
	s.t.Helper()
	if s.typeBatchFn == nil {
		entries := make([]*identityv1.IdentityTypeEntry, len(req.GetIdentityIds()))
		for i, identityID := range req.GetIdentityIds() {
			entries[i] = &identityv1.IdentityTypeEntry{IdentityId: identityID, IdentityType: identityv1.IdentityType_IDENTITY_TYPE_USER}
		}
		return &identityv1.BatchGetIdentityTypesResponse{Entries: entries}, nil
	}
	return s.typeBatchFn(ctx, req, opts...)
}

type stubAgentsService struct {
	t             *testing.T
	getAgentFn    func(ctx context.Context, req *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error)
	createFn      func(ctx context.Context, req *agentsv1.CreateInstanceRequest, opts ...grpc.CallOption) (*agentsv1.CreateInstanceResponse, error)
	getInstanceFn func(ctx context.Context, req *agentsv1.GetInstanceRequest, opts ...grpc.CallOption) (*agentsv1.GetInstanceResponse, error)
	fanoutFn      func(ctx context.Context, req *agentsv1.FanoutInboxItemRequest, opts ...grpc.CallOption) (*agentsv1.FanoutInboxItemResponse, error)
}

func (s *stubAgentsService) GetAgent(ctx context.Context, req *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
	s.t.Helper()
	if s.getAgentFn == nil {
		s.t.Fatalf("unexpected GetAgent call")
	}
	return s.getAgentFn(ctx, req, opts...)
}

func (s *stubAgentsService) CreateInstance(ctx context.Context, req *agentsv1.CreateInstanceRequest, opts ...grpc.CallOption) (*agentsv1.CreateInstanceResponse, error) {
	s.t.Helper()
	if s.createFn == nil {
		s.t.Fatalf("unexpected CreateInstance call")
	}
	return s.createFn(ctx, req, opts...)
}

func (s *stubAgentsService) GetInstance(ctx context.Context, req *agentsv1.GetInstanceRequest, opts ...grpc.CallOption) (*agentsv1.GetInstanceResponse, error) {
	s.t.Helper()
	if s.getInstanceFn == nil {
		s.t.Fatalf("unexpected GetInstance call")
	}
	return s.getInstanceFn(ctx, req, opts...)
}

func (s *stubAgentsService) FanoutInboxItem(ctx context.Context, req *agentsv1.FanoutInboxItemRequest, opts ...grpc.CallOption) (*agentsv1.FanoutInboxItemResponse, error) {
	s.t.Helper()
	if s.fanoutFn == nil {
		s.t.Fatalf("unexpected FanoutInboxItem call")
	}
	return s.fanoutFn(ctx, req, opts...)
}

type stubAuthorizationService struct {
	t       *testing.T
	checkFn func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error)
	writeFn func(ctx context.Context, req *authorizationv1.WriteRequest, opts ...grpc.CallOption) (*authorizationv1.WriteResponse, error)
}

type stubNotifier struct {
	t         *testing.T
	publishFn func(ctx context.Context, threadID, messageID uuid.UUID, recipients []uuid.UUID) error
}

func (s *stubNotifier) PublishMessageCreated(ctx context.Context, threadID, messageID uuid.UUID, recipients []uuid.UUID) error {
	s.t.Helper()
	if s.publishFn == nil {
		return nil
	}
	return s.publishFn(ctx, threadID, messageID, recipients)
}

type stubMeteringRecorder struct {
	t                     *testing.T
	recordThreadCreatedFn func(ctx context.Context, orgID, threadID uuid.UUID, createdAt time.Time) error
	recordMessageSentFn   func(ctx context.Context, orgID, threadID, messageID uuid.UUID, createdAt time.Time) error
}

func (s *stubMeteringRecorder) RecordThreadCreated(ctx context.Context, orgID, threadID uuid.UUID, createdAt time.Time) error {
	s.t.Helper()
	if s.recordThreadCreatedFn == nil {
		s.t.Fatalf("unexpected RecordThreadCreated call")
	}
	return s.recordThreadCreatedFn(ctx, orgID, threadID, createdAt)
}

func (s *stubMeteringRecorder) RecordMessageSent(ctx context.Context, orgID, threadID, messageID uuid.UUID, createdAt time.Time) error {
	s.t.Helper()
	if s.recordMessageSentFn == nil {
		s.t.Fatalf("unexpected RecordMessageSent call")
	}
	return s.recordMessageSentFn(ctx, orgID, threadID, messageID, createdAt)
}

func (s *stubAuthorizationService) Check(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
	s.t.Helper()
	if s.checkFn == nil {
		s.t.Fatalf("unexpected Check call")
	}
	return s.checkFn(ctx, req, opts...)
}

func (s *stubAuthorizationService) Write(ctx context.Context, req *authorizationv1.WriteRequest, opts ...grpc.CallOption) (*authorizationv1.WriteResponse, error) {
	s.t.Helper()
	if s.writeFn == nil {
		return &authorizationv1.WriteResponse{}, nil
	}
	return s.writeFn(ctx, req, opts...)
}

func allowAuthStub(t *testing.T) *stubAuthorizationService {
	t.Helper()
	return &stubAuthorizationService{
		t: t,
		checkFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}
}

func TestCreateThreadRecordsUsageWithCreatedThreadOrganization(t *testing.T) {
	threadID := uuid.New()
	organizationID := uuid.New()
	identityID := uuid.New()
	participantID := uuid.New()
	now := time.Now().UTC()
	recorded := make(chan struct{}, 1)

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, orgID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error) {
			if orgID != organizationID {
				t.Fatalf("expected organization %s, got %s", organizationID, orgID)
			}
			return store.Thread{
				ID:             threadID,
				OrganizationID: &organizationID,
				MessageCount:   0,
				Status:         store.ThreadStatusActive,
				CreatedAt:      now,
				UpdatedAt:      now,
				Participants: []store.Participant{
					{ID: identityID, JoinedAt: now, Passive: false},
					{ID: participantID, JoinedAt: now, Passive: false},
				},
			}, nil
		},
	}
	meteringStub := &stubMeteringRecorder{
		t: t,
		recordThreadCreatedFn: func(ctx context.Context, orgID, recordedThreadID uuid.UUID, createdAt time.Time) error {
			if orgID != organizationID {
				t.Fatalf("expected metering organization %s, got %s", organizationID, orgID)
			}
			if recordedThreadID != threadID {
				t.Fatalf("expected metering thread %s, got %s", threadID, recordedThreadID)
			}
			if !createdAt.Equal(now) {
				t.Fatalf("expected metering created_at %s, got %s", now, createdAt)
			}
			recorded <- struct{}{}
			return nil
		},
	}

	srv := New(storeStub, nil, allowAuthStub(t), &stubIdentityResolver{t: t}, nil, meteringStub)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", identityID.String(), "x-identity-type", "user"),
	)
	_, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{
		OrganizationId: &[]string{organizationID.String()}[0],
		ParticipantIds: []string{participantID.String()},
	})
	if err != nil {
		t.Fatalf("CreateThread returned error: %v", err)
	}
	select {
	case <-recorded:
	case <-time.After(time.Second):
		t.Fatal("expected thread usage to be recorded")
	}
}

func TestCreateThreadParticipantIDsRequireIdentityResolver(t *testing.T) {
	organizationID := uuid.New()
	identityID := uuid.New()
	participantID := uuid.New()
	storeStub := &stubThreadStore{t: t}
	authStub := allowAuthStub(t)
	srv := New(storeStub, nil, authStub, nil, nil, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", identityID.String(), "x-identity-type", "user", "x-organization-id", organizationID.String()),
	)
	_, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{ParticipantIds: []string{participantID.String()}})
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

func TestCreateThreadAgentInitiatorPassive(t *testing.T) {
	threadID := uuid.New()
	organizationID := uuid.New()
	agentID := uuid.New()
	participantID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, orgID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error) {
			storeCalled = true
			if orgID != organizationID {
				t.Fatalf("expected organization %s, got %s", organizationID, orgID)
			}
			if len(participants) != 2 {
				t.Fatalf("expected 2 participants, got %d", len(participants))
			}
			if participants[0].ID != agentID {
				t.Fatalf("expected initiator %s first, got %s", agentID, participants[0].ID)
			}
			if participants[0].Passive {
				t.Fatalf("expected agent passive false")
			}
			if participants[1].ID != participantID {
				t.Fatalf("expected participant %s second, got %s", participantID, participants[1].ID)
			}
			if participants[1].Passive {
				t.Fatalf("expected participant passive false")
			}
			return store.Thread{
				ID:             threadID,
				OrganizationID: &organizationID,
				MessageCount:   0,
				Status:         store.ThreadStatusActive,
				CreatedAt:      now,
				UpdatedAt:      now,
				Participants: []store.Participant{
					{ID: agentID, JoinedAt: now, Passive: false},
					{ID: participantID, JoinedAt: now, Passive: false},
				},
			}, nil
		},
	}

	authStub := allowAuthStub(t)
	srv := New(storeStub, nil, authStub, &stubIdentityResolver{t: t}, nil, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", agentID.String(), "x-identity-type", "agent", "x-organization-id", organizationID.String()),
	)
	resp, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{
		Participants: []*threadsv1.ParticipantIdentifier{
			{Identifier: &threadsv1.ParticipantIdentifier_ParticipantId{ParticipantId: participantID.String()}},
		},
	})
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
	if agentParticipant.GetPassive() {
		t.Fatal("expected agent participant passive false")
	}
}

func TestCreateThreadEmptyParticipantsWithAgentInitiator(t *testing.T) {
	threadID := uuid.New()
	organizationID := uuid.New()
	agentID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, orgID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error) {
			storeCalled = true
			if orgID != organizationID {
				t.Fatalf("expected organization %s, got %s", organizationID, orgID)
			}
			if len(participants) != 1 {
				t.Fatalf("expected 1 participant, got %d", len(participants))
			}
			if participants[0].ID != agentID {
				t.Fatalf("expected initiator %s, got %s", agentID, participants[0].ID)
			}
			if participants[0].Passive {
				t.Fatalf("expected initiator passive false")
			}
			return store.Thread{
				ID:             threadID,
				OrganizationID: &organizationID,
				MessageCount:   0,
				Status:         store.ThreadStatusActive,
				CreatedAt:      now,
				UpdatedAt:      now,
				Participants: []store.Participant{
					{ID: agentID, JoinedAt: now, Passive: false},
				},
			}, nil
		},
	}

	authStub := allowAuthStub(t)
	srv := New(storeStub, nil, authStub, &stubIdentityResolver{t: t}, nil, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", agentID.String(), "x-identity-type", "agent", "x-organization-id", organizationID.String()),
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
	if agentParticipant.GetPassive() {
		t.Fatal("expected agent participant passive false")
	}
}

func TestCreateThreadUserInitiatorActive(t *testing.T) {
	threadID := uuid.New()
	organizationID := uuid.New()
	userID := uuid.New()
	participantID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, orgID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error) {
			storeCalled = true
			if orgID != organizationID {
				t.Fatalf("expected organization %s, got %s", organizationID, orgID)
			}
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
				ID:             threadID,
				OrganizationID: &organizationID,
				MessageCount:   0,
				Status:         store.ThreadStatusActive,
				CreatedAt:      now,
				UpdatedAt:      now,
				Participants: []store.Participant{
					{ID: userID, JoinedAt: now, Passive: false},
					{ID: participantID, JoinedAt: now, Passive: false},
				},
			}, nil
		},
	}

	authStub := allowAuthStub(t)
	srv := New(storeStub, nil, authStub, &stubIdentityResolver{t: t}, nil, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", userID.String(), "x-identity-type", "user", "x-organization-id", organizationID.String()),
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

func TestCreateThreadMissingIdentityMetadataRejected(t *testing.T) {
	organizationID := uuid.New()
	participantID := uuid.New()
	storeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, orgID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error) {
			storeCalled = true
			return store.Thread{}, nil
		},
	}

	srv := New(storeStub, nil, nil, nil, nil, nil)
	orgIDValue := organizationID.String()
	_, err := srv.CreateThread(context.Background(), &threadsv1.CreateThreadRequest{
		OrganizationId: &orgIDValue,
		ParticipantIds: []string{participantID.String()},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %s: %s", st.Code(), st.Message())
	}
	if storeCalled {
		t.Fatal("expected CreateThread not to be called")
	}
}

func TestCreateThreadRejectsMismatchedOrganizationID(t *testing.T) {
	organizationID := uuid.New()
	otherOrganizationID := uuid.New()
	participantID := uuid.New()
	otherOrgValue := otherOrganizationID.String()

	srv := New(&stubThreadStore{t: t}, nil, nil, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-organization-id", organizationID.String()))
	_, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{
		OrganizationId: &otherOrgValue,
		ParticipantIds: []string{participantID.String()},
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
	if st.Message() != "organization_id does not match identity organization" {
		t.Fatalf("expected organization_id mismatch, got %s", st.Message())
	}
}

func TestCreateThreadNicknameUsesOrganizationID(t *testing.T) {
	threadID := uuid.New()
	organizationID := uuid.New()
	participantID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false
	identityCalled := false

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, orgID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error) {
			storeCalled = true
			if orgID != organizationID {
				t.Fatalf("expected organization %s, got %s", organizationID, orgID)
			}
			if len(participants) != 1 {
				t.Fatalf("expected 1 participant, got %d", len(participants))
			}
			if participants[0].ID != participantID {
				t.Fatalf("expected participant %s, got %s", participantID, participants[0].ID)
			}
			return store.Thread{
				ID:             threadID,
				OrganizationID: &organizationID,
				MessageCount:   0,
				Status:         store.ThreadStatusActive,
				CreatedAt:      now,
				UpdatedAt:      now,
				Participants: []store.Participant{
					{ID: participantID, JoinedAt: now, Passive: false},
				},
			}, nil
		},
	}
	identityStub := &stubIdentityResolver{
		t: t,
		resolveFn: func(ctx context.Context, req *identityv1.ResolveNicknameRequest, opts ...grpc.CallOption) (*identityv1.ResolveNicknameResponse, error) {
			identityCalled = true
			md, ok := metadata.FromOutgoingContext(ctx)
			if !ok {
				t.Fatal("expected outgoing metadata")
			}
			if value := md.Get(identityIDMetadataKey); len(value) != 1 || value[0] != identityID.String() {
				t.Fatalf("expected %s %s, got %v", identityIDMetadataKey, identityID, value)
			}
			if value := md.Get(identityTypeMetadataKey); len(value) != 0 {
				t.Fatalf("expected no %s metadata, got %v", identityTypeMetadataKey, value)
			}
			if value := md.Get(organizationIDMetadataKey); len(value) != 1 || value[0] != organizationID.String() {
				t.Fatalf("expected %s %s, got %v", organizationIDMetadataKey, organizationID, value)
			}
			if req.GetOrganizationId() != organizationID.String() {
				t.Fatalf("expected organization ID %s, got %s", organizationID, req.GetOrganizationId())
			}
			if req.GetNickname() != "agent-delta" {
				t.Fatalf("expected nickname agent-delta, got %s", req.GetNickname())
			}
			return &identityv1.ResolveNicknameResponse{IdentityId: participantID.String()}, nil
		},
	}

	authStub := allowAuthStub(t)
	srv := New(storeStub, nil, authStub, identityStub, nil, nil)
	orgIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", identityID.String(), "x-organization-id", organizationID.String()),
	)
	resp, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{
		OrganizationId: &orgIDValue,
		Participants: []*threadsv1.ParticipantIdentifier{
			{Identifier: &threadsv1.ParticipantIdentifier_ParticipantNickname{ParticipantNickname: "@agent-delta"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateThread returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected CreateThread to be called")
	}
	if !identityCalled {
		t.Fatal("expected ResolveNickname to be called")
	}
	if resp.GetThread() == nil || len(resp.GetThread().GetParticipants()) != 1 {
		t.Fatalf("expected 1 participant, got %v", resp.GetThread().GetParticipants())
	}
}

func TestCreateThreadNicknameUsesOrganizationIDFromAgentIdentity(t *testing.T) {
	threadID := uuid.New()
	organizationID := uuid.New()
	agentID := uuid.New()
	participantID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false
	identityCalled := false
	agentCalled := false

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, orgID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error) {
			storeCalled = true
			if orgID != organizationID {
				t.Fatalf("expected organization %s, got %s", organizationID, orgID)
			}
			if len(participants) != 2 {
				t.Fatalf("expected 2 participants, got %d", len(participants))
			}
			if participants[0].ID != agentID {
				t.Fatalf("expected initiator %s first, got %s", agentID, participants[0].ID)
			}
			if participants[0].Passive {
				t.Fatalf("expected initiator passive false")
			}
			if participants[1].ID != participantID {
				t.Fatalf("expected participant %s second, got %s", participantID, participants[1].ID)
			}
			return store.Thread{
				ID:             threadID,
				OrganizationID: &organizationID,
				MessageCount:   0,
				Status:         store.ThreadStatusActive,
				CreatedAt:      now,
				UpdatedAt:      now,
				Participants: []store.Participant{
					{ID: agentID, JoinedAt: now, Passive: false},
					{ID: participantID, JoinedAt: now, Passive: false},
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
			if req.GetNickname() != "agent-epsilon" {
				t.Fatalf("expected nickname agent-epsilon, got %s", req.GetNickname())
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
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{OrganizationId: organizationID.String()}}, nil
		},
	}

	authStub := allowAuthStub(t)
	srv := New(storeStub, nil, authStub, identityStub, agentsStub, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", agentID.String(), "x-identity-type", "agent"),
	)
	_, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{
		Participants: []*threadsv1.ParticipantIdentifier{
			{Identifier: &threadsv1.ParticipantIdentifier_ParticipantNickname{ParticipantNickname: "@agent-epsilon"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateThread returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected CreateThread to be called")
	}
	if !agentCalled {
		t.Fatal("expected GetAgent to be called")
	}
	if !identityCalled {
		t.Fatal("expected ResolveNickname to be called")
	}
}

func TestCreateThreadMixedParticipantIdentifiers(t *testing.T) {
	threadID := uuid.New()
	organizationID := uuid.New()
	participantID := uuid.New()
	nicknameID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false
	identityCalled := false

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, orgID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error) {
			storeCalled = true
			if orgID != organizationID {
				t.Fatalf("expected organization %s, got %s", organizationID, orgID)
			}
			if len(participants) != 2 {
				t.Fatalf("expected 2 participants, got %d", len(participants))
			}
			if participants[0].ID != participantID {
				t.Fatalf("expected participant %s first, got %s", participantID, participants[0].ID)
			}
			if participants[1].ID != nicknameID {
				t.Fatalf("expected participant %s second, got %s", nicknameID, participants[1].ID)
			}
			return store.Thread{
				ID:             threadID,
				OrganizationID: &organizationID,
				MessageCount:   0,
				Status:         store.ThreadStatusActive,
				CreatedAt:      now,
				UpdatedAt:      now,
				Participants: []store.Participant{
					{ID: participantID, JoinedAt: now, Passive: false},
					{ID: nicknameID, JoinedAt: now, Passive: false},
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
			if req.GetNickname() != "agent-zeta" {
				t.Fatalf("expected nickname agent-zeta, got %s", req.GetNickname())
			}
			return &identityv1.ResolveNicknameResponse{IdentityId: nicknameID.String()}, nil
		},
	}

	identityID := uuid.New()
	authStub := allowAuthStub(t)
	srv := New(storeStub, nil, authStub, identityStub, nil, nil)
	orgIDValue := organizationID.String()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{
		OrganizationId: &orgIDValue,
		Participants: []*threadsv1.ParticipantIdentifier{
			{Identifier: &threadsv1.ParticipantIdentifier_ParticipantId{ParticipantId: participantID.String()}},
			{Identifier: &threadsv1.ParticipantIdentifier_ParticipantNickname{ParticipantNickname: "@agent-zeta"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateThread returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected CreateThread to be called")
	}
	if !identityCalled {
		t.Fatal("expected ResolveNickname to be called")
	}
}

func TestCreateThreadMissingIdentityMetadataRejectsEmpty(t *testing.T) {
	srv := New(&stubThreadStore{t: t}, nil, nil, nil, nil, nil)
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
	if st.Message() != "participant_ids or participants must be provided" {
		t.Fatalf("expected participant error, got %s", st.Message())
	}
}

func TestCreateThreadRejectsMixedParticipantFields(t *testing.T) {
	participantID := uuid.New()

	srv := New(&stubThreadStore{t: t}, nil, nil, nil, nil, nil)
	_, err := srv.CreateThread(context.Background(), &threadsv1.CreateThreadRequest{
		ParticipantIds: []string{participantID.String()},
		Participants: []*threadsv1.ParticipantIdentifier{
			{Identifier: &threadsv1.ParticipantIdentifier_ParticipantId{ParticipantId: participantID.String()}},
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
	if st.Message() != "participant_ids and participants are mutually exclusive" {
		t.Fatalf("expected mutual exclusivity error, got %s", st.Message())
	}
}

func TestCreateThreadNicknameRequiresOrganizationIDForUser(t *testing.T) {
	userID := uuid.New()

	srv := New(&stubThreadStore{t: t}, nil, nil, &stubIdentityResolver{t: t}, nil, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", userID.String(), "x-identity-type", "user"),
	)
	_, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{
		Participants: []*threadsv1.ParticipantIdentifier{
			{Identifier: &threadsv1.ParticipantIdentifier_ParticipantNickname{ParticipantNickname: "@agent"}},
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

func TestCreateThreadDedupesInitiatorInParticipantIDs(t *testing.T) {
	initiatorID := uuid.New()
	participantID := uuid.New()

	assertCreateThreadDedupesInitiator(t, initiatorID, participantID, &threadsv1.CreateThreadRequest{ParticipantIds: []string{initiatorID.String(), participantID.String()}})
}

func TestCreateThreadDedupesInitiatorInParticipants(t *testing.T) {
	initiatorID := uuid.New()
	participantID := uuid.New()

	assertCreateThreadDedupesInitiator(t, initiatorID, participantID, &threadsv1.CreateThreadRequest{
		Participants: []*threadsv1.ParticipantIdentifier{
			{Identifier: &threadsv1.ParticipantIdentifier_ParticipantId{ParticipantId: initiatorID.String()}},
			{Identifier: &threadsv1.ParticipantIdentifier_ParticipantId{ParticipantId: participantID.String()}},
		},
	})
}

func assertCreateThreadDedupesInitiator(t *testing.T, initiatorID, participantID uuid.UUID, req *threadsv1.CreateThreadRequest) {
	t.Helper()
	threadID := uuid.New()
	organizationID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false
	req.OrganizationId = proto.String(organizationID.String())

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, orgID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error) {
			storeCalled = true
			if orgID != organizationID {
				t.Fatalf("expected organization %s, got %s", organizationID, orgID)
			}
			if len(participants) != 2 {
				t.Fatalf("expected 2 participants, got %d", len(participants))
			}
			if participants[0].ID != initiatorID {
				t.Fatalf("expected initiator %s first, got %s", initiatorID, participants[0].ID)
			}
			if participants[0].Passive {
				t.Fatal("expected agent initiator to be active")
			}
			if participants[1].ID != participantID {
				t.Fatalf("expected participant %s second, got %s", participantID, participants[1].ID)
			}
			return store.Thread{
				ID:             threadID,
				OrganizationID: &organizationID,
				MessageCount:   0,
				Status:         store.ThreadStatusActive,
				CreatedAt:      now,
				UpdatedAt:      now,
				Participants: []store.Participant{
					{ID: initiatorID, JoinedAt: now, Passive: false},
					{ID: participantID, JoinedAt: now, Passive: false},
				},
			}, nil
		},
	}

	srv := New(storeStub, nil, allowAuthStub(t), &stubIdentityResolver{t: t}, nil, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", initiatorID.String(), "x-identity-type", "agent", "x-organization-id", organizationID.String()),
	)
	_, err := srv.CreateThread(ctx, req)
	if err != nil {
		t.Fatalf("CreateThread returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected CreateThread to be called")
	}
}

func TestCreateThreadAuthorizationDenied(t *testing.T) {
	organizationID := uuid.New()
	identityID := uuid.New()
	participantID := uuid.New()
	storeCalled := false
	authCalled := false

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, orgID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error) {
			storeCalled = true
			return store.Thread{}, nil
		},
	}
	authStub := &stubAuthorizationService{
		t: t,
		checkFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
			authCalled = true
			if req.GetTupleKey() == nil {
				t.Fatal("expected tuple key")
			}
			if req.GetTupleKey().GetRelation() != "can_create_thread" {
				t.Fatalf("expected relation can_create_thread, got %s", req.GetTupleKey().GetRelation())
			}
			expectedUser := identityObjectPrefix + identityID.String()
			if req.GetTupleKey().GetUser() != expectedUser {
				t.Fatalf("expected user %s, got %s", expectedUser, req.GetTupleKey().GetUser())
			}
			expectedObject := organizationObjectPrefix + organizationID.String()
			if req.GetTupleKey().GetObject() != expectedObject {
				t.Fatalf("expected object %s, got %s", expectedObject, req.GetTupleKey().GetObject())
			}
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(storeStub, nil, authStub, nil, nil, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", identityID.String(), "x-identity-type", "user", "x-organization-id", organizationID.String()),
	)
	_, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{ParticipantIds: []string{participantID.String()}})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s: %s", st.Code(), st.Message())
	}
	if !authCalled {
		t.Fatal("expected authorization check")
	}
	if storeCalled {
		t.Fatal("expected CreateThread not to be called")
	}
}

func TestCreateThreadWritesAuthorizationTuples(t *testing.T) {
	threadID := uuid.New()
	organizationID := uuid.New()
	identityID := uuid.New()
	participantID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false
	checkCalled := false
	writeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, orgID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error) {
			storeCalled = true
			if orgID != organizationID {
				t.Fatalf("expected organization %s, got %s", organizationID, orgID)
			}
			if len(participants) != 2 {
				t.Fatalf("expected 2 participants, got %d", len(participants))
			}
			return store.Thread{
				ID:             threadID,
				OrganizationID: &organizationID,
				MessageCount:   0,
				Status:         store.ThreadStatusActive,
				CreatedAt:      now,
				UpdatedAt:      now,
				Participants: []store.Participant{
					{ID: identityID, JoinedAt: now, Passive: false},
					{ID: participantID, JoinedAt: now, Passive: false},
				},
			}, nil
		},
	}
	authStub := &stubAuthorizationService{
		t: t,
		checkFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
			checkCalled = true
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
		writeFn: func(ctx context.Context, req *authorizationv1.WriteRequest, opts ...grpc.CallOption) (*authorizationv1.WriteResponse, error) {
			writeCalled = true
			if len(req.GetDeletes()) != 0 {
				t.Fatalf("expected no deletes, got %d", len(req.GetDeletes()))
			}
			expected := map[string]struct{}{
				identityObjectPrefix + identityID.String() + "|participant|" + threadObjectPrefix + threadID.String():    {},
				identityObjectPrefix + participantID.String() + "|participant|" + threadObjectPrefix + threadID.String(): {},
				organizationObjectPrefix + organizationID.String() + "|org|" + threadObjectPrefix + threadID.String():    {},
			}
			if len(req.GetWrites()) != len(expected) {
				t.Fatalf("expected %d writes, got %d", len(expected), len(req.GetWrites()))
			}
			for _, tuple := range req.GetWrites() {
				key := tuple.GetUser() + "|" + tuple.GetRelation() + "|" + tuple.GetObject()
				if _, ok := expected[key]; !ok {
					t.Fatalf("unexpected tuple %s", key)
				}
			}
			return &authorizationv1.WriteResponse{}, nil
		},
	}

	srv := New(storeStub, nil, authStub, &stubIdentityResolver{t: t}, nil, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", identityID.String(), "x-identity-type", "user", "x-organization-id", organizationID.String()),
	)
	_, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{ParticipantIds: []string{participantID.String()}})
	if err != nil {
		t.Fatalf("CreateThread returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected CreateThread to be called")
	}
	if !checkCalled {
		t.Fatal("expected authorization check")
	}
	if !writeCalled {
		t.Fatal("expected authorization write")
	}
}

func TestAddParticipantRejectsPassive(t *testing.T) {
	threadID := uuid.New()
	participantID := uuid.New()
	identityID := uuid.New()

	srv := New(&stubThreadStore{t: t}, nil, allowAuthStub(t), nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.AddParticipant(ctx, &threadsv1.AddParticipantRequest{ThreadId: threadID.String(), ParticipantId: participantID.String(), Passive: true})
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

func TestAddParticipantWithParticipantIDOneof(t *testing.T) {
	threadID := uuid.New()
	participantID := uuid.New()
	now := time.Now().UTC()
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
			return store.Thread{
				ID:        threadID,
				Status:    store.ThreadStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
			}, nil
		},
	}

	identityID := uuid.New()
	authStub := allowAuthStub(t)
	srv := New(storeStub, nil, authStub, &stubIdentityResolver{t: t}, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.AddParticipant(ctx, &threadsv1.AddParticipantRequest{
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
	now := time.Now().UTC()
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
			return store.Thread{
				ID:        threadID,
				Status:    store.ThreadStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
			}, nil
		},
	}

	identityID := uuid.New()
	authStub := allowAuthStub(t)
	srv := New(storeStub, nil, authStub, &stubIdentityResolver{t: t}, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.AddParticipant(ctx, &threadsv1.AddParticipantRequest{
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

func TestAddParticipantAuthorizationDenied(t *testing.T) {
	threadID := uuid.New()
	participantID := uuid.New()
	identityID := uuid.New()
	storeCalled := false
	authCalled := false

	storeStub := &stubThreadStore{
		t: t,
		addParticipantFn: func(ctx context.Context, threadArg, participantArg uuid.UUID, passive bool) (store.Thread, error) {
			storeCalled = true
			return store.Thread{}, nil
		},
	}
	authStub := &stubAuthorizationService{
		t: t,
		checkFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
			authCalled = true
			if req.GetTupleKey() == nil {
				t.Fatal("expected tuple key")
			}
			if req.GetTupleKey().GetRelation() != "can_add_participant" {
				t.Fatalf("expected relation can_add_participant, got %s", req.GetTupleKey().GetRelation())
			}
			expectedObject := threadObjectPrefix + threadID.String()
			if req.GetTupleKey().GetObject() != expectedObject {
				t.Fatalf("expected object %s, got %s", expectedObject, req.GetTupleKey().GetObject())
			}
			expectedUser := identityObjectPrefix + identityID.String()
			if req.GetTupleKey().GetUser() != expectedUser {
				t.Fatalf("expected user %s, got %s", expectedUser, req.GetTupleKey().GetUser())
			}
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(storeStub, nil, authStub, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.AddParticipant(ctx, &threadsv1.AddParticipantRequest{ThreadId: threadID.String(), ParticipantId: participantID.String()})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s: %s", st.Code(), st.Message())
	}
	if !authCalled {
		t.Fatal("expected authorization check")
	}
	if storeCalled {
		t.Fatal("expected AddParticipant not to be called")
	}
}

func TestAddParticipantWritesAuthorizationTuple(t *testing.T) {
	threadID := uuid.New()
	participantID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false
	writeCalled := false

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
	authStub := &stubAuthorizationService{
		t: t,
		checkFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
		writeFn: func(ctx context.Context, req *authorizationv1.WriteRequest, opts ...grpc.CallOption) (*authorizationv1.WriteResponse, error) {
			writeCalled = true
			if len(req.GetWrites()) != 1 {
				t.Fatalf("expected 1 write, got %d", len(req.GetWrites()))
			}
			tuple := req.GetWrites()[0]
			expectedUser := identityObjectPrefix + participantID.String()
			if tuple.GetUser() != expectedUser {
				t.Fatalf("expected user %s, got %s", expectedUser, tuple.GetUser())
			}
			if tuple.GetRelation() != "participant" {
				t.Fatalf("expected relation participant, got %s", tuple.GetRelation())
			}
			expectedObject := threadObjectPrefix + threadID.String()
			if tuple.GetObject() != expectedObject {
				t.Fatalf("expected object %s, got %s", expectedObject, tuple.GetObject())
			}
			return &authorizationv1.WriteResponse{}, nil
		},
	}

	srv := New(storeStub, nil, authStub, &stubIdentityResolver{t: t}, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.AddParticipant(ctx, &threadsv1.AddParticipantRequest{ThreadId: threadID.String(), ParticipantId: participantID.String()})
	if err != nil {
		t.Fatalf("AddParticipant returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected AddParticipant to be called")
	}
	if !writeCalled {
		t.Fatal("expected authorization write")
	}
}

func TestAddParticipantNicknameRequiresOrganizationID(t *testing.T) {
	threadID := uuid.New()
	identityID := uuid.New()
	authStub := allowAuthStub(t)

	srv := New(&stubThreadStore{t: t}, nil, authStub, &stubIdentityResolver{t: t}, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.AddParticipant(ctx, &threadsv1.AddParticipantRequest{
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
			return store.Thread{
				ID:        threadID,
				Status:    store.ThreadStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
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
			if req.GetNickname() != "agent-beta" {
				t.Fatalf("expected nickname agent-beta, got %s", req.GetNickname())
			}
			return &identityv1.ResolveNicknameResponse{IdentityId: participantID.String()}, nil
		},
	}

	identityID := uuid.New()
	authStub := allowAuthStub(t)
	srv := New(storeStub, nil, authStub, identityStub, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-organization-id", organizationID.String(), "x-identity-id", identityID.String()))
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
	now := time.Now().UTC()
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
			return store.Thread{
				ID:        threadID,
				Status:    store.ThreadStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
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

	authStub := allowAuthStub(t)
	srv := New(storeStub, nil, authStub, identityStub, agentsStub, nil)
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
			authStub := allowAuthStub(t)
			srv := New(&stubThreadStore{t: t}, nil, authStub, &stubIdentityResolver{t: t}, nil, nil)
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
	authStub := allowAuthStub(t)

	srv := New(&stubThreadStore{t: t}, nil, authStub, &stubIdentityResolver{t: t}, agentsStub, nil)
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
	authStub := allowAuthStub(t)

	srv := New(&stubThreadStore{t: t}, nil, authStub, &stubIdentityResolver{t: t}, agentsStub, nil)
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

func TestArchiveThreadAuthorizationDenied(t *testing.T) {
	threadID := uuid.New()
	organizationID := uuid.New()
	identityID := uuid.New()
	authCalls := []string{}

	storeStub := &stubThreadStore{
		t: t,
		getThreadFn: func(ctx context.Context, id uuid.UUID) (store.Thread, error) {
			if id != threadID {
				t.Fatalf("expected thread id %s, got %s", threadID, id)
			}
			return store.Thread{
				ID:             threadID,
				OrganizationID: &organizationID,
				Status:         store.ThreadStatusActive,
				CreatedAt:      time.Now().UTC(),
				UpdatedAt:      time.Now().UTC(),
			}, nil
		},
	}
	authStub := &stubAuthorizationService{
		t: t,
		checkFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
			if req.GetTupleKey() == nil {
				t.Fatal("expected tuple key")
			}
			if req.GetTupleKey().GetUser() != identityObjectPrefix+identityID.String() {
				t.Fatalf("expected user %s, got %s", identityObjectPrefix+identityID.String(), req.GetTupleKey().GetUser())
			}
			authCalls = append(authCalls, req.GetTupleKey().GetRelation()+":"+req.GetTupleKey().GetObject())
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(storeStub, nil, authStub, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.ArchiveThread(ctx, &threadsv1.ArchiveThreadRequest{ThreadId: threadID.String()})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s: %s", st.Code(), st.Message())
	}
	if len(authCalls) != 2 {
		t.Fatalf("expected 2 authorization checks, got %d", len(authCalls))
	}
	participantKey := "participant:" + threadObjectPrefix + threadID.String()
	ownerKey := "owner:" + organizationObjectPrefix + organizationID.String()
	seen := map[string]bool{}
	for _, call := range authCalls {
		seen[call] = true
	}
	if !seen[participantKey] {
		t.Fatalf("expected participant check %s", participantKey)
	}
	if !seen[ownerKey] {
		t.Fatalf("expected owner check %s", ownerKey)
	}
}

func TestDegradeThreadNoAuth(t *testing.T) {
	threadID := uuid.New()
	now := time.Now().UTC()
	storeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		degradeThreadFn: func(ctx context.Context, id uuid.UUID) (store.Thread, error) {
			storeCalled = true
			if id != threadID {
				t.Fatalf("expected thread id %s, got %s", threadID, id)
			}
			return store.Thread{
				ID:        threadID,
				Status:    store.ThreadStatusDegraded,
				CreatedAt: now,
				UpdatedAt: now,
			}, nil
		},
	}

	srv := New(storeStub, nil, nil, nil, nil, nil)
	resp, err := srv.DegradeThread(context.Background(), &threadsv1.DegradeThreadRequest{ThreadId: threadID.String(), Reason: "needs review"})
	if err != nil {
		t.Fatalf("DegradeThread returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected DegradeThread to be called")
	}
	if resp.GetThread().GetId() != threadID.String() {
		t.Fatalf("expected thread id %s, got %s", threadID, resp.GetThread().GetId())
	}
	if resp.GetThread().GetStatus() != threadsv1.ThreadStatus_THREAD_STATUS_DEGRADED {
		t.Fatalf("expected degraded status, got %s", resp.GetThread().GetStatus())
	}
}

func TestSendMessageAuthorizationDenied(t *testing.T) {
	threadID := uuid.New()
	identityID := uuid.New()
	senderID := identityID
	storeCalled := false
	authCalled := false

	storeStub := &stubThreadStore{
		t: t,
		sendMessageFn: func(ctx context.Context, threadArg, senderArg uuid.UUID, body string, fileIDs []uuid.UUID, messageRecipientIDs []uuid.UUID, agentInstanceRecipientIDs []uuid.UUID) (store.SendMessageResult, error) {
			storeCalled = true
			return store.SendMessageResult{}, nil
		},
	}
	authStub := &stubAuthorizationService{
		t: t,
		checkFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
			authCalled = true
			if req.GetTupleKey() == nil {
				t.Fatal("expected tuple key")
			}
			if req.GetTupleKey().GetRelation() != "can_write" {
				t.Fatalf("expected relation can_write, got %s", req.GetTupleKey().GetRelation())
			}
			expectedObject := threadObjectPrefix + threadID.String()
			if req.GetTupleKey().GetObject() != expectedObject {
				t.Fatalf("expected object %s, got %s", expectedObject, req.GetTupleKey().GetObject())
			}
			expectedUser := identityObjectPrefix + identityID.String()
			if req.GetTupleKey().GetUser() != expectedUser {
				t.Fatalf("expected user %s, got %s", expectedUser, req.GetTupleKey().GetUser())
			}
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(storeStub, nil, authStub, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.SendMessage(ctx, &threadsv1.SendMessageRequest{ThreadId: threadID.String(), SenderId: senderID.String(), Body: "hi"})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s: %s", st.Code(), st.Message())
	}
	if !authCalled {
		t.Fatal("expected authorization check")
	}
	if storeCalled {
		t.Fatal("expected SendMessage not to be called")
	}
}

func TestSendMessageRecordsUsageWithThreadOrganization(t *testing.T) {
	threadID := uuid.New()
	messageID := uuid.New()
	organizationID := uuid.New()
	identityID := uuid.New()
	now := time.Now().UTC()
	recorded := make(chan struct{}, 1)

	storeStub := &stubThreadStore{
		t: t,
		getThreadFn: func(ctx context.Context, id uuid.UUID) (store.Thread, error) {
			if id != threadID {
				t.Fatalf("expected thread %s, got %s", threadID, id)
			}
			return store.Thread{ID: threadID, OrganizationID: &organizationID, Participants: []store.Participant{{ID: identityID, JoinedAt: now, Passive: false}}}, nil
		},
		sendMessageFn: func(ctx context.Context, threadArg, senderArg uuid.UUID, body string, fileIDs []uuid.UUID, messageRecipientIDs []uuid.UUID, agentInstanceRecipientIDs []uuid.UUID) (store.SendMessageResult, error) {
			if threadArg != threadID {
				t.Fatalf("expected thread %s, got %s", threadID, threadArg)
			}
			if senderArg != identityID {
				t.Fatalf("expected sender %s, got %s", identityID, senderArg)
			}
			return store.SendMessageResult{
				Message: store.Message{
					ID:        messageID,
					ThreadID:  threadID,
					SenderID:  identityID,
					Body:      body,
					CreatedAt: now,
				},
				OrganizationID: organizationID,
			}, nil
		},
	}
	meteringStub := &stubMeteringRecorder{
		t: t,
		recordMessageSentFn: func(ctx context.Context, orgID, recordedThreadID, recordedMessageID uuid.UUID, createdAt time.Time) error {
			if orgID != organizationID {
				t.Fatalf("expected metering organization %s, got %s", organizationID, orgID)
			}
			if recordedThreadID != threadID {
				t.Fatalf("expected metering thread %s, got %s", threadID, recordedThreadID)
			}
			if recordedMessageID != messageID {
				t.Fatalf("expected metering message %s, got %s", messageID, recordedMessageID)
			}
			if !createdAt.Equal(now) {
				t.Fatalf("expected metering created_at %s, got %s", now, createdAt)
			}
			recorded <- struct{}{}
			return nil
		},
	}

	srv := New(storeStub, &stubNotifier{t: t}, allowAuthStub(t), nil, nil, meteringStub)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.SendMessage(ctx, &threadsv1.SendMessageRequest{ThreadId: threadID.String(), Body: "hi"})
	if err != nil {
		t.Fatalf("SendMessage returned error: %v", err)
	}
	select {
	case <-recorded:
	case <-time.After(time.Second):
		t.Fatal("expected message usage to be recorded")
	}
}

func TestSendMessageRejectsThreadWithoutOrganization(t *testing.T) {
	threadID := uuid.New()
	identityID := uuid.New()

	storeStub := &stubThreadStore{
		t: t,
		getThreadFn: func(ctx context.Context, id uuid.UUID) (store.Thread, error) {
			return store.Thread{ID: threadID, Participants: []store.Participant{{ID: identityID}}}, nil
		},
		sendMessageFn: func(ctx context.Context, threadArg, senderArg uuid.UUID, body string, fileIDs []uuid.UUID, messageRecipientIDs []uuid.UUID, agentInstanceRecipientIDs []uuid.UUID) (store.SendMessageResult, error) {
			return store.SendMessageResult{}, store.ErrThreadOrganizationMissing
		},
	}

	srv := New(storeStub, nil, allowAuthStub(t), nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.SendMessage(ctx, &threadsv1.SendMessageRequest{ThreadId: threadID.String(), Body: "hi"})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %s: %s", st.Code(), st.Message())
	}
	if st.Message() != store.ErrThreadOrganizationMissing.Error() {
		t.Fatalf("expected message %q, got %q", store.ErrThreadOrganizationMissing.Error(), st.Message())
	}
}

func TestSendMessageRejectsSenderMismatch(t *testing.T) {
	threadID := uuid.New()
	identityID := uuid.New()
	senderID := uuid.New()
	storeCalled := false
	authCalled := false

	storeStub := &stubThreadStore{
		t: t,
		sendMessageFn: func(ctx context.Context, threadArg, senderArg uuid.UUID, body string, fileIDs []uuid.UUID, messageRecipientIDs []uuid.UUID, agentInstanceRecipientIDs []uuid.UUID) (store.SendMessageResult, error) {
			storeCalled = true
			return store.SendMessageResult{}, nil
		},
	}
	authStub := &stubAuthorizationService{
		t: t,
		checkFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
			authCalled = true
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(storeStub, nil, authStub, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.SendMessage(ctx, &threadsv1.SendMessageRequest{ThreadId: threadID.String(), SenderId: senderID.String(), Body: "hi"})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s: %s", st.Code(), st.Message())
	}
	if authCalled {
		t.Fatal("expected authorization check not to be called")
	}
	if storeCalled {
		t.Fatal("expected SendMessage not to be called")
	}
}

func TestSendMessageRejectsDegradedThread(t *testing.T) {
	threadID := uuid.New()
	identityID := uuid.New()
	storeCalled := false
	findStatus := func(err error) *status.Status {
		st, _ := status.FromError(err)
		return st
	}

	storeStub := &stubThreadStore{
		t: t,
		getThreadFn: func(ctx context.Context, id uuid.UUID) (store.Thread, error) {
			return store.Thread{ID: threadID, Participants: []store.Participant{{ID: identityID}}}, nil
		},
		sendMessageFn: func(ctx context.Context, threadArg, senderArg uuid.UUID, body string, fileIDs []uuid.UUID, messageRecipientIDs []uuid.UUID, agentInstanceRecipientIDs []uuid.UUID) (store.SendMessageResult, error) {
			storeCalled = true
			if threadArg != threadID {
				t.Fatalf("expected thread id %s, got %s", threadID, threadArg)
			}
			return store.SendMessageResult{}, store.ErrThreadDegraded
		},
	}
	authStub := allowAuthStub(t)

	srv := New(storeStub, nil, authStub, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.SendMessage(ctx, &threadsv1.SendMessageRequest{ThreadId: threadID.String(), Body: "hi"})
	if err == nil {
		t.Fatal("expected error")
	}
	st := findStatus(err)
	if st == nil {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %s: %s", st.Code(), st.Message())
	}
	if st.Message() != "thread is degraded" {
		t.Fatalf("expected message 'thread is degraded', got %q", st.Message())
	}
	if !storeCalled {
		t.Fatal("expected SendMessage to be called")
	}
}

func TestGetThreadsMissingIdentity(t *testing.T) {
	participantID := uuid.New()

	srv := New(&stubThreadStore{t: t}, nil, nil, nil, nil, nil)
	_, err := srv.GetThreads(context.Background(), &threadsv1.GetThreadsRequest{ParticipantId: participantID.String()})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.Unauthenticated {
		t.Fatalf("expected Unauthenticated, got %s: %s", st.Code(), st.Message())
	}
}

func TestGetThreadsParticipantMismatch(t *testing.T) {
	participantID := uuid.New()
	identityID := uuid.New()

	srv := New(&stubThreadStore{t: t}, nil, nil, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.GetThreads(ctx, &threadsv1.GetThreadsRequest{ParticipantId: participantID.String()})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s: %s", st.Code(), st.Message())
	}
}

func TestGetOrganizationThreadsAuthorizationDenied(t *testing.T) {
	organizationID := uuid.New()
	identityID := uuid.New()
	authCalled := false

	authStub := &stubAuthorizationService{
		t: t,
		checkFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
			authCalled = true
			if req.GetTupleKey() == nil {
				t.Fatal("expected tuple key")
			}
			if req.GetTupleKey().GetRelation() != "can_view_threads" {
				t.Fatalf("expected relation can_view_threads, got %s", req.GetTupleKey().GetRelation())
			}
			expectedUser := identityObjectPrefix + identityID.String()
			if req.GetTupleKey().GetUser() != expectedUser {
				t.Fatalf("expected user %s, got %s", expectedUser, req.GetTupleKey().GetUser())
			}
			expectedObject := organizationObjectPrefix + organizationID.String()
			if req.GetTupleKey().GetObject() != expectedObject {
				t.Fatalf("expected object %s, got %s", expectedObject, req.GetTupleKey().GetObject())
			}
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(&stubThreadStore{t: t}, nil, authStub, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.GetOrganizationThreads(ctx, &threadsv1.GetOrganizationThreadsRequest{OrganizationId: organizationID.String()})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s: %s", st.Code(), st.Message())
	}
	if !authCalled {
		t.Fatal("expected authorization check")
	}
}

func TestGetOrganizationThreadsPagination(t *testing.T) {
	organizationID := uuid.New()
	identityID := uuid.New()
	threadID := uuid.New()
	participantID := uuid.New()
	createdAt := time.Now().UTC().Add(-2 * time.Hour)
	updatedAt := createdAt.Add(10 * time.Minute)
	pageCursor := store.OrganizationThreadCursor{CreatedAt: createdAt.Add(30 * time.Minute), ThreadID: uuid.New()}
	filter := store.OrganizationThreadFilter{}
	sortSpec := store.OrganizationThreadSort{Field: store.OrganizationThreadSortFieldCreated, Direction: store.SortDirectionDesc}
	pageToken, err := store.EncodeOrganizationThreadPageToken(organizationID, filter, sortSpec, pageCursor)
	if err != nil {
		t.Fatalf("encode page token: %v", err)
	}
	nextCursor := store.OrganizationThreadCursor{CreatedAt: createdAt, ThreadID: threadID}
	expectedNextToken, err := store.EncodeOrganizationThreadPageToken(organizationID, filter, sortSpec, nextCursor)
	if err != nil {
		t.Fatalf("encode next token: %v", err)
	}
	storeCalled := false
	authCalled := false

	storeStub := &stubThreadStore{
		t: t,
		listOrgThreadsFn: func(ctx context.Context, orgID uuid.UUID, listFilter store.OrganizationThreadFilter, listSort store.OrganizationThreadSort, pageSize int32, cursor *store.OrganizationThreadCursor) (store.OrganizationThreadListResult, error) {
			storeCalled = true
			if orgID != organizationID {
				t.Fatalf("expected organization %s, got %s", organizationID, orgID)
			}
			if !organizationThreadFiltersEqual(listFilter, filter) {
				t.Fatalf("unexpected filter: %+v", listFilter)
			}
			if listSort != sortSpec {
				t.Fatalf("unexpected sort: %+v", listSort)
			}
			if pageSize != 1 {
				t.Fatalf("expected page size 1, got %d", pageSize)
			}
			if cursor == nil {
				t.Fatal("expected cursor")
			}
			if cursor.ThreadID != pageCursor.ThreadID || !cursor.CreatedAt.Equal(pageCursor.CreatedAt) {
				t.Fatalf("unexpected cursor: %+v", cursor)
			}
			return store.OrganizationThreadListResult{
				Threads: []store.Thread{
					{
						ID:             threadID,
						OrganizationID: &organizationID,
						MessageCount:   3,
						Status:         store.ThreadStatusActive,
						CreatedAt:      createdAt,
						UpdatedAt:      updatedAt,
						Participants: []store.Participant{
							{ID: participantID, JoinedAt: createdAt, Passive: false},
						},
					},
				},
				NextCursor: &nextCursor,
			}, nil
		},
	}
	authStub := &stubAuthorizationService{
		t: t,
		checkFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
			authCalled = true
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(storeStub, nil, authStub, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	resp, err := srv.GetOrganizationThreads(ctx, &threadsv1.GetOrganizationThreadsRequest{
		OrganizationId: organizationID.String(),
		PageSize:       1,
		PageToken:      pageToken,
	})
	if err != nil {
		t.Fatalf("GetOrganizationThreads returned error: %v", err)
	}
	if !authCalled {
		t.Fatal("expected authorization check")
	}
	if !storeCalled {
		t.Fatal("expected GetOrganizationThreads to be called")
	}
	if len(resp.GetThreads()) != 1 {
		t.Fatalf("expected 1 thread, got %d", len(resp.GetThreads()))
	}
	if resp.GetThreads()[0].GetId() != threadID.String() {
		t.Fatalf("expected thread id %s, got %s", threadID, resp.GetThreads()[0].GetId())
	}
	if resp.GetNextPageToken() != expectedNextToken {
		t.Fatalf("expected next page token %s, got %s", expectedNextToken, resp.GetNextPageToken())
	}
}

func TestGetOrganizationThreadsDegradedStatusFilter(t *testing.T) {
	organizationID := uuid.New()
	identityID := uuid.New()
	storeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		listOrgThreadsFn: func(ctx context.Context, orgID uuid.UUID, filter store.OrganizationThreadFilter, sortSpec store.OrganizationThreadSort, pageSize int32, cursor *store.OrganizationThreadCursor) (store.OrganizationThreadListResult, error) {
			storeCalled = true
			if orgID != organizationID {
				t.Fatalf("expected organization %s, got %s", organizationID, orgID)
			}
			if len(filter.StatusIn) != 1 || filter.StatusIn[0] != store.ThreadStatusDegraded {
				t.Fatalf("expected degraded status filter, got %+v", filter.StatusIn)
			}
			expectedSort := store.OrganizationThreadSort{Field: store.OrganizationThreadSortFieldCreated, Direction: store.SortDirectionDesc}
			if sortSpec != expectedSort {
				t.Fatalf("unexpected sort: %+v", sortSpec)
			}
			return store.OrganizationThreadListResult{}, nil
		},
	}
	authStub := allowAuthStub(t)

	srv := New(storeStub, nil, authStub, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.GetOrganizationThreads(ctx, &threadsv1.GetOrganizationThreadsRequest{OrganizationId: organizationID.String(), Status: threadsv1.ThreadStatus_THREAD_STATUS_DEGRADED})
	if err != nil {
		t.Fatalf("GetOrganizationThreads returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected GetOrganizationThreads to be called")
	}
}

func TestListOrganizationThreadsAuthorizationDenied(t *testing.T) {
	organizationID := uuid.New()
	identityID := uuid.New()
	authCalled := false

	authStub := &stubAuthorizationService{
		t: t,
		checkFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
			authCalled = true
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(&stubThreadStore{t: t}, nil, authStub, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.ListOrganizationThreads(ctx, &threadsv1.ListOrganizationThreadsRequest{OrganizationId: organizationID.String()})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s: %s", st.Code(), st.Message())
	}
	if !authCalled {
		t.Fatal("expected authorization check")
	}
}

func TestListOrganizationThreadsFilterSortPagination(t *testing.T) {
	organizationID := uuid.New()
	identityID := uuid.New()
	identityType := "user"
	participantA := uuid.New()
	participantB := uuid.New()
	createdAfter := time.Now().UTC().Add(-24 * time.Hour)
	createdBefore := time.Now().UTC().Add(-2 * time.Hour)
	updatedAt := time.Now().UTC().Add(-1 * time.Hour)
	threadID := uuid.New()
	pageCursor := store.OrganizationThreadCursor{UpdatedAt: updatedAt.Add(-5 * time.Minute), ThreadID: uuid.New()}

	filter := &threadsv1.ListOrganizationThreadsFilter{
		StatusIn:        []threadsv1.ThreadStatus{threadsv1.ThreadStatus_THREAD_STATUS_DEGRADED, threadsv1.ThreadStatus_THREAD_STATUS_ACTIVE},
		ParticipantIdIn: []string{participantB.String(), " " + participantA.String() + " "},
		CreatedAfter:    timestamppb.New(createdAfter),
		CreatedBefore:   timestamppb.New(createdBefore),
	}
	sortSpec := &threadsv1.ListOrganizationThreadsSort{
		Field:     threadsv1.ListOrganizationThreadsSortField_LIST_ORGANIZATION_THREADS_SORT_FIELD_UPDATED,
		Direction: threadsv1.SortDirection_SORT_DIRECTION_ASC,
	}
	expectedStatuses := []store.ThreadStatus{store.ThreadStatusActive, store.ThreadStatusDegraded}
	sort.Slice(expectedStatuses, func(i, j int) bool { return expectedStatuses[i] < expectedStatuses[j] })
	expectedParticipants := []uuid.UUID{participantA, participantB}
	sort.Slice(expectedParticipants, func(i, j int) bool { return expectedParticipants[i].String() < expectedParticipants[j].String() })
	expectedFilter := store.OrganizationThreadFilter{
		StatusIn:       expectedStatuses,
		ParticipantIDs: expectedParticipants,
		CreatedAfter:   &createdAfter,
		CreatedBefore:  &createdBefore,
	}
	expectedSort := store.OrganizationThreadSort{Field: store.OrganizationThreadSortFieldUpdated, Direction: store.SortDirectionAsc}
	pageToken, err := store.EncodeOrganizationThreadPageToken(organizationID, expectedFilter, expectedSort, pageCursor)
	if err != nil {
		t.Fatalf("encode page token: %v", err)
	}
	nextCursor := store.OrganizationThreadCursor{UpdatedAt: updatedAt, ThreadID: threadID}
	expectedNextToken, err := store.EncodeOrganizationThreadPageToken(organizationID, expectedFilter, expectedSort, nextCursor)
	if err != nil {
		t.Fatalf("encode next page token: %v", err)
	}

	storeCalled := false
	identityCalled := false

	storeStub := &stubThreadStore{
		t: t,
		listOrgThreadsFn: func(ctx context.Context, orgID uuid.UUID, listFilter store.OrganizationThreadFilter, listSort store.OrganizationThreadSort, pageSize int32, cursor *store.OrganizationThreadCursor) (store.OrganizationThreadListResult, error) {
			storeCalled = true
			if orgID != organizationID {
				t.Fatalf("expected organization %s, got %s", organizationID, orgID)
			}
			if !organizationThreadFiltersEqual(listFilter, expectedFilter) {
				t.Fatalf("unexpected filter: %+v", listFilter)
			}
			if listSort != expectedSort {
				t.Fatalf("unexpected sort: %+v", listSort)
			}
			if pageSize != 2 {
				t.Fatalf("expected page size 2, got %d", pageSize)
			}
			if cursor == nil {
				t.Fatal("expected cursor")
			}
			if cursor.ThreadID != pageCursor.ThreadID || !cursor.UpdatedAt.Equal(pageCursor.UpdatedAt) {
				t.Fatalf("unexpected cursor: %+v", cursor)
			}
			return store.OrganizationThreadListResult{
				Threads: []store.Thread{
					{
						ID:             threadID,
						OrganizationID: &organizationID,
						MessageCount:   1,
						Status:         store.ThreadStatusActive,
						CreatedAt:      createdAfter.Add(2 * time.Hour),
						UpdatedAt:      updatedAt,
						Participants: []store.Participant{
							{ID: participantA, JoinedAt: createdAfter, Passive: false},
							{ID: participantB, JoinedAt: createdAfter, Passive: true},
						},
					},
				},
				NextCursor: &nextCursor,
			}, nil
		},
	}
	identityStub := &stubIdentityResolver{
		t: t,
		batchFn: func(ctx context.Context, req *identityv1.BatchGetNicknamesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetNicknamesResponse, error) {
			identityCalled = true
			md, ok := metadata.FromOutgoingContext(ctx)
			if !ok {
				t.Fatal("expected outgoing metadata")
			}
			if value := md.Get(identityIDMetadataKey); len(value) != 1 || value[0] != identityID.String() {
				t.Fatalf("expected %s %s, got %v", identityIDMetadataKey, identityID, value)
			}
			if value := md.Get(identityTypeMetadataKey); len(value) != 1 || value[0] != identityType {
				t.Fatalf("expected %s %s, got %v", identityTypeMetadataKey, identityType, value)
			}
			if value := md.Get(organizationIDMetadataKey); len(value) != 1 || value[0] != organizationID.String() {
				t.Fatalf("expected %s %s, got %v", organizationIDMetadataKey, organizationID, value)
			}
			if req.GetOrganizationId() != organizationID.String() {
				t.Fatalf("expected organization id %s, got %s", organizationID, req.GetOrganizationId())
			}
			expectedIDs := []string{participantA.String(), participantB.String()}
			sort.Strings(expectedIDs)
			if !reflect.DeepEqual(req.GetIdentityIds(), expectedIDs) {
				t.Fatalf("expected identity ids %v, got %v", expectedIDs, req.GetIdentityIds())
			}
			return &identityv1.BatchGetNicknamesResponse{}, nil
		},
	}
	authStub := allowAuthStub(t)

	srv := New(storeStub, nil, authStub, identityStub, nil, nil)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("x-identity-id", identityID.String(), "x-identity-type", identityType, "x-organization-id", organizationID.String()),
	)
	resp, err := srv.ListOrganizationThreads(ctx, &threadsv1.ListOrganizationThreadsRequest{
		OrganizationId: organizationID.String(),
		Filter:         filter,
		Sort:           sortSpec,
		PageSize:       2,
		PageToken:      pageToken,
	})
	if err != nil {
		t.Fatalf("ListOrganizationThreads returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected ListOrganizationThreads to be called")
	}
	if !identityCalled {
		t.Fatal("expected BatchGetNicknames to be called")
	}
	if resp.GetNextPageToken() != expectedNextToken {
		t.Fatalf("expected next page token %s, got %s", expectedNextToken, resp.GetNextPageToken())
	}
}

func TestListOrganizationThreadsNicknameEnrichment(t *testing.T) {
	organizationID := uuid.New()
	identityID := uuid.New()
	threadID := uuid.New()
	participantA := uuid.New()
	participantB := uuid.New()
	createdAt := time.Now().UTC().Add(-2 * time.Hour)

	storeStub := &stubThreadStore{
		t: t,
		listOrgThreadsFn: func(ctx context.Context, orgID uuid.UUID, filter store.OrganizationThreadFilter, sortSpec store.OrganizationThreadSort, pageSize int32, cursor *store.OrganizationThreadCursor) (store.OrganizationThreadListResult, error) {
			return store.OrganizationThreadListResult{
				Threads: []store.Thread{
					{
						ID:             threadID,
						OrganizationID: &organizationID,
						MessageCount:   2,
						Status:         store.ThreadStatusActive,
						CreatedAt:      createdAt,
						UpdatedAt:      createdAt,
						Participants: []store.Participant{
							{ID: participantA, JoinedAt: createdAt, Passive: false},
							{ID: participantB, JoinedAt: createdAt, Passive: false},
						},
					},
				},
			}, nil
		},
	}
	identityStub := &stubIdentityResolver{
		t: t,
		batchFn: func(ctx context.Context, req *identityv1.BatchGetNicknamesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetNicknamesResponse, error) {
			return &identityv1.BatchGetNicknamesResponse{
				Entries: []*identityv1.NicknameEntry{
					{IdentityId: participantA.String(), Nickname: "alpha"},
				},
			}, nil
		},
	}
	authStub := allowAuthStub(t)

	srv := New(storeStub, nil, authStub, identityStub, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	resp, err := srv.ListOrganizationThreads(ctx, &threadsv1.ListOrganizationThreadsRequest{OrganizationId: organizationID.String()})
	if err != nil {
		t.Fatalf("ListOrganizationThreads returned error: %v", err)
	}
	if len(resp.GetThreads()) != 1 {
		t.Fatalf("expected 1 thread, got %d", len(resp.GetThreads()))
	}
	thread := resp.GetThreads()[0]
	participantOne := findProtoParticipant(thread, participantA)
	if participantOne == nil || participantOne.GetNickname() != "alpha" {
		t.Fatalf("expected nickname alpha for participantA, got %+v", participantOne)
	}
	participantTwo := findProtoParticipant(thread, participantB)
	if participantTwo == nil {
		t.Fatal("expected participantB")
	}
	if participantTwo.GetNickname() != "" {
		t.Fatalf("expected empty nickname for participantB, got %s", participantTwo.GetNickname())
	}
}

func TestGetThreadAuthorizationDenied(t *testing.T) {
	organizationID := uuid.New()
	threadID := uuid.New()
	identityID := uuid.New()
	authCalled := false

	storeStub := &stubThreadStore{
		t: t,
		getThreadFn: func(ctx context.Context, id uuid.UUID) (store.Thread, error) {
			if id != threadID {
				t.Fatalf("expected thread id %s, got %s", threadID, id)
			}
			return store.Thread{
				ID:             threadID,
				OrganizationID: &organizationID,
				Status:         store.ThreadStatusActive,
				CreatedAt:      time.Now().UTC(),
				UpdatedAt:      time.Now().UTC(),
			}, nil
		},
	}
	authStub := &stubAuthorizationService{
		t: t,
		checkFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
			authCalled = true
			if req.GetTupleKey() == nil {
				t.Fatal("expected tuple key")
			}
			if req.GetTupleKey().GetRelation() != "can_read" {
				t.Fatalf("expected relation can_read, got %s", req.GetTupleKey().GetRelation())
			}
			expectedObject := threadObjectPrefix + threadID.String()
			if req.GetTupleKey().GetObject() != expectedObject {
				t.Fatalf("expected object %s, got %s", expectedObject, req.GetTupleKey().GetObject())
			}
			expectedUser := identityObjectPrefix + identityID.String()
			if req.GetTupleKey().GetUser() != expectedUser {
				t.Fatalf("expected user %s, got %s", expectedUser, req.GetTupleKey().GetUser())
			}
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(storeStub, nil, authStub, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.GetThread(ctx, &threadsv1.GetThreadRequest{ThreadId: threadID.String()})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s: %s", st.Code(), st.Message())
	}
	if !authCalled {
		t.Fatal("expected authorization check")
	}
}

func TestGetThreadMissingOrganizationID(t *testing.T) {
	threadID := uuid.New()
	identityID := uuid.New()
	storeStub := &stubThreadStore{
		t: t,
		getThreadFn: func(ctx context.Context, id uuid.UUID) (store.Thread, error) {
			if id != threadID {
				t.Fatalf("expected thread id %s, got %s", threadID, id)
			}
			return store.Thread{
				ID:        threadID,
				Status:    store.ThreadStatusActive,
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}, nil
		},
	}

	srv := New(storeStub, nil, nil, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.GetThread(ctx, &threadsv1.GetThreadRequest{ThreadId: threadID.String()})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Fatalf("expected NotFound, got %s: %s", st.Code(), st.Message())
	}
}

func TestGetUnackedMessagesPermissionDenied(t *testing.T) {
	participantID := uuid.New()
	identityID := uuid.New()

	srv := New(&stubThreadStore{t: t}, nil, nil, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.GetUnackedMessages(ctx, &threadsv1.GetUnackedMessagesRequest{ParticipantId: participantID.String()})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s: %s", st.Code(), st.Message())
	}
}

func TestGetUnackedMessageCountsPermissionDenied(t *testing.T) {
	participantID := uuid.New()
	identityID := uuid.New()

	srv := New(&stubThreadStore{t: t}, nil, nil, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.GetUnackedMessageCounts(ctx, &threadsv1.GetUnackedMessageCountsRequest{ParticipantId: participantID.String()})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s: %s", st.Code(), st.Message())
	}
}

func TestGetUnackedMessageCounts(t *testing.T) {
	participantID := uuid.New()
	threadID := uuid.New()
	zeroThreadID := uuid.New()
	storeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		unackedCountsFn: func(ctx context.Context, id uuid.UUID) (map[uuid.UUID]int32, error) {
			storeCalled = true
			if id != participantID {
				t.Fatalf("expected participant id %s, got %s", participantID, id)
			}
			return map[uuid.UUID]int32{threadID: 3, zeroThreadID: 0}, nil
		},
	}

	srv := New(storeStub, nil, nil, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", participantID.String()))
	resp, err := srv.GetUnackedMessageCounts(ctx, &threadsv1.GetUnackedMessageCountsRequest{ParticipantId: participantID.String()})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !storeCalled {
		t.Fatal("expected store call")
	}
	expected := map[string]int32{threadID.String(): 3}
	if !reflect.DeepEqual(resp.GetCountsByThreadId(), expected) {
		t.Fatalf("expected counts %v, got %v", expected, resp.GetCountsByThreadId())
	}
}

func TestAckMessagesPermissionDenied(t *testing.T) {
	participantID := uuid.New()
	identityID := uuid.New()
	messageID := uuid.New()

	srv := New(&stubThreadStore{t: t}, nil, nil, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.AckMessages(ctx, &threadsv1.AckMessagesRequest{
		ParticipantId: participantID.String(),
		MessageIds:    []string{messageID.String()},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s: %s", st.Code(), st.Message())
	}
}

func TestGetMessagesAuthorizationDenied(t *testing.T) {
	threadID := uuid.New()
	identityID := uuid.New()
	storeCalled := false
	authCalled := false

	storeStub := &stubThreadStore{
		t: t,
		listMessagesFn: func(ctx context.Context, id uuid.UUID, pageSize int32, cursor *store.MessageCursor, order store.MessageOrder) (store.MessageListResult, error) {
			storeCalled = true
			return store.MessageListResult{}, nil
		},
	}
	authStub := &stubAuthorizationService{
		t: t,
		checkFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
			authCalled = true
			if req.GetTupleKey() == nil {
				t.Fatal("expected tuple key")
			}
			if req.GetTupleKey().GetRelation() != "can_read" {
				t.Fatalf("expected relation can_read, got %s", req.GetTupleKey().GetRelation())
			}
			expectedObject := threadObjectPrefix + threadID.String()
			if req.GetTupleKey().GetObject() != expectedObject {
				t.Fatalf("expected object %s, got %s", expectedObject, req.GetTupleKey().GetObject())
			}
			expectedUser := identityObjectPrefix + identityID.String()
			if req.GetTupleKey().GetUser() != expectedUser {
				t.Fatalf("expected user %s, got %s", expectedUser, req.GetTupleKey().GetUser())
			}
			return &authorizationv1.CheckResponse{Allowed: false}, nil
		},
	}

	srv := New(storeStub, nil, authStub, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	_, err := srv.GetMessages(ctx, &threadsv1.GetMessagesRequest{ThreadId: threadID.String()})
	if err == nil {
		t.Fatal("expected error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s: %s", st.Code(), st.Message())
	}
	if !authCalled {
		t.Fatal("expected authorization check")
	}
	if storeCalled {
		t.Fatal("expected ListMessages not to be called")
	}
}

func TestGetMessagesNewestFirst(t *testing.T) {
	threadID := uuid.New()
	messageID := uuid.New()
	senderID := uuid.New()
	createdAt := time.Now().UTC()
	storeCalled := false

	storeStub := &stubThreadStore{
		t: t,
		listMessagesFn: func(ctx context.Context, id uuid.UUID, pageSize int32, cursor *store.MessageCursor, order store.MessageOrder) (store.MessageListResult, error) {
			storeCalled = true
			if id != threadID {
				t.Fatalf("expected thread id %s, got %s", threadID, id)
			}
			if pageSize != 2 {
				t.Fatalf("expected page size 2, got %d", pageSize)
			}
			if cursor != nil {
				t.Fatal("expected nil cursor")
			}
			if order != store.MessageOrderNewestFirst {
				t.Fatalf("expected newest-first order, got %v", order)
			}
			return store.MessageListResult{
				Messages: []store.Message{
					{
						ID:        messageID,
						ThreadID:  threadID,
						SenderID:  senderID,
						Body:      "hello",
						CreatedAt: createdAt,
					},
				},
			}, nil
		},
	}

	identityID := uuid.New()
	authStub := allowAuthStub(t)
	srv := New(storeStub, nil, authStub, nil, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String()))
	resp, err := srv.GetMessages(ctx, &threadsv1.GetMessagesRequest{
		ThreadId: threadID.String(),
		PageSize: 2,
		Order:    threadsv1.MessageOrder_MESSAGE_ORDER_NEWEST_FIRST,
	})
	if err != nil {
		t.Fatalf("GetMessages returned error: %v", err)
	}
	if !storeCalled {
		t.Fatal("expected ListMessages to be called")
	}
	if len(resp.GetMessages()) != 1 {
		t.Fatalf("expected 1 message, got %d", len(resp.GetMessages()))
	}
	if resp.GetMessages()[0].GetId() != messageID.String() {
		t.Fatalf("expected message id %s, got %s", messageID, resp.GetMessages()[0].GetId())
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

func organizationThreadFiltersEqual(left, right store.OrganizationThreadFilter) bool {
	if len(left.StatusIn) != len(right.StatusIn) {
		return false
	}
	for i := range left.StatusIn {
		if left.StatusIn[i] != right.StatusIn[i] {
			return false
		}
	}
	if len(left.ParticipantIDs) != len(right.ParticipantIDs) {
		return false
	}
	for i := range left.ParticipantIDs {
		if left.ParticipantIDs[i] != right.ParticipantIDs[i] {
			return false
		}
	}
	if (left.CreatedAfter == nil) != (right.CreatedAfter == nil) {
		return false
	}
	if left.CreatedAfter != nil && !left.CreatedAfter.Equal(*right.CreatedAfter) {
		return false
	}
	if (left.CreatedBefore == nil) != (right.CreatedBefore == nil) {
		return false
	}
	if left.CreatedBefore != nil && !left.CreatedBefore.Equal(*right.CreatedBefore) {
		return false
	}
	return true
}

func TestCreateThreadStoresAgentInstanceForAgentClassParticipant(t *testing.T) {
	threadID := uuid.New()
	organizationID := uuid.New()
	identityID := uuid.New()
	agentID := uuid.New()
	instanceID := uuid.New()
	now := time.Now().UTC()

	storeStub := &stubThreadStore{
		t: t,
		createThreadFn: func(ctx context.Context, orgID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error) {
			if orgID != organizationID {
				t.Fatalf("expected organization %s, got %s", organizationID, orgID)
			}
			if len(participants) != 2 {
				t.Fatalf("expected 2 participants, got %d", len(participants))
			}
			if participants[1].ID != instanceID {
				t.Fatalf("expected stored agent instance %s, got %s", instanceID, participants[1].ID)
			}
			return store.Thread{
				ID:             threadID,
				OrganizationID: &organizationID,
				Status:         store.ThreadStatusActive,
				CreatedAt:      now,
				UpdatedAt:      now,
				Participants: []store.Participant{
					{ID: identityID, JoinedAt: now},
					{ID: instanceID, JoinedAt: now},
				},
			}, nil
		},
	}
	identityStub := &stubIdentityResolver{
		t: t,
		typeBatchFn: func(ctx context.Context, req *identityv1.BatchGetIdentityTypesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetIdentityTypesResponse, error) {
			entries := make([]*identityv1.IdentityTypeEntry, len(req.GetIdentityIds()))
			for i, id := range req.GetIdentityIds() {
				identityType := identityv1.IdentityType_IDENTITY_TYPE_USER
				if id == agentID.String() {
					identityType = identityv1.IdentityType_IDENTITY_TYPE_AGENT
				}
				entries[i] = &identityv1.IdentityTypeEntry{IdentityId: id, IdentityType: identityType}
			}
			return &identityv1.BatchGetIdentityTypesResponse{Entries: entries}, nil
		},
	}
	agentsStub := &stubAgentsService{
		t: t,
		createFn: func(ctx context.Context, req *agentsv1.CreateInstanceRequest, opts ...grpc.CallOption) (*agentsv1.CreateInstanceResponse, error) {
			if req.GetAgentId() != agentID.String() {
				t.Fatalf("expected agent id %s, got %s", agentID, req.GetAgentId())
			}
			return &agentsv1.CreateInstanceResponse{Instance: &agentsv1.AgentInstance{Meta: &agentsv1.EntityMeta{Id: instanceID.String()}, AgentId: agentID.String()}}, nil
		},
	}
	authStub := &stubAuthorizationService{
		t: t,
		checkFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
			if req.GetTupleKey().GetRelation() == "can_initiate" && req.GetTupleKey().GetObject() != "agent:"+agentID.String() {
				t.Fatalf("expected can_initiate on agent %s, got %s", agentID, req.GetTupleKey().GetObject())
			}
			return &authorizationv1.CheckResponse{Allowed: true}, nil
		},
	}

	srv := New(storeStub, nil, authStub, identityStub, agentsStub, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", identityID.String(), "x-identity-type", "user", "x-organization-id", organizationID.String()))
	_, err := srv.CreateThread(ctx, &threadsv1.CreateThreadRequest{Participants: []*threadsv1.ParticipantIdentifier{{Identifier: &threadsv1.ParticipantIdentifier_ParticipantId{ParticipantId: agentID.String()}}}})
	if err != nil {
		t.Fatalf("CreateThread returned error: %v", err)
	}
}

func TestSendMessageFansOutAgentInstancesOnly(t *testing.T) {
	threadID := uuid.New()
	messageID := uuid.New()
	organizationID := uuid.New()
	senderID := uuid.New()
	userRecipientID := uuid.New()
	agentInstanceID := uuid.New()
	claimID := uuid.New()
	now := time.Now().UTC()
	var fanoutCalled bool
	var deliveredMarked bool

	storeStub := &stubThreadStore{
		t: t,
		claimPendingAgentInboxDeliveriesFn: func(ctx context.Context, limit int32) ([]store.AgentInboxDelivery, error) {
			if limit != agentInboxDeliveryLimit {
				t.Fatalf("expected limit %d, got %d", agentInboxDeliveryLimit, limit)
			}
			return []store.AgentInboxDelivery{{MessageID: messageID, ThreadID: threadID, SenderID: senderID, AgentInstanceID: agentInstanceID, Body: "claimed", FileIDs: []uuid.UUID{}, ClaimID: claimID}}, nil
		},
		markAgentInboxDeliveryDeliveredFn: func(ctx context.Context, messageArg, agentInstanceArg, claimArg uuid.UUID) error {
			deliveredMarked = true
			if messageArg != messageID || agentInstanceArg != agentInstanceID || claimArg != claimID {
				t.Fatalf("unexpected delivered mark: %s %s %s", messageArg, agentInstanceArg, claimArg)
			}
			return nil
		},
		getThreadFn: func(ctx context.Context, id uuid.UUID) (store.Thread, error) {
			return store.Thread{ID: threadID, OrganizationID: &organizationID, Participants: []store.Participant{{ID: senderID}, {ID: userRecipientID}, {ID: agentInstanceID}}}, nil
		},
		sendMessageFn: func(ctx context.Context, threadArg, senderArg uuid.UUID, body string, fileIDs []uuid.UUID, messageRecipientIDs []uuid.UUID, agentInstanceRecipientIDs []uuid.UUID) (store.SendMessageResult, error) {
			if !reflect.DeepEqual(messageRecipientIDs, []uuid.UUID{userRecipientID}) {
				t.Fatalf("expected user/app message recipients only, got %v", messageRecipientIDs)
			}
			if !reflect.DeepEqual(agentInstanceRecipientIDs, []uuid.UUID{agentInstanceID}) {
				t.Fatalf("expected agent instance recipients, got %v", agentInstanceRecipientIDs)
			}
			return store.SendMessageResult{Message: store.Message{ID: messageID, ThreadID: threadID, SenderID: senderID, Body: body, FileIDs: fileIDs, CreatedAt: now}, OrganizationID: organizationID, Recipients: messageRecipientIDs, AgentInboxDeliveries: []store.AgentInboxDelivery{{MessageID: messageID, ThreadID: threadID, SenderID: senderID, AgentInstanceID: agentInstanceID, Body: "unclaimed", FileIDs: fileIDs}}}, nil
		},
	}
	identityStub := &stubIdentityResolver{
		t: t,
		typeBatchFn: func(ctx context.Context, req *identityv1.BatchGetIdentityTypesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetIdentityTypesResponse, error) {
			entries := make([]*identityv1.IdentityTypeEntry, len(req.GetIdentityIds()))
			for i, id := range req.GetIdentityIds() {
				identityType := identityv1.IdentityType_IDENTITY_TYPE_USER
				if id == agentInstanceID.String() {
					identityType = identityv1.IdentityType_IDENTITY_TYPE_AGENT_INSTANCE
				}
				entries[i] = &identityv1.IdentityTypeEntry{IdentityId: id, IdentityType: identityType}
			}
			return &identityv1.BatchGetIdentityTypesResponse{Entries: entries}, nil
		},
	}
	agentsStub := &stubAgentsService{
		t: t,
		fanoutFn: func(ctx context.Context, req *agentsv1.FanoutInboxItemRequest, opts ...grpc.CallOption) (*agentsv1.FanoutInboxItemResponse, error) {
			fanoutCalled = true
			if req.GetAgentInstanceId() != agentInstanceID.String() || req.GetThreadId() != threadID.String() || req.GetMessageId() != messageID.String() || req.GetSenderId() != senderID.String() {
				t.Fatalf("unexpected fanout request: %+v", req)
			}
			if req.GetBody() != "claimed" {
				t.Fatalf("expected claimed outbox row body, got %q", req.GetBody())
			}
			return &agentsv1.FanoutInboxItemResponse{}, nil
		},
	}
	notifierStub := &stubNotifier{t: t, publishFn: func(ctx context.Context, threadArg, messageArg uuid.UUID, recipients []uuid.UUID) error {
		if !reflect.DeepEqual(recipients, []uuid.UUID{userRecipientID}) {
			t.Fatalf("expected user notification recipients only, got %v", recipients)
		}
		return nil
	}}

	srv := New(storeStub, notifierStub, allowAuthStub(t), identityStub, agentsStub, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", senderID.String()))
	_, err := srv.SendMessage(ctx, &threadsv1.SendMessageRequest{ThreadId: threadID.String(), Body: "hi"})
	if err != nil {
		t.Fatalf("SendMessage returned error: %v", err)
	}
	if !fanoutCalled {
		t.Fatal("expected agent instance fanout")
	}
	if !deliveredMarked {
		t.Fatal("expected claimed delivery to be marked delivered")
	}
}

func TestIdentityTypesRequiresCompleteResponse(t *testing.T) {
	firstID := uuid.New()
	secondID := uuid.New()
	callerID := uuid.New()
	identityStub := &stubIdentityResolver{
		t: t,
		typeBatchFn: func(ctx context.Context, req *identityv1.BatchGetIdentityTypesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetIdentityTypesResponse, error) {
			return &identityv1.BatchGetIdentityTypesResponse{Entries: []*identityv1.IdentityTypeEntry{{IdentityId: firstID.String(), IdentityType: identityv1.IdentityType_IDENTITY_TYPE_USER}}}, nil
		},
	}

	srv := New(&stubThreadStore{t: t}, nil, nil, identityStub, nil, nil)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-identity-id", callerID.String()))
	_, err := srv.identityTypes(ctx, []uuid.UUID{firstID, secondID})
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

func TestDrainPendingAgentInboxDeliveriesMarksDelivered(t *testing.T) {
	messageID := uuid.New()
	threadID := uuid.New()
	senderID := uuid.New()
	agentInstanceID := uuid.New()
	claimID := uuid.New()
	var marked bool
	storeStub := &stubThreadStore{
		t: t,
		claimPendingAgentInboxDeliveriesFn: func(ctx context.Context, limit int32) ([]store.AgentInboxDelivery, error) {
			if limit != agentInboxDeliveryLimit {
				t.Fatalf("expected limit %d, got %d", agentInboxDeliveryLimit, limit)
			}
			return []store.AgentInboxDelivery{{MessageID: messageID, ThreadID: threadID, SenderID: senderID, AgentInstanceID: agentInstanceID, ClaimID: claimID, Body: "hi"}}, nil
		},
		markAgentInboxDeliveryDeliveredFn: func(ctx context.Context, messageArg, agentInstanceArg, claimArg uuid.UUID) error {
			marked = true
			if messageArg != messageID || agentInstanceArg != agentInstanceID || claimArg != claimID {
				t.Fatalf("unexpected delivered mark: %s %s %s", messageArg, agentInstanceArg, claimArg)
			}
			return nil
		},
	}
	agentsStub := &stubAgentsService{
		t: t,
		fanoutFn: func(ctx context.Context, req *agentsv1.FanoutInboxItemRequest, opts ...grpc.CallOption) (*agentsv1.FanoutInboxItemResponse, error) {
			if req.GetMessageId() != messageID.String() || req.GetAgentInstanceId() != agentInstanceID.String() {
				t.Fatalf("unexpected fanout request: %+v", req)
			}
			return &agentsv1.FanoutInboxItemResponse{}, nil
		},
	}

	srv := New(storeStub, nil, nil, nil, agentsStub, nil)
	if err := srv.DrainPendingAgentInboxDeliveries(context.Background()); err != nil {
		t.Fatalf("DrainPendingAgentInboxDeliveries returned error: %v", err)
	}
	if !marked {
		t.Fatal("expected delivered mark")
	}
}

func TestDrainPendingAgentInboxDeliveriesContinuesAfterFailure(t *testing.T) {
	failedMessageID := uuid.New()
	succeededMessageID := uuid.New()
	threadID := uuid.New()
	senderID := uuid.New()
	failedAgentInstanceID := uuid.New()
	succeededAgentInstanceID := uuid.New()
	failedClaimID := uuid.New()
	succeededClaimID := uuid.New()
	fanoutCalls := make([]uuid.UUID, 0, 2)
	failedMarked := false
	deliveredMarked := false
	storeStub := &stubThreadStore{
		t: t,
		claimPendingAgentInboxDeliveriesFn: func(ctx context.Context, limit int32) ([]store.AgentInboxDelivery, error) {
			return []store.AgentInboxDelivery{
				{MessageID: failedMessageID, ThreadID: threadID, SenderID: senderID, AgentInstanceID: failedAgentInstanceID, ClaimID: failedClaimID, Body: "first"},
				{MessageID: succeededMessageID, ThreadID: threadID, SenderID: senderID, AgentInstanceID: succeededAgentInstanceID, ClaimID: succeededClaimID, Body: "second"},
			}, nil
		},
		markAgentInboxDeliveryFailedFn: func(ctx context.Context, messageArg, agentInstanceArg, claimArg uuid.UUID, deliveryError string) error {
			failedMarked = true
			if messageArg != failedMessageID || agentInstanceArg != failedAgentInstanceID || claimArg != failedClaimID {
				t.Fatalf("unexpected failed mark: %s %s %s", messageArg, agentInstanceArg, claimArg)
			}
			if deliveryError == "" {
				t.Fatal("expected delivery error")
			}
			return nil
		},
		markAgentInboxDeliveryDeliveredFn: func(ctx context.Context, messageArg, agentInstanceArg, claimArg uuid.UUID) error {
			deliveredMarked = true
			if messageArg != succeededMessageID || agentInstanceArg != succeededAgentInstanceID || claimArg != succeededClaimID {
				t.Fatalf("unexpected delivered mark: %s %s %s", messageArg, agentInstanceArg, claimArg)
			}
			return nil
		},
	}
	agentsStub := &stubAgentsService{
		t: t,
		fanoutFn: func(ctx context.Context, req *agentsv1.FanoutInboxItemRequest, opts ...grpc.CallOption) (*agentsv1.FanoutInboxItemResponse, error) {
			messageID, err := uuid.Parse(req.GetMessageId())
			if err != nil {
				t.Fatalf("parse message id: %v", err)
			}
			fanoutCalls = append(fanoutCalls, messageID)
			if messageID == failedMessageID {
				return nil, status.Error(codes.Unavailable, "agents unavailable")
			}
			return &agentsv1.FanoutInboxItemResponse{}, nil
		},
	}

	srv := New(storeStub, nil, nil, nil, agentsStub, nil)
	err := srv.DrainPendingAgentInboxDeliveries(context.Background())
	if err == nil {
		t.Fatal("expected drain error")
	}
	if len(fanoutCalls) != 2 {
		t.Fatalf("expected 2 fanout calls, got %d", len(fanoutCalls))
	}
	if fanoutCalls[0] != failedMessageID || fanoutCalls[1] != succeededMessageID {
		t.Fatalf("unexpected fanout order: %v", fanoutCalls)
	}
	if !failedMarked {
		t.Fatal("expected failed mark")
	}
	if !deliveredMarked {
		t.Fatal("expected delivered mark after failure")
	}
}
