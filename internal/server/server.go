package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
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

	"github.com/agynio/threads/internal/store"
)

type Server struct {
	threadsv1.UnimplementedThreadsServiceServer
	store         threadStore
	notifier      notifierPublisher
	authorization authorizationChecker
	identity      identityResolver
	agents        agentsService
	metering      meteringRecorder
}

const (
	organizationIDMetadataKey = "x-organization-id"
	identityIDMetadataKey     = "x-identity-id"
	identityTypeMetadataKey   = "x-identity-type"
	agentIdentityType         = "agent"
	meteringTimeout           = 5 * time.Second
	identityObjectPrefix      = "identity:"
	organizationObjectPrefix  = "organization:"
	threadObjectPrefix        = "thread:"
)

type threadStore interface {
	CreateThread(ctx context.Context, organizationID uuid.UUID, participants []store.ParticipantInput) (store.Thread, error)
	ArchiveThread(ctx context.Context, threadID uuid.UUID) (store.Thread, error)
	DegradeThread(ctx context.Context, threadID uuid.UUID) (store.Thread, error)
	AddParticipant(ctx context.Context, threadID, participantID uuid.UUID, passive bool) (store.Thread, error)
	SendMessage(ctx context.Context, threadID, senderID uuid.UUID, body string, fileIDs []uuid.UUID) (store.SendMessageResult, error)
	GetThread(ctx context.Context, threadID uuid.UUID) (store.Thread, error)
	ListThreads(ctx context.Context, participantID uuid.UUID, pageSize int32, cursor *store.ThreadCursor) (store.ThreadListResult, error)
	ListOrganizationThreads(ctx context.Context, organizationID uuid.UUID, filter store.OrganizationThreadFilter, sort store.OrganizationThreadSort, pageSize int32, cursor *store.OrganizationThreadCursor) (store.OrganizationThreadListResult, error)
	ListMessages(ctx context.Context, threadID uuid.UUID, pageSize int32, cursor *store.MessageCursor, order store.MessageOrder) (store.MessageListResult, error)
	ListUnackedMessages(ctx context.Context, participantID uuid.UUID, threadID *uuid.UUID, pageSize int32, cursor *store.MessageCursor) (store.MessageListResult, error)
	GetUnackedMessageCounts(ctx context.Context, participantID uuid.UUID) (map[uuid.UUID]int32, error)
	AckMessages(ctx context.Context, participantID uuid.UUID, messageIDs []uuid.UUID) (int32, error)
}

type identityResolver interface {
	ResolveNickname(ctx context.Context, in *identityv1.ResolveNicknameRequest, opts ...grpc.CallOption) (*identityv1.ResolveNicknameResponse, error)
	BatchGetNicknames(ctx context.Context, in *identityv1.BatchGetNicknamesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetNicknamesResponse, error)
	BatchGetIdentityTypes(ctx context.Context, in *identityv1.BatchGetIdentityTypesRequest, opts ...grpc.CallOption) (*identityv1.BatchGetIdentityTypesResponse, error)
}

type agentsService interface {
	GetAgent(ctx context.Context, in *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error)
}

type authorizationChecker interface {
	Check(ctx context.Context, in *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error)
	Write(ctx context.Context, in *authorizationv1.WriteRequest, opts ...grpc.CallOption) (*authorizationv1.WriteResponse, error)
}

type notifierPublisher interface {
	PublishMessageCreated(ctx context.Context, threadID, messageID uuid.UUID, recipients []uuid.UUID) error
}

type meteringRecorder interface {
	RecordThreadCreated(ctx context.Context, orgID, threadID uuid.UUID, createdAt time.Time) error
	RecordMessageSent(ctx context.Context, orgID, threadID, messageID uuid.UUID, createdAt time.Time) error
}

func New(store threadStore, notifier notifierPublisher, authorization authorizationChecker, identity identityResolver, agents agentsService, metering meteringRecorder) *Server {
	return &Server{store: store, notifier: notifier, authorization: authorization, identity: identity, agents: agents, metering: metering}
}

func (s *Server) CreateThread(ctx context.Context, req *threadsv1.CreateThreadRequest) (*threadsv1.CreateThreadResponse, error) {
	initiator, hasInitiator, err := initiatorInfoFromContext(ctx)
	if err != nil {
		return nil, err
	}
	ids := req.GetParticipantIds()
	identifiers := req.GetParticipants()
	if len(ids) > 0 && len(identifiers) > 0 {
		return nil, status.Error(codes.InvalidArgument, "participant_ids and participants are mutually exclusive")
	}
	if len(ids) == 0 && len(identifiers) == 0 && !hasInitiator {
		return nil, status.Error(codes.InvalidArgument, "participant_ids or participants must be provided")
	}
	seen := make(map[uuid.UUID]struct{}, len(ids)+len(identifiers))
	resolved := make([]store.ParticipantInput, 0, len(ids)+len(identifiers))
	if len(ids) > 0 {
		for i, raw := range ids {
			id, err := parseUUID(strings.TrimSpace(raw))
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "participant_ids[%d]: %v", i, err)
			}
			if err := addResolvedParticipant(id, initiator, hasInitiator, seen, &resolved, "participant_ids", i); err != nil {
				return nil, err
			}
		}
	}
	var organizationID uuid.UUID
	organizationResolved := false
	if len(identifiers) > 0 {
		for i, identifier := range identifiers {
			if identifier == nil {
				return nil, status.Errorf(codes.InvalidArgument, "participants[%d]: identifier must be provided", i)
			}
			switch value := identifier.GetIdentifier().(type) {
			case *threadsv1.ParticipantIdentifier_ParticipantId:
				id, err := parseUUID(strings.TrimSpace(value.ParticipantId))
				if err != nil {
					return nil, status.Errorf(codes.InvalidArgument, "participants[%d].participant_id: %v", i, err)
				}
				if err := addResolvedParticipant(id, initiator, hasInitiator, seen, &resolved, "participants", i); err != nil {
					return nil, err
				}
			case *threadsv1.ParticipantIdentifier_ParticipantNickname:
				if !organizationResolved {
					orgID, err := s.organizationIDForNickname(ctx, req.OrganizationId)
					if err != nil {
						return nil, err
					}
					organizationID = orgID
					organizationResolved = true
				}
				id, err := s.resolveNickname(ctx, organizationID, value.ParticipantNickname, fmt.Sprintf("participants[%d].participant_nickname", i))
				if err != nil {
					return nil, err
				}
				if err := addResolvedParticipant(id, initiator, hasInitiator, seen, &resolved, "participants", i); err != nil {
					return nil, err
				}
			default:
				return nil, status.Errorf(codes.InvalidArgument, "participants[%d]: identifier must be provided", i)
			}
		}
	}
	if !organizationResolved {
		orgID, err := s.organizationIDForThread(ctx, req.OrganizationId)
		if err != nil {
			return nil, err
		}
		organizationID = orgID
	}
	identityID, err := identityIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.requireAllowed(ctx, identityID, "can_create_thread", fmt.Sprintf("%s%s", organizationObjectPrefix, organizationID.String())); err != nil {
		return nil, err
	}
	capacity := len(resolved)
	if hasInitiator {
		capacity++
	}
	participants := make([]store.ParticipantInput, 0, capacity)
	if hasInitiator {
		participants = append(participants, store.ParticipantInput{ID: initiator.ID, Passive: initiator.Passive})
	}
	participants = append(participants, resolved...)
	if err := s.requireCanInitiateAgentParticipants(ctx, identityID, participants); err != nil {
		return nil, err
	}

	thread, err := s.store.CreateThread(ctx, organizationID, participants)
	if err != nil {
		return nil, toStatusError(err)
	}
	if thread.OrganizationID == nil {
		return nil, status.Error(codes.Internal, "thread organization_id missing")
	}
	threadObject := fmt.Sprintf("%s%s", threadObjectPrefix, thread.ID.String())
	writes := make([]*authorizationv1.TupleKey, 0, 1+len(thread.Participants))
	writes = append(writes, &authorizationv1.TupleKey{
		User:     fmt.Sprintf("%s%s", organizationObjectPrefix, thread.OrganizationID.String()),
		Relation: "org",
		Object:   threadObject,
	})
	for _, participant := range thread.Participants {
		writes = append(writes, &authorizationv1.TupleKey{
			User:     fmt.Sprintf("%s%s", identityObjectPrefix, participant.ID.String()),
			Relation: "participant",
			Object:   threadObject,
		})
	}
	if err := s.writeTuples(ctx, writes); err != nil {
		return nil, err
	}
	s.recordThreadCreated(ctx, thread)
	return &threadsv1.CreateThreadResponse{Thread: toProtoThread(thread)}, nil
}

func addResolvedParticipant(id uuid.UUID, initiator initiatorInfo, hasInitiator bool, seen map[uuid.UUID]struct{}, resolved *[]store.ParticipantInput, fieldName string, index int) error {
	if hasInitiator && id == initiator.ID {
		return status.Errorf(codes.InvalidArgument, "%s must not include initiator", fieldName)
	}
	if _, ok := seen[id]; ok {
		return status.Errorf(codes.InvalidArgument, "%s[%d]: duplicate participant", fieldName, index)
	}
	seen[id] = struct{}{}
	*resolved = append(*resolved, store.ParticipantInput{ID: id, Passive: false})
	return nil
}

func (s *Server) requireCanInitiateAgentParticipants(ctx context.Context, callerID uuid.UUID, participants []store.ParticipantInput) error {
	agentIDs, err := s.agentParticipantIDs(ctx, participants)
	if err != nil {
		return err
	}
	for _, agentID := range agentIDs {
		if err := s.requireAllowed(ctx, callerID, "can_initiate", fmt.Sprintf("agent:%s", agentID.String())); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) agentParticipantIDs(ctx context.Context, participants []store.ParticipantInput) ([]uuid.UUID, error) {
	if len(participants) == 0 {
		return nil, nil
	}
	if s.identity == nil {
		return nil, status.Error(codes.Internal, "identity service not configured")
	}
	identityIDs := make([]string, len(participants))
	participantIDs := make(map[uuid.UUID]struct{}, len(participants))
	for i, participant := range participants {
		identityIDs[i] = participant.ID.String()
		participantIDs[participant.ID] = struct{}{}
	}
	identityCtx, err := identityClientContext(ctx)
	if err != nil {
		return nil, err
	}
	response, err := s.identity.BatchGetIdentityTypes(identityCtx, &identityv1.BatchGetIdentityTypesRequest{IdentityIds: identityIDs})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "batch get identity types: %v", err)
	}
	agentIDs := make([]uuid.UUID, 0)
	for i, entry := range response.GetEntries() {
		if entry == nil {
			return nil, status.Errorf(codes.Internal, "identity type entry[%d]: missing", i)
		}
		if entry.GetIdentityType() != identityv1.IdentityType_IDENTITY_TYPE_AGENT {
			continue
		}
		identityID, err := parseUUID(entry.GetIdentityId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "identity type entry[%d].identity_id: %v", i, err)
		}
		if _, ok := participantIDs[identityID]; !ok {
			return nil, status.Errorf(codes.Internal, "identity type entry[%d].identity_id: unexpected identity", i)
		}
		agentIDs = append(agentIDs, identityID)
	}
	return agentIDs, nil
}

func (s *Server) ArchiveThread(ctx context.Context, req *threadsv1.ArchiveThreadRequest) (*threadsv1.ArchiveThreadResponse, error) {
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	identityID, err := identityIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	thread, err := s.store.GetThread(ctx, threadID)
	if err != nil {
		return nil, toStatusError(err)
	}
	if thread.OrganizationID == nil {
		return nil, status.Error(codes.NotFound, store.ErrThreadNotFound.Error())
	}
	threadObject := fmt.Sprintf("%s%s", threadObjectPrefix, threadID.String())
	allowed, err := s.checkAllowed(ctx, identityID, "participant", threadObject)
	if err != nil {
		return nil, err
	}
	if !allowed {
		allowed, err = s.checkAllowed(ctx, identityID, "owner", fmt.Sprintf("%s%s", organizationObjectPrefix, thread.OrganizationID.String()))
		if err != nil {
			return nil, err
		}
		if !allowed {
			return nil, status.Error(codes.PermissionDenied, "permission denied")
		}
	}
	thread, err = s.store.ArchiveThread(ctx, threadID)
	if err != nil {
		return nil, toStatusError(err)
	}
	return &threadsv1.ArchiveThreadResponse{Thread: toProtoThread(thread)}, nil
}

func (s *Server) DegradeThread(ctx context.Context, req *threadsv1.DegradeThreadRequest) (*threadsv1.DegradeThreadResponse, error) {
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	reason := strings.TrimSpace(req.GetReason())
	if reason == "" {
		log.Printf("degrade thread requested without reason: thread_id=%s", threadID.String())
	} else {
		log.Printf("degrade thread requested: thread_id=%s reason=%s", threadID.String(), reason)
	}
	thread, err := s.store.DegradeThread(ctx, threadID)
	if err != nil {
		return nil, toStatusError(err)
	}
	return &threadsv1.DegradeThreadResponse{Thread: toProtoThread(thread)}, nil
}

func (s *Server) AddParticipant(ctx context.Context, req *threadsv1.AddParticipantRequest) (*threadsv1.AddParticipantResponse, error) {
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	identityID, err := identityIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.requireAllowed(ctx, identityID, "can_add_participant", fmt.Sprintf("%s%s", threadObjectPrefix, threadID.String())); err != nil {
		return nil, err
	}
	participantID, err := s.resolveParticipantID(ctx, req)
	if err != nil {
		return nil, err
	}
	if err := s.requireCanInitiateAgentParticipants(ctx, identityID, []store.ParticipantInput{{ID: participantID}}); err != nil {
		return nil, err
	}
	thread, err := s.store.AddParticipant(ctx, threadID, participantID, req.GetPassive())
	if err != nil {
		return nil, toStatusError(err)
	}
	if err := s.writeTuples(ctx, []*authorizationv1.TupleKey{
		{
			User:     fmt.Sprintf("%s%s", identityObjectPrefix, participantID.String()),
			Relation: "participant",
			Object:   fmt.Sprintf("%s%s", threadObjectPrefix, threadID.String()),
		},
	}); err != nil {
		return nil, err
	}
	return &threadsv1.AddParticipantResponse{Thread: toProtoThread(thread)}, nil
}

func (s *Server) SendMessage(ctx context.Context, req *threadsv1.SendMessageRequest) (*threadsv1.SendMessageResponse, error) {
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	if req.GetBody() == "" && len(req.GetFileIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "body or file_ids must be provided")
	}
	fileIDs := make([]uuid.UUID, len(req.GetFileIds()))
	for i, raw := range req.GetFileIds() {
		id, err := parseUUID(raw)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "file_ids[%d]: %v", i, err)
		}
		fileIDs[i] = id
	}
	identityID, err := identityIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	senderID := identityID
	if senderValue := strings.TrimSpace(req.GetSenderId()); senderValue != "" {
		senderID, err = parseUUID(senderValue)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "sender_id: %v", err)
		}
		if senderID != identityID {
			return nil, status.Error(codes.PermissionDenied, "permission denied")
		}
	}
	if err := s.requireAllowed(ctx, identityID, "can_write", fmt.Sprintf("%s%s", threadObjectPrefix, threadID.String())); err != nil {
		return nil, err
	}

	result, err := s.store.SendMessage(ctx, threadID, senderID, req.GetBody(), fileIDs)
	if err != nil {
		return nil, toStatusError(err)
	}
	s.recordMessageSent(ctx, result)
	if err := s.notifier.PublishMessageCreated(ctx, threadID, result.Message.ID, result.Recipients); err != nil {
		return nil, status.Errorf(codes.Internal, "notify recipients: %v", err)
	}
	return &threadsv1.SendMessageResponse{Message: toProtoMessage(result.Message)}, nil
}

func (s *Server) GetThreads(ctx context.Context, req *threadsv1.GetThreadsRequest) (*threadsv1.GetThreadsResponse, error) {
	participantID, err := parseUUID(req.GetParticipantId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "participant_id: %v", err)
	}
	identityID, err := identityIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if participantID != identityID {
		return nil, status.Error(codes.PermissionDenied, "permission denied")
	}
	var cursor *store.ThreadCursor
	if token := req.GetPageToken(); token != "" {
		tokenID, tokenCursor, err := store.DecodeThreadPageToken(token)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", err)
		}
		if tokenID != participantID {
			return nil, status.Error(codes.InvalidArgument, "page_token does not match participant")
		}
		cursor = &tokenCursor
	}

	result, err := s.store.ListThreads(ctx, participantID, req.GetPageSize(), cursor)
	if err != nil {
		return nil, toStatusError(err)
	}
	resp := &threadsv1.GetThreadsResponse{Threads: make([]*threadsv1.Thread, len(result.Threads))}
	for i, thread := range result.Threads {
		resp.Threads[i] = toProtoThread(thread)
	}
	if result.NextCursor != nil {
		token, err := store.EncodeThreadPageToken(participantID, *result.NextCursor)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode page token: %v", err)
		}
		resp.NextPageToken = token
	}
	return resp, nil
}

func (s *Server) ListOrganizationThreads(ctx context.Context, req *threadsv1.ListOrganizationThreadsRequest) (*threadsv1.ListOrganizationThreadsResponse, error) {
	organizationID, err := parseUUID(req.GetOrganizationId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
	}
	identityID, err := identityIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.checkCanViewThreads(ctx, identityID, organizationID); err != nil {
		return nil, err
	}

	filter, err := listOrganizationThreadsFilterFromProto(req.GetFilter())
	if err != nil {
		return nil, err
	}
	sortSpec, err := listOrganizationThreadsSortFromProto(req.GetSort())
	if err != nil {
		return nil, err
	}

	var cursor *store.OrganizationThreadCursor
	if token := req.GetPageToken(); token != "" {
		tokenOrgID, tokenFilter, tokenSort, tokenCursor, err := store.DecodeOrganizationThreadPageToken(token)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", err)
		}
		if tokenOrgID != organizationID {
			return nil, status.Error(codes.InvalidArgument, "page_token does not match organization")
		}
		if !organizationThreadFilterEqual(tokenFilter, filter) || !organizationThreadSortEqual(tokenSort, sortSpec) {
			return nil, status.Error(codes.InvalidArgument, "page_token does not match request")
		}
		cursor = &tokenCursor
	}

	result, err := s.store.ListOrganizationThreads(ctx, organizationID, filter, sortSpec, req.GetPageSize(), cursor)
	if err != nil {
		return nil, toStatusError(err)
	}

	nicknames, err := s.batchResolveNicknames(ctx, organizationID, result.Threads)
	if err != nil {
		return nil, err
	}

	resp := &threadsv1.ListOrganizationThreadsResponse{Threads: make([]*threadsv1.Thread, len(result.Threads))}
	for i, thread := range result.Threads {
		resp.Threads[i] = toProtoThreadWithNicknames(thread, nicknames)
	}
	if result.NextCursor != nil {
		token, err := store.EncodeOrganizationThreadPageToken(organizationID, filter, sortSpec, *result.NextCursor)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode page token: %v", err)
		}
		resp.NextPageToken = token
	}
	return resp, nil
}

func (s *Server) GetOrganizationThreads(ctx context.Context, req *threadsv1.GetOrganizationThreadsRequest) (*threadsv1.GetOrganizationThreadsResponse, error) {
	organizationID, err := parseUUID(req.GetOrganizationId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
	}
	identityID, err := identityIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.checkCanViewThreads(ctx, identityID, organizationID); err != nil {
		return nil, err
	}

	statusFilter, err := threadStatusFilterFromProto(req.GetStatus())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "status: %v", err)
	}
	filter := store.OrganizationThreadFilter{}
	if statusFilter != nil {
		filter.StatusIn = []store.ThreadStatus{*statusFilter}
	}
	sortSpec := store.OrganizationThreadSort{Field: store.OrganizationThreadSortFieldCreated, Direction: store.SortDirectionDesc}

	var cursor *store.OrganizationThreadCursor
	if token := req.GetPageToken(); token != "" {
		tokenOrgID, tokenFilter, tokenSort, tokenCursor, err := store.DecodeOrganizationThreadPageToken(token)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", err)
		}
		if tokenOrgID != organizationID {
			return nil, status.Error(codes.InvalidArgument, "page_token does not match organization")
		}
		if !organizationThreadFilterEqual(tokenFilter, filter) || !organizationThreadSortEqual(tokenSort, sortSpec) {
			return nil, status.Error(codes.InvalidArgument, "page_token does not match request")
		}
		cursor = &tokenCursor
	}

	result, err := s.store.ListOrganizationThreads(ctx, organizationID, filter, sortSpec, req.GetPageSize(), cursor)
	if err != nil {
		return nil, toStatusError(err)
	}
	resp := &threadsv1.GetOrganizationThreadsResponse{Threads: make([]*threadsv1.Thread, len(result.Threads))}
	for i, thread := range result.Threads {
		resp.Threads[i] = toProtoThread(thread)
	}
	if result.NextCursor != nil {
		token, err := store.EncodeOrganizationThreadPageToken(organizationID, filter, sortSpec, *result.NextCursor)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode page token: %v", err)
		}
		resp.NextPageToken = token
	}
	return resp, nil
}

func (s *Server) GetThread(ctx context.Context, req *threadsv1.GetThreadRequest) (*threadsv1.GetThreadResponse, error) {
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	identityID, err := identityIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	thread, err := s.store.GetThread(ctx, threadID)
	if err != nil {
		return nil, toStatusError(err)
	}
	if thread.OrganizationID == nil {
		return nil, status.Error(codes.NotFound, store.ErrThreadNotFound.Error())
	}
	if err := s.requireAllowed(ctx, identityID, "can_read", fmt.Sprintf("%s%s", threadObjectPrefix, threadID.String())); err != nil {
		return nil, err
	}
	return &threadsv1.GetThreadResponse{Thread: toProtoThread(thread)}, nil
}

func (s *Server) GetMessages(ctx context.Context, req *threadsv1.GetMessagesRequest) (*threadsv1.GetMessagesResponse, error) {
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	order, err := messageOrderFromProto(req.GetOrder())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "order: %v", err)
	}
	var cursor *store.MessageCursor
	if token := req.GetPageToken(); token != "" {
		tokenID, tokenCursor, err := store.DecodeThreadMessagePageToken(token)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", err)
		}
		if tokenID != threadID {
			return nil, status.Error(codes.InvalidArgument, "page_token does not match thread")
		}
		cursor = &tokenCursor
	}
	identityID, err := identityIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := s.requireAllowed(ctx, identityID, "can_read", fmt.Sprintf("%s%s", threadObjectPrefix, threadID.String())); err != nil {
		return nil, err
	}

	result, err := s.store.ListMessages(ctx, threadID, req.GetPageSize(), cursor, order)
	if err != nil {
		return nil, toStatusError(err)
	}
	resp := &threadsv1.GetMessagesResponse{Messages: make([]*threadsv1.Message, len(result.Messages))}
	for i, message := range result.Messages {
		resp.Messages[i] = toProtoMessage(message)
	}
	if result.NextCursor != nil {
		token, err := store.EncodeThreadMessagePageToken(threadID, *result.NextCursor)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode page token: %v", err)
		}
		resp.NextPageToken = token
	}
	return resp, nil
}

func (s *Server) GetUnackedMessages(ctx context.Context, req *threadsv1.GetUnackedMessagesRequest) (*threadsv1.GetUnackedMessagesResponse, error) {
	participantID, err := parseUUID(req.GetParticipantId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "participant_id: %v", err)
	}
	identityID, err := identityIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if identityID != participantID {
		return nil, status.Error(codes.PermissionDenied, "permission denied")
	}
	var threadID *uuid.UUID
	if req.ThreadId != nil {
		parsedID, err := parseUUID(req.GetThreadId())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
		}
		threadID = &parsedID
	}
	var cursor *store.MessageCursor
	if token := req.GetPageToken(); token != "" {
		tokenID, tokenCursor, err := store.DecodeUnackedMessagePageToken(token)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", err)
		}
		if tokenID != participantID {
			return nil, status.Error(codes.InvalidArgument, "page_token does not match participant")
		}
		cursor = &tokenCursor
	}

	result, err := s.store.ListUnackedMessages(ctx, participantID, threadID, req.GetPageSize(), cursor)
	if err != nil {
		return nil, toStatusError(err)
	}
	resp := &threadsv1.GetUnackedMessagesResponse{Messages: make([]*threadsv1.Message, len(result.Messages))}
	for i, message := range result.Messages {
		resp.Messages[i] = toProtoMessage(message)
	}
	if result.NextCursor != nil {
		token, err := store.EncodeUnackedMessagePageToken(participantID, *result.NextCursor)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode page token: %v", err)
		}
		resp.NextPageToken = token
	}
	return resp, nil
}

func (s *Server) GetUnackedMessageCounts(ctx context.Context, req *threadsv1.GetUnackedMessageCountsRequest) (*threadsv1.GetUnackedMessageCountsResponse, error) {
	participantID, err := parseUUID(req.GetParticipantId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "participant_id: %v", err)
	}
	identityID, err := identityIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if identityID != participantID {
		return nil, status.Error(codes.PermissionDenied, "permission denied")
	}

	counts, err := s.store.GetUnackedMessageCounts(ctx, participantID)
	if err != nil {
		return nil, toStatusError(err)
	}
	resp := &threadsv1.GetUnackedMessageCountsResponse{CountsByThreadId: make(map[string]int32, len(counts))}
	for threadID, count := range counts {
		if count <= 0 {
			continue
		}
		resp.CountsByThreadId[threadID.String()] = count
	}
	return resp, nil
}

func (s *Server) AckMessages(ctx context.Context, req *threadsv1.AckMessagesRequest) (*threadsv1.AckMessagesResponse, error) {
	participantID, err := parseUUID(req.GetParticipantId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "participant_id: %v", err)
	}
	identityID, err := identityIDFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if identityID != participantID {
		return nil, status.Error(codes.PermissionDenied, "permission denied")
	}
	if len(req.GetMessageIds()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "message_ids must be provided")
	}
	messageIDs := make([]uuid.UUID, len(req.GetMessageIds()))
	for i, raw := range req.GetMessageIds() {
		id, err := parseUUID(raw)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "message_ids[%d]: %v", i, err)
		}
		messageIDs[i] = id
	}

	count, err := s.store.AckMessages(ctx, participantID, messageIDs)
	if err != nil {
		return nil, toStatusError(err)
	}
	return &threadsv1.AckMessagesResponse{AckedCount: count}, nil
}

func (s *Server) recordThreadCreated(ctx context.Context, thread store.Thread) {
	if thread.OrganizationID == nil {
		panic("thread organization_id missing")
	}
	threadID := thread.ID
	orgID := *thread.OrganizationID
	createdAt := thread.CreatedAt
	s.recordUsageAsync(ctx, "thread_created", func(recordCtx context.Context) error {
		return s.metering.RecordThreadCreated(recordCtx, orgID, threadID, createdAt)
	})
}

func (s *Server) recordMessageSent(ctx context.Context, result store.SendMessageResult) {
	orgID := result.OrganizationID
	message := result.Message
	messageID := message.ID
	threadID := message.ThreadID
	createdAt := message.CreatedAt
	s.recordUsageAsync(ctx, "message_sent", func(recordCtx context.Context) error {
		return s.metering.RecordMessageSent(recordCtx, orgID, threadID, messageID, createdAt)
	})
}

func (s *Server) recordUsageAsync(ctx context.Context, label string, record func(context.Context) error) {
	if s.metering == nil {
		return
	}
	go func() {
		recordCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), meteringTimeout)
		defer cancel()
		if err := record(recordCtx); err != nil {
			log.Printf("metering: %s: %v", label, err)
		}
	}()
}

func (s *Server) resolveParticipantID(ctx context.Context, req *threadsv1.AddParticipantRequest) (uuid.UUID, error) {
	if req.GetParticipant() != nil {
		identifier := req.GetParticipant().GetIdentifier()
		switch value := identifier.(type) {
		case *threadsv1.ParticipantIdentifier_ParticipantId:
			participantID, err := parseUUID(strings.TrimSpace(value.ParticipantId))
			if err != nil {
				return uuid.UUID{}, status.Errorf(codes.InvalidArgument, "participant.participant_id: %v", err)
			}
			return participantID, nil
		case *threadsv1.ParticipantIdentifier_ParticipantNickname:
			return s.resolveAddParticipantNickname(ctx, req.OrganizationId, value.ParticipantNickname)
		default:
			return uuid.UUID{}, status.Error(codes.InvalidArgument, "participant identifier must be provided")
		}
	}
	if req.GetParticipantId() != "" {
		participantID, err := parseUUID(strings.TrimSpace(req.GetParticipantId()))
		if err != nil {
			return uuid.UUID{}, status.Errorf(codes.InvalidArgument, "participant_id: %v", err)
		}
		return participantID, nil
	}
	return uuid.UUID{}, status.Error(codes.InvalidArgument, "participant identifier must be provided")
}

func (s *Server) resolveAddParticipantNickname(ctx context.Context, organizationIDValue *string, nickname string) (uuid.UUID, error) {
	organizationID, err := s.organizationIDForNickname(ctx, organizationIDValue)
	if err != nil {
		return uuid.UUID{}, err
	}
	return s.resolveNickname(ctx, organizationID, nickname, "participant.participant_nickname")
}

func (s *Server) resolveNickname(ctx context.Context, organizationID uuid.UUID, nickname string, fieldName string) (uuid.UUID, error) {
	trimmed := strings.TrimSpace(nickname)
	if trimmed == "" {
		return uuid.UUID{}, status.Errorf(codes.InvalidArgument, "%s must be provided", fieldName)
	}
	cleaned := strings.TrimPrefix(trimmed, "@")
	if cleaned == "" {
		return uuid.UUID{}, status.Errorf(codes.InvalidArgument, "%s must be provided", fieldName)
	}
	identityCtx, err := identityClientContext(ctx)
	if err != nil {
		return uuid.UUID{}, err
	}
	response, err := s.identity.ResolveNickname(identityCtx, &identityv1.ResolveNicknameRequest{
		OrganizationId: organizationID.String(),
		Nickname:       cleaned,
	})
	if err != nil {
		return uuid.UUID{}, status.Errorf(codes.Internal, "resolve nickname: %v", err)
	}
	participantID, err := parseUUID(response.GetIdentityId())
	if err != nil {
		return uuid.UUID{}, status.Errorf(codes.Internal, "resolve nickname identity_id: %v", err)
	}
	return participantID, nil
}

func (s *Server) organizationIDForNickname(ctx context.Context, organizationIDValue *string) (uuid.UUID, error) {
	return s.organizationIDForRequest(ctx, organizationIDValue, "organization_id must be provided for participant_nickname")
}

func (s *Server) organizationIDForThread(ctx context.Context, organizationIDValue *string) (uuid.UUID, error) {
	return s.organizationIDForRequest(ctx, organizationIDValue, "organization_id must be provided")
}

func (s *Server) organizationIDForRequest(ctx context.Context, organizationIDValue *string, missingMessage string) (uuid.UUID, error) {
	var requestedID *uuid.UUID
	if organizationIDValue != nil {
		organizationID, err := parseUUID(strings.TrimSpace(*organizationIDValue))
		if err != nil {
			return uuid.UUID{}, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
		}
		requestedID = &organizationID
	}
	organizationID, ok, err := organizationIDFromContext(ctx)
	if err != nil {
		return uuid.UUID{}, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if ok {
		if requestedID != nil && *requestedID != organizationID {
			return uuid.UUID{}, status.Error(codes.InvalidArgument, "organization_id does not match identity organization")
		}
		return organizationID, nil
	}
	if requestedID != nil {
		if isAgentIdentity(ctx) {
			identityOrgID, err := s.organizationIDFromIdentity(ctx, missingMessage)
			if err != nil {
				return uuid.UUID{}, err
			}
			if identityOrgID != *requestedID {
				return uuid.UUID{}, status.Error(codes.InvalidArgument, "organization_id does not match identity organization")
			}
			return identityOrgID, nil
		}
		return *requestedID, nil
	}
	return s.organizationIDFromIdentity(ctx, missingMessage)
}

func organizationIDFromContext(ctx context.Context) (uuid.UUID, bool, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return uuid.UUID{}, false, nil
	}
	value := metadataValue(md, organizationIDMetadataKey)
	if value == "" {
		return uuid.UUID{}, false, nil
	}
	orgID, err := parseUUID(value)
	if err != nil {
		return uuid.UUID{}, false, fmt.Errorf("organization_id: %w", err)
	}
	return orgID, true, nil
}

func (s *Server) organizationIDFromIdentity(ctx context.Context, missingMessage string) (uuid.UUID, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return uuid.UUID{}, status.Error(codes.InvalidArgument, missingMessage)
	}
	identityID := metadataValue(md, identityIDMetadataKey)
	identityType := metadataValue(md, identityTypeMetadataKey)
	if identityID == "" || identityType == "" {
		return uuid.UUID{}, status.Error(codes.InvalidArgument, missingMessage)
	}
	if !strings.EqualFold(identityType, agentIdentityType) {
		return uuid.UUID{}, status.Error(codes.InvalidArgument, missingMessage)
	}
	if s.agents == nil {
		return uuid.UUID{}, status.Error(codes.Internal, "agents service not configured")
	}
	response, err := s.agents.GetAgent(ctx, &agentsv1.GetAgentRequest{Id: identityID})
	if err != nil {
		return uuid.UUID{}, status.Errorf(codes.Internal, "get agent: %v", err)
	}
	agent := response.GetAgent()
	if agent == nil {
		return uuid.UUID{}, status.Error(codes.Internal, "get agent: agent missing")
	}
	orgIDValue := strings.TrimSpace(agent.GetOrganizationId())
	if orgIDValue == "" {
		return uuid.UUID{}, status.Error(codes.Internal, "get agent: organization_id missing")
	}
	orgID, err := parseUUID(orgIDValue)
	if err != nil {
		return uuid.UUID{}, status.Errorf(codes.Internal, "get agent organization_id: %v", err)
	}
	return orgID, nil
}

func identityIDFromContext(ctx context.Context) (uuid.UUID, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return uuid.UUID{}, status.Error(codes.Unauthenticated, "identity not available")
	}
	identityID := metadataValue(md, identityIDMetadataKey)
	if identityID == "" {
		return uuid.UUID{}, status.Error(codes.Unauthenticated, "identity not available")
	}
	parsedID, err := parseUUID(identityID)
	if err != nil {
		return uuid.UUID{}, status.Errorf(codes.InvalidArgument, "identity_id: %v", err)
	}
	return parsedID, nil
}

func isAgentIdentity(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	identityType := metadataValue(md, identityTypeMetadataKey)
	return strings.EqualFold(identityType, agentIdentityType)
}

func (s *Server) checkAllowed(ctx context.Context, identityID uuid.UUID, relation, object string) (bool, error) {
	if s.authorization == nil {
		return false, status.Error(codes.Internal, "authorization service not configured")
	}
	response, err := s.authorization.Check(ctx, &authorizationv1.CheckRequest{
		TupleKey: &authorizationv1.TupleKey{
			User:     fmt.Sprintf("%s%s", identityObjectPrefix, identityID.String()),
			Relation: relation,
			Object:   object,
		},
	})
	if err != nil {
		return false, status.Errorf(codes.Internal, "authorization check: %v", err)
	}
	return response.GetAllowed(), nil
}

func (s *Server) requireAllowed(ctx context.Context, identityID uuid.UUID, relation, object string) error {
	allowed, err := s.checkAllowed(ctx, identityID, relation, object)
	if err != nil {
		return err
	}
	if !allowed {
		return status.Error(codes.PermissionDenied, "permission denied")
	}
	return nil
}

func (s *Server) writeTuples(ctx context.Context, writes []*authorizationv1.TupleKey) error {
	if s.authorization == nil {
		return status.Error(codes.Internal, "authorization service not configured")
	}
	_, err := s.authorization.Write(ctx, &authorizationv1.WriteRequest{Writes: writes})
	if err != nil {
		return status.Errorf(codes.Internal, "authorization write: %v", err)
	}
	return nil
}

func (s *Server) checkCanViewThreads(ctx context.Context, identityID, organizationID uuid.UUID) error {
	return s.requireAllowed(ctx, identityID, "can_view_threads", fmt.Sprintf("%s%s", organizationObjectPrefix, organizationID.String()))
}

func listOrganizationThreadsFilterFromProto(filter *threadsv1.ListOrganizationThreadsFilter) (store.OrganizationThreadFilter, error) {
	if filter == nil {
		return store.OrganizationThreadFilter{}, nil
	}

	result := store.OrganizationThreadFilter{}
	if len(filter.GetStatusIn()) > 0 {
		seen := make(map[store.ThreadStatus]struct{}, len(filter.GetStatusIn()))
		for i, statusValue := range filter.GetStatusIn() {
			parsed, err := threadStatusFromProto(statusValue)
			if err != nil {
				return store.OrganizationThreadFilter{}, status.Errorf(codes.InvalidArgument, "filter.status_in[%d]: %v", i, err)
			}
			if _, ok := seen[parsed]; ok {
				continue
			}
			seen[parsed] = struct{}{}
			result.StatusIn = append(result.StatusIn, parsed)
		}
		sort.Slice(result.StatusIn, func(i, j int) bool { return result.StatusIn[i] < result.StatusIn[j] })
	}

	if len(filter.GetParticipantIdIn()) > 0 {
		seen := make(map[uuid.UUID]struct{}, len(filter.GetParticipantIdIn()))
		for i, raw := range filter.GetParticipantIdIn() {
			participantID, err := parseUUID(strings.TrimSpace(raw))
			if err != nil {
				return store.OrganizationThreadFilter{}, status.Errorf(codes.InvalidArgument, "filter.participant_id_in[%d]: %v", i, err)
			}
			if _, ok := seen[participantID]; ok {
				continue
			}
			seen[participantID] = struct{}{}
			result.ParticipantIDs = append(result.ParticipantIDs, participantID)
		}
		sort.Slice(result.ParticipantIDs, func(i, j int) bool { return result.ParticipantIDs[i].String() < result.ParticipantIDs[j].String() })
	}

	if filter.GetCreatedAfter() != nil {
		if err := filter.GetCreatedAfter().CheckValid(); err != nil {
			return store.OrganizationThreadFilter{}, status.Errorf(codes.InvalidArgument, "filter.created_after: %v", err)
		}
		value := filter.GetCreatedAfter().AsTime().UTC()
		result.CreatedAfter = &value
	}
	if filter.GetCreatedBefore() != nil {
		if err := filter.GetCreatedBefore().CheckValid(); err != nil {
			return store.OrganizationThreadFilter{}, status.Errorf(codes.InvalidArgument, "filter.created_before: %v", err)
		}
		value := filter.GetCreatedBefore().AsTime().UTC()
		result.CreatedBefore = &value
	}

	return result, nil
}

func listOrganizationThreadsSortFromProto(sortSpec *threadsv1.ListOrganizationThreadsSort) (store.OrganizationThreadSort, error) {
	field := threadsv1.ListOrganizationThreadsSortField_LIST_ORGANIZATION_THREADS_SORT_FIELD_CREATED
	direction := threadsv1.SortDirection_SORT_DIRECTION_DESC
	if sortSpec != nil {
		if sortSpec.GetField() != threadsv1.ListOrganizationThreadsSortField_LIST_ORGANIZATION_THREADS_SORT_FIELD_UNSPECIFIED {
			field = sortSpec.GetField()
		}
		if sortSpec.GetDirection() != threadsv1.SortDirection_SORT_DIRECTION_UNSPECIFIED {
			direction = sortSpec.GetDirection()
		}
	}

	var storeField store.OrganizationThreadSortField
	switch field {
	case threadsv1.ListOrganizationThreadsSortField_LIST_ORGANIZATION_THREADS_SORT_FIELD_CREATED:
		storeField = store.OrganizationThreadSortFieldCreated
	case threadsv1.ListOrganizationThreadsSortField_LIST_ORGANIZATION_THREADS_SORT_FIELD_UPDATED:
		storeField = store.OrganizationThreadSortFieldUpdated
	case threadsv1.ListOrganizationThreadsSortField_LIST_ORGANIZATION_THREADS_SORT_FIELD_MESSAGE_COUNT:
		storeField = store.OrganizationThreadSortFieldMessageCount
	case threadsv1.ListOrganizationThreadsSortField_LIST_ORGANIZATION_THREADS_SORT_FIELD_STATUS:
		storeField = store.OrganizationThreadSortFieldStatus
	default:
		return store.OrganizationThreadSort{}, status.Error(codes.InvalidArgument, "sort.field: invalid sort field")
	}

	var storeDirection store.SortDirection
	switch direction {
	case threadsv1.SortDirection_SORT_DIRECTION_ASC:
		storeDirection = store.SortDirectionAsc
	case threadsv1.SortDirection_SORT_DIRECTION_DESC:
		storeDirection = store.SortDirectionDesc
	default:
		return store.OrganizationThreadSort{}, status.Error(codes.InvalidArgument, "sort.direction: invalid sort direction")
	}

	return store.OrganizationThreadSort{
		Field:     storeField,
		Direction: storeDirection,
	}, nil
}

func threadStatusFromProto(status threadsv1.ThreadStatus) (store.ThreadStatus, error) {
	switch status {
	case threadsv1.ThreadStatus_THREAD_STATUS_ACTIVE:
		return store.ThreadStatusActive, nil
	case threadsv1.ThreadStatus_THREAD_STATUS_ARCHIVED:
		return store.ThreadStatusArchived, nil
	case threadsv1.ThreadStatus_THREAD_STATUS_DEGRADED:
		return store.ThreadStatusDegraded, nil
	default:
		return store.ThreadStatusUnspecified, errors.New("invalid thread status")
	}
}

func threadStatusFilterFromProto(status threadsv1.ThreadStatus) (*store.ThreadStatus, error) {
	switch status {
	case threadsv1.ThreadStatus_THREAD_STATUS_UNSPECIFIED:
		return nil, nil
	case threadsv1.ThreadStatus_THREAD_STATUS_ACTIVE:
		value := store.ThreadStatusActive
		return &value, nil
	case threadsv1.ThreadStatus_THREAD_STATUS_ARCHIVED:
		value := store.ThreadStatusArchived
		return &value, nil
	case threadsv1.ThreadStatus_THREAD_STATUS_DEGRADED:
		value := store.ThreadStatusDegraded
		return &value, nil
	default:
		return nil, errors.New("invalid thread status")
	}
}

func messageOrderFromProto(order threadsv1.MessageOrder) (store.MessageOrder, error) {
	switch order {
	case threadsv1.MessageOrder_MESSAGE_ORDER_UNSPECIFIED, threadsv1.MessageOrder_MESSAGE_ORDER_OLDEST_FIRST:
		return store.MessageOrderOldestFirst, nil
	case threadsv1.MessageOrder_MESSAGE_ORDER_NEWEST_FIRST:
		return store.MessageOrderNewestFirst, nil
	default:
		return store.MessageOrderOldestFirst, errors.New("invalid message order")
	}
}

func organizationThreadFilterEqual(left, right store.OrganizationThreadFilter) bool {
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

func organizationThreadSortEqual(left, right store.OrganizationThreadSort) bool {
	return left.Field == right.Field && left.Direction == right.Direction
}

func (s *Server) batchResolveNicknames(ctx context.Context, organizationID uuid.UUID, threads []store.Thread) (map[uuid.UUID]string, error) {
	if s.identity == nil {
		return nil, status.Error(codes.Internal, "identity service not configured")
	}
	participantIDs := make(map[uuid.UUID]struct{})
	for _, thread := range threads {
		for _, participant := range thread.Participants {
			participantIDs[participant.ID] = struct{}{}
		}
	}
	if len(participantIDs) == 0 {
		return map[uuid.UUID]string{}, nil
	}

	identityIDs := make([]string, 0, len(participantIDs))
	for id := range participantIDs {
		identityIDs = append(identityIDs, id.String())
	}
	sort.Strings(identityIDs)

	identityCtx, err := identityClientContext(ctx)
	if err != nil {
		return nil, err
	}
	response, err := s.identity.BatchGetNicknames(identityCtx, &identityv1.BatchGetNicknamesRequest{
		OrganizationId: organizationID.String(),
		IdentityIds:    identityIDs,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "batch get nicknames: %v", err)
	}

	nicknames := make(map[uuid.UUID]string, len(response.GetEntries()))
	for i, entry := range response.GetEntries() {
		if entry == nil {
			return nil, status.Errorf(codes.Internal, "nickname entry[%d]: missing", i)
		}
		identityID, err := parseUUID(entry.GetIdentityId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "nickname entry[%d].identity_id: %v", i, err)
		}
		nicknames[identityID] = entry.GetNickname()
	}
	return nicknames, nil
}

type initiatorInfo struct {
	ID      uuid.UUID
	Passive bool
}

func initiatorInfoFromContext(ctx context.Context) (initiatorInfo, bool, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return initiatorInfo{}, false, nil
	}
	identityID := metadataValue(md, identityIDMetadataKey)
	identityType := metadataValue(md, identityTypeMetadataKey)
	if identityID == "" || identityType == "" {
		return initiatorInfo{}, false, nil
	}
	initiatorID, err := parseUUID(identityID)
	if err != nil {
		return initiatorInfo{}, false, status.Errorf(codes.InvalidArgument, "identity_id: %v", err)
	}
	passive := strings.EqualFold(identityType, agentIdentityType)
	return initiatorInfo{ID: initiatorID, Passive: passive}, true, nil
}

func metadataValue(md metadata.MD, key string) string {
	values := md.Get(key)
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		return trimmed
	}
	return ""
}

func identityClientContext(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "identity not available")
	}
	identityID := metadataValue(md, identityIDMetadataKey)
	if identityID == "" {
		return nil, status.Error(codes.Unauthenticated, "identity not available")
	}
	outgoing := metadata.MD{identityIDMetadataKey: []string{identityID}}
	identityType := metadataValue(md, identityTypeMetadataKey)
	if identityType != "" {
		outgoing[identityTypeMetadataKey] = []string{identityType}
	}
	organizationID := metadataValue(md, organizationIDMetadataKey)
	if organizationID != "" {
		outgoing[organizationIDMetadataKey] = []string{organizationID}
	}
	return metadata.NewOutgoingContext(ctx, outgoing), nil
}

func parseUUID(value string) (uuid.UUID, error) {
	if value == "" {
		return uuid.UUID{}, fmt.Errorf("value is empty")
	}
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.UUID{}, err
	}
	return id, nil
}

func toStatusError(err error) error {
	switch {
	case errors.Is(err, store.ErrThreadNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrThreadArchived):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrThreadDegraded):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrParticipantNotInThread):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Errorf(codes.Internal, "internal error: %v", err)
	}
}
