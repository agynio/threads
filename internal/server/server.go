package server

import (
	"context"
	"errors"
	"fmt"

	identityv1 "github.com/agynio/threads/.gen/go/agynio/api/identity/v1"
	threadsv1 "github.com/agynio/threads/.gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agynio/threads/internal/store"
)

type Server struct {
	threadsv1.UnimplementedThreadsServiceServer
	store    threadStore
	notifier notifierPublisher
	identity identityResolver
}

type threadStore interface {
	CreateThread(ctx context.Context, participantIDs []uuid.UUID) (store.Thread, error)
	ArchiveThread(ctx context.Context, threadID uuid.UUID) (store.Thread, error)
	AddParticipant(ctx context.Context, threadID, participantID uuid.UUID, passive bool) (store.Thread, error)
	SendMessage(ctx context.Context, threadID, senderID uuid.UUID, body string, fileIDs []uuid.UUID) (store.SendMessageResult, error)
	ListThreads(ctx context.Context, participantID uuid.UUID, pageSize int32, cursor *store.ThreadCursor) (store.ThreadListResult, error)
	ListMessages(ctx context.Context, threadID uuid.UUID, pageSize int32, cursor *store.MessageCursor) (store.MessageListResult, error)
	ListUnackedMessages(ctx context.Context, participantID uuid.UUID, threadID *uuid.UUID, pageSize int32, cursor *store.MessageCursor) (store.MessageListResult, error)
	AckMessages(ctx context.Context, participantID uuid.UUID, messageIDs []uuid.UUID) (int32, error)
}

type identityResolver interface {
	ResolveNickname(ctx context.Context, in *identityv1.ResolveNicknameRequest, opts ...grpc.CallOption) (*identityv1.ResolveNicknameResponse, error)
}

type notifierPublisher interface {
	PublishMessageCreated(ctx context.Context, threadID, messageID uuid.UUID, recipients []uuid.UUID) error
}

func New(store threadStore, notifier notifierPublisher, identity identityResolver) *Server {
	return &Server{store: store, notifier: notifier, identity: identity}
}

func (s *Server) CreateThread(ctx context.Context, req *threadsv1.CreateThreadRequest) (*threadsv1.CreateThreadResponse, error) {
	ids := req.GetParticipantIds()
	if len(ids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "participant_ids must be provided")
	}
	participantIDs := make([]uuid.UUID, len(ids))
	seen := make(map[uuid.UUID]struct{}, len(ids))
	for i, raw := range ids {
		id, err := parseUUID(raw)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "participant_ids[%d]: %v", i, err)
		}
		if _, ok := seen[id]; ok {
			return nil, status.Errorf(codes.InvalidArgument, "participant_ids[%d]: duplicate participant", i)
		}
		seen[id] = struct{}{}
		participantIDs[i] = id
	}

	thread, err := s.store.CreateThread(ctx, participantIDs)
	if err != nil {
		return nil, toStatusError(err)
	}
	return &threadsv1.CreateThreadResponse{Thread: toProtoThread(thread)}, nil
}

func (s *Server) ArchiveThread(ctx context.Context, req *threadsv1.ArchiveThreadRequest) (*threadsv1.ArchiveThreadResponse, error) {
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	thread, err := s.store.ArchiveThread(ctx, threadID)
	if err != nil {
		return nil, toStatusError(err)
	}
	return &threadsv1.ArchiveThreadResponse{Thread: toProtoThread(thread)}, nil
}

func (s *Server) AddParticipant(ctx context.Context, req *threadsv1.AddParticipantRequest) (*threadsv1.AddParticipantResponse, error) {
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	participantID, err := s.resolveParticipantID(ctx, req)
	if err != nil {
		return nil, err
	}
	thread, err := s.store.AddParticipant(ctx, threadID, participantID, req.GetPassive())
	if err != nil {
		return nil, toStatusError(err)
	}
	return &threadsv1.AddParticipantResponse{Thread: toProtoThread(thread)}, nil
}

func (s *Server) SendMessage(ctx context.Context, req *threadsv1.SendMessageRequest) (*threadsv1.SendMessageResponse, error) {
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
	}
	senderID, err := parseUUID(req.GetSenderId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "sender_id: %v", err)
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

	result, err := s.store.SendMessage(ctx, threadID, senderID, req.GetBody(), fileIDs)
	if err != nil {
		return nil, toStatusError(err)
	}
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

func (s *Server) GetMessages(ctx context.Context, req *threadsv1.GetMessagesRequest) (*threadsv1.GetMessagesResponse, error) {
	threadID, err := parseUUID(req.GetThreadId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "thread_id: %v", err)
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

	result, err := s.store.ListMessages(ctx, threadID, req.GetPageSize(), cursor)
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

func (s *Server) AckMessages(ctx context.Context, req *threadsv1.AckMessagesRequest) (*threadsv1.AckMessagesResponse, error) {
	participantID, err := parseUUID(req.GetParticipantId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "participant_id: %v", err)
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

func (s *Server) resolveParticipantID(ctx context.Context, req *threadsv1.AddParticipantRequest) (uuid.UUID, error) {
	if req.GetParticipant() == nil {
		participantID, err := parseUUID(req.GetParticipantId())
		if err != nil {
			return uuid.UUID{}, status.Errorf(codes.InvalidArgument, "participant_id: %v", err)
		}
		return participantID, nil
	}
	identifier := req.GetParticipant().GetIdentifier()
	switch value := identifier.(type) {
	case *threadsv1.ParticipantIdentifier_ParticipantId:
		participantID, err := parseUUID(value.ParticipantId)
		if err != nil {
			return uuid.UUID{}, status.Errorf(codes.InvalidArgument, "participant.participant_id: %v", err)
		}
		return participantID, nil
	case *threadsv1.ParticipantIdentifier_ParticipantNickname:
		if req.OrganizationId == nil {
			return uuid.UUID{}, status.Error(codes.InvalidArgument, "organization_id must be provided for participant_nickname")
		}
		if value.ParticipantNickname == "" {
			return uuid.UUID{}, status.Error(codes.InvalidArgument, "participant.participant_nickname must be provided")
		}
		organizationID, err := parseUUID(req.GetOrganizationId())
		if err != nil {
			return uuid.UUID{}, status.Errorf(codes.InvalidArgument, "organization_id: %v", err)
		}
		response, err := s.identity.ResolveNickname(ctx, &identityv1.ResolveNicknameRequest{
			OrganizationId: organizationID.String(),
			Nickname:       value.ParticipantNickname,
		})
		if err != nil {
			return uuid.UUID{}, status.Errorf(codes.Internal, "resolve nickname: %v", err)
		}
		participantID, err := parseUUID(response.GetIdentityId())
		if err != nil {
			return uuid.UUID{}, status.Errorf(codes.Internal, "resolve nickname identity_id: %v", err)
		}
		return participantID, nil
	default:
		return uuid.UUID{}, status.Error(codes.InvalidArgument, "participant identifier must be provided")
	}
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
	case errors.Is(err, store.ErrParticipantNotInThread):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Errorf(codes.Internal, "internal error: %v", err)
	}
}
