package server

import (
	"context"
	"errors"
	"fmt"

	threadsv1 "github.com/agynio/threads/gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agynio/threads/internal/notifier"
	"github.com/agynio/threads/internal/store"
)

type Server struct {
	threadsv1.UnimplementedThreadsServiceServer
	store    *store.Store
	notifier *notifier.Notifier
}

func New(store *store.Store, notifier *notifier.Notifier) *Server {
	return &Server{store: store, notifier: notifier}
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
	participantID, err := parseUUID(req.GetParticipantId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "participant_id: %v", err)
	}
	thread, err := s.store.AddParticipant(ctx, threadID, participantID)
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
		tokenID, tokenCursor, err := store.DecodeMessagePageToken(token)
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
		token, err := store.EncodeMessagePageToken(threadID, *result.NextCursor)
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
	var cursor *store.MessageCursor
	if token := req.GetPageToken(); token != "" {
		tokenID, tokenCursor, err := store.DecodeMessagePageToken(token)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid page_token: %v", err)
		}
		if tokenID != participantID {
			return nil, status.Error(codes.InvalidArgument, "page_token does not match participant")
		}
		cursor = &tokenCursor
	}

	result, err := s.store.ListUnackedMessages(ctx, participantID, req.GetPageSize(), cursor)
	if err != nil {
		return nil, toStatusError(err)
	}
	resp := &threadsv1.GetUnackedMessagesResponse{Messages: make([]*threadsv1.Message, len(result.Messages))}
	for i, message := range result.Messages {
		resp.Messages[i] = toProtoMessage(message)
	}
	if result.NextCursor != nil {
		token, err := store.EncodeMessagePageToken(participantID, *result.NextCursor)
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
