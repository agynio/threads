package server

import (
	"fmt"

	threadsv1 "github.com/agynio/threads/.gen/go/agynio/api/threads/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/agynio/threads/internal/store"
)

func toProtoThread(thread store.Thread) *threadsv1.Thread {
	protoThread := &threadsv1.Thread{
		Id:           thread.ID.String(),
		Status:       toProtoThreadStatus(thread.Status),
		CreatedAt:    timestamppb.New(thread.CreatedAt),
		UpdatedAt:    timestamppb.New(thread.UpdatedAt),
		MessageCount: thread.MessageCount,
	}
	if thread.OrganizationID != nil {
		protoThread.OrganizationId = thread.OrganizationID.String()
	}
	if len(thread.Participants) > 0 {
		protoThread.Participants = make([]*threadsv1.Participant, len(thread.Participants))
		for i, participant := range thread.Participants {
			protoThread.Participants[i] = &threadsv1.Participant{
				Id:       participant.ID.String(),
				JoinedAt: timestamppb.New(participant.JoinedAt),
				Passive:  participant.Passive,
			}
		}
	}
	return protoThread
}

func toProtoMessage(message store.Message) *threadsv1.Message {
	fileIDs := make([]string, len(message.FileIDs))
	for i, id := range message.FileIDs {
		fileIDs[i] = id.String()
	}
	return &threadsv1.Message{
		Id:        message.ID.String(),
		ThreadId:  message.ThreadID.String(),
		SenderId:  message.SenderID.String(),
		Body:      message.Body,
		FileIds:   fileIDs,
		CreatedAt: timestamppb.New(message.CreatedAt),
	}
}

func toProtoThreadStatus(status store.ThreadStatus) threadsv1.ThreadStatus {
	switch status {
	case store.ThreadStatusActive:
		return threadsv1.ThreadStatus_THREAD_STATUS_ACTIVE
	case store.ThreadStatusArchived:
		return threadsv1.ThreadStatus_THREAD_STATUS_ARCHIVED
	case store.ThreadStatusDegraded:
		return threadsv1.ThreadStatus_THREAD_STATUS_DEGRADED
	default:
		panic(fmt.Sprintf("unsupported thread status: %d", status))
	}
}
