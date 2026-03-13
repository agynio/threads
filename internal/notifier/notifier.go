package notifier

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"

	notificationsv1 "github.com/agynio/threads/gen/go/agynio/api/notifications/v1"
)

const (
	messageCreatedEvent = "message.created"
	messageSource       = "threads"
)

type Notifier struct {
	client notificationsv1.NotificationsServiceClient
}

func New(client notificationsv1.NotificationsServiceClient) *Notifier {
	return &Notifier{client: client}
}

func (n *Notifier) PublishMessageCreated(ctx context.Context, threadID, messageID uuid.UUID, recipients []uuid.UUID) error {
	if len(recipients) == 0 {
		return nil
	}
	rooms := make([]string, len(recipients))
	for i, recipient := range recipients {
		rooms[i] = fmt.Sprintf("thread_participant:%s", recipient)
	}
	payload, err := structpb.NewStruct(map[string]any{
		"thread_id":  threadID.String(),
		"message_id": messageID.String(),
	})
	if err != nil {
		return fmt.Errorf("build payload: %w", err)
	}
	_, err = n.client.Publish(ctx, &notificationsv1.PublishRequest{
		Event:   messageCreatedEvent,
		Rooms:   rooms,
		Payload: payload,
		Source:  messageSource,
	})
	if err != nil {
		return fmt.Errorf("publish notification: %w", err)
	}
	return nil
}
