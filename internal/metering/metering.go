package metering

import (
	"context"
	"fmt"
	"time"

	meteringv1 "github.com/agynio/threads/.gen/go/agynio/api/metering/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	producerName    = "threads"
	countValue      = int64(1000000)
	labelResource   = "resource"
	labelResourceID = "resource_id"
	labelKind       = "kind"
	labelThreadID   = "thread_id"
	resourceThread  = "thread"
	resourceMessage = "message"
	kindThread      = "thread"
	kindMessage     = "message"
)

type Recorder struct {
	client meteringv1.MeteringServiceClient
}

func New(client meteringv1.MeteringServiceClient) *Recorder {
	return &Recorder{client: client}
}

func (r *Recorder) RecordThreadCreated(ctx context.Context, orgID, threadID uuid.UUID, createdAt time.Time) error {
	record := &meteringv1.UsageRecord{
		OrgId:          orgID.String(),
		IdempotencyKey: threadID.String(),
		Producer:       producerName,
		Timestamp:      timestamppb.New(createdAt),
		Labels: map[string]string{
			labelResourceID: threadID.String(),
			labelResource:   resourceThread,
			labelKind:       kindThread,
		},
		Unit:  meteringv1.Unit_UNIT_COUNT,
		Value: countValue,
	}
	return r.record(ctx, record)
}

func (r *Recorder) RecordMessageSent(ctx context.Context, orgID, threadID, messageID uuid.UUID, createdAt time.Time) error {
	record := &meteringv1.UsageRecord{
		OrgId:          orgID.String(),
		IdempotencyKey: messageID.String(),
		Producer:       producerName,
		Timestamp:      timestamppb.New(createdAt),
		Labels: map[string]string{
			labelResourceID: messageID.String(),
			labelResource:   resourceMessage,
			labelKind:       kindMessage,
			labelThreadID:   threadID.String(),
		},
		Unit:  meteringv1.Unit_UNIT_COUNT,
		Value: countValue,
	}
	return r.record(ctx, record)
}

func (r *Recorder) record(ctx context.Context, record *meteringv1.UsageRecord) error {
	if r == nil {
		return nil
	}
	_, err := r.client.Record(ctx, &meteringv1.RecordRequest{Records: []*meteringv1.UsageRecord{record}})
	if err != nil {
		return fmt.Errorf("record usage: %w", err)
	}
	return nil
}
