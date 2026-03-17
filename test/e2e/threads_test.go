//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	threadsv1 "github.com/agynio/threads/.gen/go/agynio/api/threads/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestThreadsRejectsMissingParticipant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, threadsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial threads: %v", err)
	}
	defer conn.Close()

	client := threadsv1.NewThreadsServiceClient(conn)

	_, err = client.GetThreads(ctx, &threadsv1.GetThreadsRequest{})
	if err == nil {
		t.Fatal("expected invalid argument error")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %s: %s", st.Code(), st.Message())
	}
}
