package server

import (
	"context"
	"strings"
	"testing"

	agentsv1 "github.com/agynio/threads/.gen/go/agynio/api/agents/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func identityContext(identityID uuid.UUID, identityType string) context.Context {
	return metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs(identityIDMetadataKey, identityID.String(), identityTypeMetadataKey, identityType),
	)
}

func instanceWithDefaultThread(t *testing.T, instanceID uuid.UUID, defaultThreadID string) *stubAgentsService {
	t.Helper()
	instance := &agentsv1.AgentInstance{Meta: &agentsv1.EntityMeta{Id: instanceID.String()}}
	if defaultThreadID != "" {
		instance.DefaultThreadId = &defaultThreadID
	}
	return &stubAgentsService{
		t: t,
		getInstanceFn: func(_ context.Context, req *agentsv1.GetInstanceRequest, _ ...grpc.CallOption) (*agentsv1.GetInstanceResponse, error) {
			if req.GetId() != instanceID.String() {
				t.Fatalf("expected the caller's own instance %s, got %s", instanceID, req.GetId())
			}
			return &agentsv1.GetInstanceResponse{Instance: instance}, nil
		},
	}
}

// An instance leaves thread_id out and gets its default. Resolution is from the
// caller's own identity, so nothing the container carries can misroute it.
func TestResolveSendThreadUsesTheInstanceDefault(t *testing.T) {
	instanceID := uuid.New()
	defaultThreadID := uuid.New()
	srv := New(nil, nil, nil, nil, instanceWithDefaultThread(t, instanceID, defaultThreadID.String()), nil)

	got, err := srv.resolveSendThread(identityContext(instanceID, agentInstanceIdentityType), "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got != defaultThreadID {
		t.Fatalf("expected %s, got %s", defaultThreadID, got)
	}
}

// A named thread is used as given; the instance is never consulted. This is
// what lets B reach a sub-thread rather than always answering its origin.
func TestResolveSendThreadPrefersAnExplicitThread(t *testing.T) {
	instanceID := uuid.New()
	explicitThreadID := uuid.New()
	agents := &stubAgentsService{
		t: t,
		getInstanceFn: func(context.Context, *agentsv1.GetInstanceRequest, ...grpc.CallOption) (*agentsv1.GetInstanceResponse, error) {
			t.Fatal("expected no instance lookup when a thread is named")
			return nil, nil
		},
	}
	srv := New(nil, nil, nil, nil, agents, nil)

	got, err := srv.resolveSendThread(identityContext(instanceID, agentInstanceIdentityType), explicitThreadID.String())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got != explicitThreadID {
		t.Fatalf("expected %s, got %s", explicitThreadID, got)
	}
}

// Only an instance has a default to fall back to.
func TestResolveSendThreadRejectsOmissionByOtherCallers(t *testing.T) {
	for _, identityType := range []string{"user", "app", agentIdentityType} {
		t.Run(identityType, func(t *testing.T) {
			srv := New(nil, nil, nil, nil, &stubAgentsService{t: t}, nil)

			_, err := srv.resolveSendThread(identityContext(uuid.New(), identityType), "")
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %v", err)
			}
		})
	}
}

// "none" means the platform infers nothing, and an instance that was never
// given a thread has nowhere to send. Refused rather than guessed at.
func TestResolveSendThreadRejectsAnInstanceWithoutADefault(t *testing.T) {
	instanceID := uuid.New()
	srv := New(nil, nil, nil, nil, instanceWithDefaultThread(t, instanceID, ""), nil)

	_, err := srv.resolveSendThread(identityContext(instanceID, agentInstanceIdentityType), "")
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
	if !strings.Contains(status.Convert(err).Message(), "no default thread") {
		t.Fatalf("expected the reason to name the missing default, got %v", err)
	}
}

func TestResolveSendThreadRejectsAMalformedThread(t *testing.T) {
	srv := New(nil, nil, nil, nil, &stubAgentsService{t: t}, nil)

	_, err := srv.resolveSendThread(identityContext(uuid.New(), agentInstanceIdentityType), "not-a-uuid")
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}

// Agents checks that the caller can initiate the class, so the identity of
// whoever is adding the agent has to reach it. Called as Threads -- with no
// identity at all -- CreateInstance refuses and class-on-add fails outright.
func TestCreateAgentInstanceForwardsTheCallerIdentity(t *testing.T) {
	callerID := uuid.New()
	originThreadID := uuid.New()
	instanceID := uuid.New()
	var seen metadata.MD
	agents := &stubAgentsService{
		t: t,
		createFn: func(ctx context.Context, req *agentsv1.CreateInstanceRequest, _ ...grpc.CallOption) (*agentsv1.CreateInstanceResponse, error) {
			seen, _ = metadata.FromOutgoingContext(ctx)
			if got := req.GetContext().GetThreadId(); got != originThreadID.String() {
				t.Fatalf("expected the origin thread %s, got %q", originThreadID, got)
			}
			return &agentsv1.CreateInstanceResponse{Instance: &agentsv1.AgentInstance{
				Meta: &agentsv1.EntityMeta{Id: instanceID.String()},
			}}, nil
		},
	}
	srv := New(nil, nil, nil, nil, agents, nil)

	got, err := srv.createAgentInstance(identityContext(callerID, "user"), uuid.New(), originThreadID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got != instanceID {
		t.Fatalf("expected %s, got %s", instanceID, got)
	}
	if values := seen.Get(identityIDMetadataKey); len(values) != 1 || values[0] != callerID.String() {
		t.Fatalf("expected the caller identity to be forwarded, got %v", values)
	}
}
