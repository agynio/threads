package store

import (
	"strings"
	"testing"
)

func TestClaimPendingAgentInboxDeliveriesQueryAtomicallyClaimsRows(t *testing.T) {
	if !strings.Contains(claimPendingAgentInboxDeliveriesSQL, "FOR UPDATE SKIP LOCKED") {
		t.Fatalf("expected claim query to lock pending rows, got %s", claimPendingAgentInboxDeliveriesSQL)
	}
	if !strings.Contains(claimPendingAgentInboxDeliveriesSQL, "UPDATE agent_inbox_deliveries") {
		t.Fatalf("expected claim query to update claimed rows, got %s", claimPendingAgentInboxDeliveriesSQL)
	}
	if !strings.Contains(claimPendingAgentInboxDeliveriesSQL, "SET claimed_at = NOW(), claim_id = $3") {
		t.Fatalf("expected claim query to assign claim fields, got %s", claimPendingAgentInboxDeliveriesSQL)
	}
	if !strings.Contains(claimPendingAgentInboxDeliveriesSQL, "claimed_at IS NULL OR claimed_at < $2") {
		t.Fatalf("expected claim query to reclaim stale rows, got %s", claimPendingAgentInboxDeliveriesSQL)
	}
	if !strings.Contains(claimPendingAgentInboxDeliveriesSQL, "next_attempt_at IS NULL OR next_attempt_at <= NOW()") {
		t.Fatalf("expected claim query to skip deferred retries, got %s", claimPendingAgentInboxDeliveriesSQL)
	}
}

func TestAgentInboxDeliveryMarkQueriesRequireClaimOwnership(t *testing.T) {
	for name, query := range map[string]string{
		"delivered": markAgentInboxDeliveryDeliveredSQL,
		"failed":    markAgentInboxDeliveryFailedSQL,
	} {
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(query, "claim_id = $3") {
				t.Fatalf("expected %s query to require claim ownership, got %s", name, query)
			}
			if !strings.Contains(query, "delivered_at IS NULL") {
				t.Fatalf("expected %s query to update only pending rows, got %s", name, query)
			}
		})
	}
}

func TestAgentInboxDeliveryFailureDefersRetry(t *testing.T) {
	if !strings.Contains(markAgentInboxDeliveryFailedSQL, "next_attempt_at = $5") {
		t.Fatalf("expected failed query to defer retry, got %s", markAgentInboxDeliveryFailedSQL)
	}
}
