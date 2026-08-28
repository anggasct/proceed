package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
)

func TestExternalWaitProjectionAndRebuild(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	src := readFixture(t, "../../internal/compiler/testdata/customer-research.yaml")
	doc := compileFixture(t, src)
	frozen, err := s.FreezeDefinition(ctx, "customer-research.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.CreateRun(ctx, frozen.GraphVersionID)
	if err != nil {
		t.Fatal(err)
	}
	runID := run.ID

	now := time.Now().UnixMilli() - 10000
	waitID := ulid.Make().String()
	eventType := "ci.completed"
	corrKey := "repo=proceed/app;pr=42;head=sha256:abc1234"
	expectedCond := `{"status":"success"}`

	// 1. Append external_wait_requested
	reqPayload, _ := json.Marshal(externalWaitRequestedPayload{
		WaitID:            waitID,
		NodeKey:           "test",
		EventType:         eventType,
		CorrelationKey:    corrKey,
		ExpectedCondition: json.RawMessage(expectedCond),
		ExpiresAt:         now + 60000,
	})
	ev1, err := s.Append(ctx, Event{
		EventID:       ulid.Make().String(),
		RunID:         runID,
		Sequence:      2,
		SchemaVersion: "proceed/v1",
		Type:          "external_wait_requested",
		OccurredAt:    now,
		ActorType:     "controller",
		ActorID:       "controller",
		Payload:       string(reqPayload),
	})
	if err != nil {
		t.Fatalf("append external_wait_requested: %v", err)
	}

	// Verify projection in external_wait table
	w, err := s.GetExternalWait(ctx, waitID)
	if err != nil || w == nil {
		t.Fatalf("GetExternalWait failed: %v, wait: %+v", err, w)
	}
	if w.Status != "pending" {
		t.Errorf("status = %q, want pending", w.Status)
	}
	if w.EventType != eventType || w.CorrelationKey != corrKey {
		t.Errorf("eventType=%q, corrKey=%q, want %q, %q", w.EventType, w.CorrelationKey, eventType, corrKey)
	}
	if !w.ExpiresAt.Valid || w.ExpiresAt.Int64 != now+60000 {
		t.Errorf("expires_at = %v, want %d", w.ExpiresAt, now+60000)
	}

	// Verify FindPendingExternalWait
	found, err := s.FindPendingExternalWait(ctx, eventType, corrKey)
	if err != nil || found == nil {
		t.Fatalf("FindPendingExternalWait failed: %v", err)
	}
	if found.ID != waitID {
		t.Errorf("found.ID = %q, want %q", found.ID, waitID)
	}

	// 2. Append external_event_received
	receivedEventID := ulid.Make().String()
	providerEventID := "github:check_run:9988"
	eventPayload := `{"conclusion":"success","run_id":9988}`
	recvPayload, _ := json.Marshal(externalEventReceivedPayload{
		WaitID:          waitID,
		ProviderEventID: providerEventID,
		EventType:       eventType,
		Source:          "github",
		CorrelationKey:  corrKey,
		OccurredAt:      now + 1000,
		Status:          "success",
		PayloadDigest:   payloadDigest(eventPayload),
		Payload:         json.RawMessage(eventPayload),
	})
	_, err = s.Append(ctx, Event{
		EventID:        receivedEventID,
		RunID:          runID,
		Sequence:       3,
		SchemaVersion:  "proceed/v1",
		Type:           "external_event_received",
		OccurredAt:     now + 1000,
		ActorType:      "executor",
		ActorID:        "github",
		IdempotencyKey: providerEventID,
		Payload:        string(recvPayload),
	})
	if err != nil {
		t.Fatalf("append external_event_received: %v", err)
	}

	// 3. Append external_wait_completed
	completedPayload, _ := json.Marshal(externalWaitCompletedPayload{
		WaitID:          waitID,
		NodeKey:         "test",
		ReceivedEventID: receivedEventID,
		Status:          "completed",
		PayloadDigest:   payloadDigest(eventPayload),
	})
	_, err = s.Append(ctx, Event{
		EventID:       ulid.Make().String(),
		RunID:         runID,
		Sequence:      4,
		SchemaVersion: "proceed/v1",
		Type:          "external_wait_completed",
		OccurredAt:    now + 1100,
		ActorType:     "controller",
		ActorID:       "controller",
		CausationID:   receivedEventID,
		Payload:       string(completedPayload),
	})
	if err != nil {
		t.Fatalf("append external_wait_completed: %v", err)
	}

	// Verify wait projection is now completed
	w, err = s.GetExternalWait(ctx, waitID)
	if err != nil || w == nil {
		t.Fatalf("GetExternalWait failed: %v", err)
	}
	if w.Status != "completed" {
		t.Errorf("status = %q, want completed", w.Status)
	}
	if !w.CompletedEventID.Valid || w.CompletedEventID.String != receivedEventID {
		t.Errorf("completed_event_id = %v, want %q", w.CompletedEventID, receivedEventID)
	}
	if !w.CompletedAt.Valid || w.CompletedAt.Int64 != now+1100 {
		t.Errorf("completed_at = %v, want %d", w.CompletedAt, now+1100)
	}

	// Pending lookup should now return nil because status is completed
	pending, err := s.FindPendingExternalWait(ctx, eventType, corrKey)
	if err != nil {
		t.Fatalf("FindPendingExternalWait: %v", err)
	}
	if pending != nil {
		t.Errorf("FindPendingExternalWait returned %v, want nil", pending)
	}

	// 4. Test RebuildProjections preserves external_wait exactly
	digestBefore, err := s.ProjectionDigest(ctx)
	if err != nil {
		t.Fatalf("ProjectionDigest: %v", err)
	}
	report, err := s.RebuildProjections(ctx)
	if err != nil {
		t.Fatalf("RebuildProjections: %v", err)
	}
	if report.Diverged {
		t.Fatalf("projections diverged after rebuild: before=%s after=%s", report.Before, report.After)
	}
	if report.After != digestBefore {
		t.Fatalf("digest changed after rebuild: got %s, want %s", report.After, digestBefore)
	}

	// Verify rebuilt wait row
	wRebuilt, err := s.GetExternalWait(ctx, waitID)
	if err != nil || wRebuilt == nil {
		t.Fatalf("GetExternalWait after rebuild: %v", err)
	}
	if wRebuilt.Status != "completed" || wRebuilt.CompletedEventID.String != receivedEventID {
		t.Errorf("rebuilt wait = %+v, mismatch", wRebuilt)
	}

	_ = ev1
}

func TestExternalWaitUniquePendingConstraint(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	runID := seededRun(t, s)

	now := time.Now().UnixMilli() - 10000
	eventType := "ci.completed"
	corrKey := "repo=proceed/app;pr=1;head=sha256:111"

	reqPayload1, _ := json.Marshal(externalWaitRequestedPayload{
		WaitID:            ulid.Make().String(),
		NodeKey:           "test",
		EventType:         eventType,
		CorrelationKey:    corrKey,
		ExpectedCondition: json.RawMessage(`{}`),
	})
	_, err := s.Append(ctx, Event{
		EventID:       ulid.Make().String(),
		RunID:         runID,
		Sequence:      2,
		SchemaVersion: "proceed/v1",
		Type:          "external_wait_requested",
		OccurredAt:    now,
		ActorType:     "controller",
		ActorID:       "controller",
		Payload:       string(reqPayload1),
	})
	if err != nil {
		t.Fatalf("first wait append: %v", err)
	}

	// Second wait with identical pending eventType and correlationKey must fail unique index
	reqPayload2, _ := json.Marshal(externalWaitRequestedPayload{
		WaitID:            ulid.Make().String(),
		NodeKey:           "test",
		EventType:         eventType,
		CorrelationKey:    corrKey,
		ExpectedCondition: json.RawMessage(`{}`),
	})
	_, err = s.Append(ctx, Event{
		EventID:       ulid.Make().String(),
		RunID:         runID,
		Sequence:      3,
		SchemaVersion: "proceed/v1",
		Type:          "external_wait_requested",
		OccurredAt:    now + 1,
		ActorType:     "controller",
		ActorID:       "controller",
		Payload:       string(reqPayload2),
	})
	if err == nil {
		t.Fatalf("second pending wait on same event_type and correlation_key should fail UNIQUE constraint")
	}
}

func TestExternalWaitExpiryAndCancellation(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t)
	runID := seededRun(t, s)
	now := time.Now().UnixMilli() - 10000

	waitID1 := ulid.Make().String()
	waitID2 := ulid.Make().String()

	// Wait 1 to be expired
	req1, _ := json.Marshal(externalWaitRequestedPayload{
		WaitID:            waitID1,
		NodeKey:           "test",
		EventType:         "deploy.completed",
		CorrelationKey:    "env=staging",
		ExpectedCondition: json.RawMessage(`{}`),
		ExpiresAt:         now - 1000,
	})
	_, err := s.Append(ctx, Event{
		EventID:       ulid.Make().String(),
		RunID:         runID,
		Sequence:      2,
		SchemaVersion: "proceed/v1",
		Type:          "external_wait_requested",
		OccurredAt:    now,
		ActorType:     "controller",
		ActorID:       "controller",
		Payload:       string(req1),
	})
	if err != nil {
		t.Fatalf("append wait 1: %v", err)
	}

	// Wait 2 to be cancelled
	req2, _ := json.Marshal(externalWaitRequestedPayload{
		WaitID:            waitID2,
		NodeKey:           "test",
		EventType:         "deploy.completed",
		CorrelationKey:    "env=prod",
		ExpectedCondition: json.RawMessage(`{}`),
		ExpiresAt:         now + 100000,
	})
	_, err = s.Append(ctx, Event{
		EventID:       ulid.Make().String(),
		RunID:         runID,
		Sequence:      3,
		SchemaVersion: "proceed/v1",
		Type:          "external_wait_requested",
		OccurredAt:    now,
		ActorType:     "controller",
		ActorID:       "controller",
		Payload:       string(req2),
	})
	if err != nil {
		t.Fatalf("append wait 2: %v", err)
	}

	// List expired waits
	expired, err := s.ListExpiredExternalWaits(ctx, now)
	if err != nil {
		t.Fatalf("ListExpiredExternalWaits: %v", err)
	}
	if len(expired) != 1 || expired[0].ID != waitID1 {
		t.Errorf("ListExpiredExternalWaits = %+v, want waitID1", expired)
	}

	// Expire wait 1
	expPayload, _ := json.Marshal(externalWaitExpiredPayload{WaitID: waitID1, NodeKey: "test"})
	_, err = s.Append(ctx, Event{
		EventID:       ulid.Make().String(),
		RunID:         runID,
		Sequence:      4,
		SchemaVersion: "proceed/v1",
		Type:          "external_wait_expired",
		OccurredAt:    now + 50,
		ActorType:     "controller",
		ActorID:       "controller",
		Payload:       string(expPayload),
	})
	if err != nil {
		t.Fatalf("append external_wait_expired: %v", err)
	}

	w1, _ := s.GetExternalWait(ctx, waitID1)
	if w1.Status != "expired" {
		t.Errorf("w1.Status = %q, want expired", w1.Status)
	}

	// Cancel wait 2
	cancelPayload, _ := json.Marshal(externalWaitCancelledPayload{WaitID: waitID2, NodeKey: "test"})
	_, err = s.Append(ctx, Event{
		EventID:       ulid.Make().String(),
		RunID:         runID,
		Sequence:      5,
		SchemaVersion: "proceed/v1",
		Type:          "external_wait_cancelled",
		OccurredAt:    now + 60,
		ActorType:     "controller",
		ActorID:       "controller",
		Payload:       string(cancelPayload),
	})
	if err != nil {
		t.Fatalf("append external_wait_cancelled: %v", err)
	}

	w2, _ := s.GetExternalWait(ctx, waitID2)
	if w2.Status != "cancelled" {
		t.Errorf("w2.Status = %q, want cancelled", w2.Status)
	}
}
