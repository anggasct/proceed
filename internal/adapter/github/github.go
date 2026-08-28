package github

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"proceed/internal/controller"
)

type CheckRunPayload struct {
	ID           int64            `json:"id"`
	Name         string           `json:"name"`
	HeadSHA      string           `json:"head_sha"`
	Status       string           `json:"status"`
	Conclusion   string           `json:"conclusion"`
	CompletedAt  string           `json:"completed_at,omitempty"`
	PullRequests []PullRequestRef `json:"pull_requests,omitempty"`
}

type PullRequestRef struct {
	Number int64 `json:"number"`
}

type RepoRef struct {
	FullName string `json:"full_name"`
}

type CheckRunEvent struct {
	Action     string          `json:"action"`
	CheckRun   CheckRunPayload `json:"check_run"`
	Repository RepoRef         `json:"repository"`
}

func VerifyWebhookSignature(secret, signatureHeader string, payload []byte) bool {
	if secret == "" || signatureHeader == "" {
		return false
	}
	sigHex := strings.TrimPrefix(signatureHeader, "sha256=")
	sigBytes, err := hex.DecodeString(sigHex)
	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	expected := mac.Sum(nil)
	return hmac.Equal(sigBytes, expected)
}

func CorrelationKey(repo string, pr int64, headSHA string) string {
	return fmt.Sprintf("repo=%s;pr=%d;head=%s", repo, pr, headSHA)
}

func NormalizeCheckRun(event *CheckRunEvent, waitID string, prOverride int64) (*controller.CompleteWaitRequest, error) {
	if event == nil {
		return nil, fmt.Errorf("check run event is nil")
	}
	if event.Action != "" && event.Action != "completed" {
		return nil, fmt.Errorf("check run action %q is not completed", event.Action)
	}
	if event.CheckRun.Status != "completed" {
		return nil, fmt.Errorf("check run is not completed (action=%s, status=%s)", event.Action, event.CheckRun.Status)
	}
	if event.Repository.FullName == "" {
		return nil, fmt.Errorf("repository full_name is required")
	}
	if event.CheckRun.HeadSHA == "" {
		return nil, fmt.Errorf("check run head_sha is required")
	}

	pr := prOverride
	if pr == 0 && len(event.CheckRun.PullRequests) > 0 {
		pr = event.CheckRun.PullRequests[0].Number
	}
	if pr == 0 {
		return nil, fmt.Errorf("pull request number is required")
	}

	status := "failure"
	if event.CheckRun.Conclusion == "success" {
		status = "success"
	}

	normPayload := map[string]any{
		"repository":   event.Repository.FullName,
		"pull_request": pr,
		"head_sha":     event.CheckRun.HeadSHA,
		"check_name":   event.CheckRun.Name,
		"conclusion":   event.CheckRun.Conclusion,
	}
	payloadBytes, err := json.Marshal(normPayload)
	if err != nil {
		return nil, fmt.Errorf("marshal normalized payload: %w", err)
	}

	sum := sha256.Sum256(payloadBytes)
	digest := hex.EncodeToString(sum[:])

	occurredAt := time.Now().UnixMilli()
	if event.CheckRun.CompletedAt != "" {
		if t, err := time.Parse(time.RFC3339, event.CheckRun.CompletedAt); err == nil {
			occurredAt = t.UnixMilli()
		}
	}

	return &controller.CompleteWaitRequest{
		WaitID:          waitID,
		ProviderEventID: fmt.Sprintf("github:check_run:%d", event.CheckRun.ID),
		EventType:       "ci.completed",
		Source:          "github",
		CorrelationKey:  CorrelationKey(event.Repository.FullName, pr, event.CheckRun.HeadSHA),
		OccurredAt:      occurredAt,
		Status:          status,
		PayloadDigest:   "sha256:" + digest,
		Payload:         json.RawMessage(payloadBytes),
	}, nil
}

type DeliveryTracker struct {
	mu        sync.Mutex
	completed map[string]time.Time
	inFlight  map[string]time.Time
	ttl       time.Duration
}

func NewDeliveryTracker(ttl time.Duration) *DeliveryTracker {
	if ttl <= 0 {
		ttl = 1 * time.Hour
	}
	return &DeliveryTracker{
		completed: make(map[string]time.Time),
		inFlight:  make(map[string]time.Time),
		ttl:       ttl,
	}
}

func (dt *DeliveryTracker) Begin(deliveryID string) (func(bool), bool) {
	if deliveryID == "" {
		return nil, false
	}
	dt.mu.Lock()
	defer dt.mu.Unlock()

	now := time.Now()
	for id, t := range dt.completed {
		if now.Sub(t) > dt.ttl {
			delete(dt.completed, id)
		}
	}
	for id, t := range dt.inFlight {
		if now.Sub(t) > dt.ttl {
			delete(dt.inFlight, id)
		}
	}
	if _, exists := dt.completed[deliveryID]; exists {
		return nil, false
	}
	if _, exists := dt.inFlight[deliveryID]; exists {
		return nil, false
	}
	dt.inFlight[deliveryID] = now

	done := func(completed bool) {
		dt.mu.Lock()
		defer dt.mu.Unlock()
		delete(dt.inFlight, deliveryID)
		if completed {
			dt.completed[deliveryID] = time.Now()
		}
	}
	return done, true
}

func (dt *DeliveryTracker) CheckAndRecord(deliveryID string) bool {
	done, ok := dt.Begin(deliveryID)
	if !ok {
		return false
	}
	done(true)
	return true
}

type Adapter struct {
	BaseURL     string
	Token       string
	HTTPClient  *http.Client
	Tracker     *DeliveryTracker
	MaxRetries  int
	BackoffBase time.Duration
}

func NewAdapter(baseURL, token string) *Adapter {
	return &Adapter{
		BaseURL:     strings.TrimSuffix(baseURL, "/"),
		Token:       token,
		HTTPClient:  &http.Client{Timeout: 10 * time.Second},
		Tracker:     NewDeliveryTracker(1 * time.Hour),
		MaxRetries:  3,
		BackoffBase: 20 * time.Millisecond,
	}
}

func (a *Adapter) ProcessVerifiedWebhook(ctx context.Context, secret, signatureHeader, deliveryID string, rawPayload []byte, waitID string, prOverride int64) (*controller.CompletionResult, error) {
	if !VerifyWebhookSignature(secret, signatureHeader, rawPayload) {
		return nil, fmt.Errorf("invalid or missing webhook signature")
	}

	var done func(bool)
	if a.Tracker != nil {
		var ok bool
		done, ok = a.Tracker.Begin(deliveryID)
		if !ok {
			return nil, fmt.Errorf("delivery %s already processed (replay detected)", deliveryID)
		}
	}

	completed := false
	defer func() {
		if done != nil {
			done(completed)
		}
	}()

	var event CheckRunEvent
	if err := json.Unmarshal(rawPayload, &event); err != nil {
		return nil, fmt.Errorf("unmarshal webhook payload: %w", err)
	}

	req, err := NormalizeCheckRun(&event, waitID, prOverride)
	if err != nil {
		return nil, err
	}

	res, err := a.CompleteWait(ctx, *req)
	if err != nil {
		return nil, err
	}

	completed = res.Accepted()
	return res, nil
}

// Only these HTTP/status pairings are recognized completion outcomes. Unknown
// or mismatched pairings surface as errors so the delivery stays retryable
// instead of being silently treated as handled.
func validCompletionOutcome(httpStatus int, code string) bool {
	switch {
	case httpStatus == http.StatusAccepted && code == "WAIT_COMPLETED":
		return true
	case httpStatus == http.StatusOK && code == "WAIT_ALREADY_COMPLETED":
		return true
	case httpStatus == http.StatusAccepted && code == "WAIT_REJECTED":
		return true
	}
	return false
}

func validErrorOutcome(httpStatus int, code string) bool {
	switch {
	case httpStatus == http.StatusBadRequest && code == "GRAPH_INVALID":
		return true
	case httpStatus == http.StatusUnauthorized && code == "UNAUTHORIZED":
		return true
	case httpStatus == http.StatusForbidden && code == "POLICY_DENIED":
		return true
	case httpStatus == http.StatusNotFound && (code == "WAIT_NOT_FOUND" || code == "RUN_NOT_FOUND"):
		return true
	case httpStatus == http.StatusConflict && (code == "WAIT_CONFLICT" || code == "STORE_CONFLICT"):
		return true
	}
	return false
}

func (a *Adapter) CompleteWait(ctx context.Context, req controller.CompleteWaitRequest) (*controller.CompletionResult, error) {
	url := fmt.Sprintf("%s/v1/waits/%s/complete", a.BaseURL, req.WaitID)
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	maxAttempts := a.MaxRetries
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	backoff := a.BackoffBase
	if backoff <= 0 {
		backoff = 20 * time.Millisecond
	}

	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff * (1 << (attempt - 1))):
			}
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if a.Token != "" {
			httpReq.Header.Set("Authorization", "Bearer "+a.Token)
		}

		resp, err := a.HTTPClient.Do(httpReq)
		if err != nil {
			lastErr = err
			continue
		}

		// Transient 5xx server status codes -> retry with same provider_event_id
		if resp.StatusCode >= 500 && resp.StatusCode <= 504 {
			resp.Body.Close()
			lastErr = fmt.Errorf("server returned transient status %d", resp.StatusCode)
			continue
		}

		var result controller.CompletionResult
		result.HTTPStatus = resp.StatusCode

		if resp.StatusCode >= 400 {
			var errResp struct {
				Error struct {
					Code    string         `json:"code"`
					Message string         `json:"message"`
					Details map[string]any `json:"details"`
				} `json:"error"`
			}
			decodeErr := json.NewDecoder(resp.Body).Decode(&errResp)
			resp.Body.Close()
			if decodeErr != nil {
				return nil, fmt.Errorf("complete wait: status %d with undecodable error body: %w", resp.StatusCode, decodeErr)
			}
			if !validErrorOutcome(resp.StatusCode, errResp.Error.Code) {
				return nil, fmt.Errorf("complete wait: unrecognized error outcome %q with status %d", errResp.Error.Code, resp.StatusCode)
			}
			result.Code = errResp.Error.Code
			result.Message = errResp.Error.Message
			result.Details = errResp.Error.Details
			return &result, nil
		}

		var okResp struct {
			WaitID  string `json:"wait_id"`
			Status  string `json:"status"`
			RunID   string `json:"run_id"`
			NodeKey string `json:"node_key"`
			Message string `json:"message"`
		}
		decodeErr := json.NewDecoder(resp.Body).Decode(&okResp)
		resp.Body.Close()
		if decodeErr != nil {
			return nil, fmt.Errorf("complete wait: status %d with undecodable success body: %w", resp.StatusCode, decodeErr)
		}
		if !validCompletionOutcome(resp.StatusCode, okResp.Status) {
			return nil, fmt.Errorf("complete wait: unrecognized completion outcome %q with status %d", okResp.Status, resp.StatusCode)
		}
		result.WaitID = okResp.WaitID
		result.Code = okResp.Status
		result.RunID = okResp.RunID
		result.NodeKey = okResp.NodeKey
		result.Message = okResp.Message
		return &result, nil
	}

	return nil, fmt.Errorf("complete wait failed after %d attempts: %w", maxAttempts, lastErr)
}
