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
	if event.Action != "completed" && event.CheckRun.Status != "completed" {
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
	mu   sync.Mutex
	seen map[string]time.Time
	ttl  time.Duration
}

func NewDeliveryTracker(ttl time.Duration) *DeliveryTracker {
	if ttl <= 0 {
		ttl = 1 * time.Hour
	}
	return &DeliveryTracker{
		seen: make(map[string]time.Time),
		ttl:  ttl,
	}
}

func (dt *DeliveryTracker) CheckAndRecord(deliveryID string) bool {
	if deliveryID == "" {
		return false
	}
	dt.mu.Lock()
	defer dt.mu.Unlock()

	now := time.Now()
	for id, t := range dt.seen {
		if now.Sub(t) > dt.ttl {
			delete(dt.seen, id)
		}
	}
	if _, exists := dt.seen[deliveryID]; exists {
		return false
	}
	dt.seen[deliveryID] = now
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

	if a.Tracker != nil {
		if !a.Tracker.CheckAndRecord(deliveryID) {
			return nil, fmt.Errorf("delivery %s already processed (replay detected)", deliveryID)
		}
	}

	var event CheckRunEvent
	if err := json.Unmarshal(rawPayload, &event); err != nil {
		return nil, fmt.Errorf("unmarshal webhook payload: %w", err)
	}

	req, err := NormalizeCheckRun(&event, waitID, prOverride)
	if err != nil {
		return nil, err
	}

	return a.CompleteWait(ctx, *req)
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
			_ = json.NewDecoder(resp.Body).Decode(&errResp)
			resp.Body.Close()
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
		_ = json.NewDecoder(resp.Body).Decode(&okResp)
		resp.Body.Close()
		result.WaitID = okResp.WaitID
		result.Code = okResp.Status
		result.RunID = okResp.RunID
		result.NodeKey = okResp.NodeKey
		result.Message = okResp.Message
		return &result, nil
	}

	return nil, fmt.Errorf("complete wait failed after %d attempts: %w", maxAttempts, lastErr)
}
