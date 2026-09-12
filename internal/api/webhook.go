package api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"proceed/internal/controller"
	"proceed/internal/store"
)

const (
	maxWebhookBody  = 256 << 10
	timestampWindow = 300 * time.Second
	triggerRate     = 60
)

type triggerLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	now     func() time.Time
}

type tokenBucket struct {
	tokens float64
	last   time.Time
}

func newTriggerLimiter(now func() time.Time) *triggerLimiter {
	return &triggerLimiter{buckets: map[string]*tokenBucket{}, now: now}
}

func (l *triggerLimiter) allow(name string) (ok bool, retryAfter time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b, exists := l.buckets[name]
	if !exists {
		b = &tokenBucket{tokens: triggerRate, last: now}
		l.buckets[name] = b
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed > 0 {
		b.tokens += elapsed
		if b.tokens > triggerRate {
			b.tokens = triggerRate
		}
		b.last = now
	}
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) * float64(time.Second))
}

func (s *Server) handleFireTrigger(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	reject := func(status int, code, reason string) {
		log.Printf("webhook %s rejected: %s", name, reason)
		writeError(w, status, code, reason, nil)
	}

	trigger, err := s.deps.Store.WebhookTrigger(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "internal error", nil)
		return
	}
	if trigger == nil {
		reject(http.StatusNotFound, "TRIGGER_NOT_FOUND", "unknown trigger")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBody+1))
	if err != nil {
		reject(http.StatusBadRequest, "GRAPH_INVALID", "body could not be read")
		return
	}
	if len(body) > maxWebhookBody {
		reject(http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "body exceeds 256 KiB")
		return
	}

	secret, known := s.deps.Config.TriggerSecret(name)
	if !known {
		reject(http.StatusUnauthorized, "UNAUTHORIZED", "signature mismatch")
		return
	}
	now := s.now()
	ts, err := strconv.ParseInt(r.Header.Get("X-Proceed-Timestamp"), 10, 64)
	if err != nil {
		reject(http.StatusUnauthorized, "UNAUTHORIZED", "signature mismatch")
		return
	}
	skew := now.Sub(time.Unix(ts, 0))
	if skew > timestampWindow || skew < -timestampWindow {
		reject(http.StatusUnauthorized, "UNAUTHORIZED", "signature mismatch")
		return
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	if !signatureMatches(r.Header.Get("X-Proceed-Signature"), mac.Sum(nil)) {
		reject(http.StatusUnauthorized, "UNAUTHORIZED", "signature mismatch")
		return
	}

	if ok, retryAfter := s.limiter.allow(name); !ok {
		w.Header().Set("Retry-After", strconv.Itoa(max(1, int(retryAfter.Seconds()))))
		log.Printf("webhook %s rejected: rate_limited", name)
		writeError(w, http.StatusTooManyRequests, "RATE_LIMITED", "rate limit exceeded", nil)
		return
	}

	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil || payload == nil {
		reject(http.StatusBadRequest, "GRAPH_INVALID", "body must be a JSON object")
		return
	}
	decls, err := s.deps.Store.GraphParams(r.Context(), trigger.GraphVersionID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL", "internal error", nil)
		return
	}
	declared := map[string]bool{}
	for _, d := range decls {
		declared[d.Name] = true
	}
	bindings, err := webhookParamBindings(payload, declared)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	bound, err := controller.BindRunParams(decls, bindings)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	runID, err := s.deps.Controller.Run(r.Context(), controller.RunInput{
		GraphVersionID: trigger.GraphVersionID,
		Params:         bound,
		TriggerName:    name,
	})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		_ = s.deps.Controller.Drain(ctx, runID)
	}()
	log.Printf("webhook %s fired run %s", name, runID)
	writeJSON(w, http.StatusAccepted, map[string]any{"run_id": runID})
}

func signatureMatches(header string, want []byte) bool {
	value, ok := strings.CutPrefix(header, "sha256=")
	if !ok {
		return false
	}
	got, err := hex.DecodeString(value)
	if err != nil {
		return false
	}
	return hmac.Equal(got, want)
}

func webhookParamBindings(payload map[string]json.RawMessage, declared map[string]bool) ([]controller.ParamBinding, error) {
	names := make([]string, 0, len(payload))
	for name := range payload {
		if declared[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	out := make([]controller.ParamBinding, 0, len(names))
	for _, name := range names {
		dec := json.NewDecoder(strings.NewReader(string(payload[name])))
		dec.UseNumber()
		var value any
		if err := dec.Decode(&value); err != nil {
			return nil, store.NewCodeError(store.CodeGraphInvalid,
				"param %q must be a JSON scalar", name)
		}
		switch v := value.(type) {
		case string:
			out = append(out, controller.ParamBinding{Name: name, Value: v})
		case json.Number:
			out = append(out, controller.ParamBinding{Name: name, Value: v.String()})
		case bool:
			out = append(out, controller.ParamBinding{Name: name, Value: strconv.FormatBool(v)})
		default:
			return nil, store.NewCodeError(store.CodeGraphInvalid,
				"param %q must be a JSON scalar", name)
		}
	}
	return out, nil
}
