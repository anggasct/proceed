package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"proceed/internal/capability"
	"proceed/internal/executor"
)

const DefaultOutputLimit = 1 << 20

type Executor struct {
	Client      *http.Client
	OutputLimit int
}

func New() *Executor {
	return &Executor{OutputLimit: DefaultOutputLimit}
}

func (e *Executor) Kind() executor.Kind { return executor.HTTP }

type httpConfig struct {
	method      string
	url         string
	headers     map[string]string
	body        []byte
	allowHosts  []string
	secretNames map[string]bool
}

var allowedMethods = map[string]bool{
	http.MethodGet: true, http.MethodHead: true, http.MethodPost: true,
	http.MethodPut: true, http.MethodPatch: true, http.MethodDelete: true,
}

func (e *Executor) Admit(ctx context.Context, req *executor.Request) error {
	if req == nil {
		return &capability.Error{Message: "executor request is required"}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := parseConfig(req.Config)
	return err
}

func parseConfig(config map[string]any) (httpConfig, error) {
	if config == nil {
		return httpConfig{}, &capability.Error{Message: "executor request is required"}
	}
	raw, ok := config["executor"].(map[string]any)
	if !ok {
		return httpConfig{}, &capability.Error{Message: "http executor config is required"}
	}
	if kind, _ := raw["kind"].(string); kind != string(executor.HTTP) {
		return httpConfig{}, &capability.Error{Message: "http executor kind is required"}
	}
	cfg := httpConfig{method: http.MethodGet, secretNames: map[string]bool{}}
	if value, ok := raw["method"].(string); ok && value != "" {
		cfg.method = strings.ToUpper(value)
	}
	if !allowedMethods[cfg.method] {
		return httpConfig{}, &capability.Error{Message: "http method is not supported"}
	}
	rawURL, ok := raw["url"].(string)
	if !ok || rawURL == "" {
		return httpConfig{}, &capability.Error{Message: "http url is required"}
	}
	cfg.url = rawURL
	target, err := url.Parse(rawURL)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Host == "" {
		return httpConfig{}, &capability.Error{Message: "http url must be an absolute http or https url"}
	}
	if target.User != nil {
		return httpConfig{}, &capability.Error{Message: "http url must not contain credentials"}
	}
	if value, ok := raw["headers"]; ok && value != nil {
		headers, ok := value.(map[string]any)
		if !ok {
			return httpConfig{}, &capability.Error{Message: "http headers must be an object"}
		}
		cfg.headers = make(map[string]string, len(headers))
		for name, value := range headers {
			headerValue, ok := value.(string)
			if !ok || !validHeaderName(name) || strings.ContainsAny(headerValue, "\r\n") {
				return httpConfig{}, &capability.Error{Message: "http header names and values are invalid"}
			}
			if _, exists := cfg.headers[http.CanonicalHeaderKey(name)]; exists {
				return httpConfig{}, &capability.Error{Message: "http header names must be unique"}
			}
			if strings.Contains(headerValue, "${") && !isFullSecretReference(headerValue) {
				return httpConfig{}, &capability.Error{Message: "http header secret references must be whole values"}
			}
			cfg.headers[name] = headerValue
		}
	}
	switch body := raw["body"].(type) {
	case nil:
	case string:
		cfg.body = []byte(body)
	default:
		encoded, err := json.Marshal(body)
		if err != nil {
			return httpConfig{}, &capability.Error{Message: "http body must be a string or a json value"}
		}
		cfg.body = encoded
	}

	capBlock, ok := config["capability"].(map[string]any)
	if !ok {
		return httpConfig{}, &capability.Error{Message: "http requires the network capability"}
	}
	network, ok := capBlock["network"].(map[string]any)
	if !ok {
		return httpConfig{}, &capability.Error{Message: "http requires an allowlisted_hosts network capability"}
	}
	rawHosts, ok := network["allowlisted_hosts"].([]any)
	if !ok || len(rawHosts) == 0 {
		return httpConfig{}, &capability.Error{Message: "allowlisted_hosts requires at least one host"}
	}
	for _, value := range rawHosts {
		host, ok := value.(string)
		if !ok || canonicalHost(host) == "" {
			return httpConfig{}, &capability.Error{Message: "allowlisted hosts must be hostnames"}
		}
		cfg.allowHosts = append(cfg.allowHosts, canonicalHost(host))
	}
	if !hostAllowed(cfg.allowHosts, target) {
		return httpConfig{}, &capability.Error{Message: "target host is not allowlisted"}
	}
	if value, ok := capBlock["secrets"]; ok && value != nil {
		names, ok := value.([]any)
		if !ok {
			return httpConfig{}, &capability.Error{Message: "capability secrets must be a list"}
		}
		for _, name := range names {
			secret, ok := name.(string)
			if !ok {
				return httpConfig{}, &capability.Error{Message: "capability secrets must be strings"}
			}
			normalized, ok := capability.NormalizeSecretReference(secret)
			if !ok {
				return httpConfig{}, &capability.Error{Message: "secret reference has invalid name"}
			}
			cfg.secretNames[normalized] = true
		}
	}
	for name, value := range cfg.headers {
		if isFullSecretReference(value) && !cfg.secretNames[value[2:len(value)-1]] {
			return httpConfig{}, &capability.Error{Message: "header references secret which is not declared: " + name}
		}
	}
	return cfg, nil
}

func isFullSecretReference(value string) bool {
	if !strings.HasPrefix(value, "${") || !strings.HasSuffix(value, "}") {
		return false
	}
	_, ok := capability.NormalizeSecretReference(value)
	return ok
}

func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if r > 127 || !tokenRune(r) {
			return false
		}
	}
	return true
}

func tokenRune(r rune) bool {
	switch r {
	case '(', ')', '<', '>', '@', ',', ';', ':', '\\', '"', '/', '[', ']', '?', '=', '{', '}', ' ':
		return false
	}
	return r > 32 && r < 127
}

func canonicalHost(host string) string {
	return strings.ToLower(strings.TrimSuffix(host, "."))
}

func hostAllowed(hosts []string, target *url.URL) bool {
	host := canonicalHost(target.Hostname())
	for _, allowed := range hosts {
		if allowed == host {
			return true
		}
	}
	return false
}

func (e *Executor) Execute(ctx context.Context, req *executor.Request) (*executor.Result, error) {
	if req == nil {
		return nil, &capability.Error{Message: "executor request is required"}
	}
	if err := e.Admit(ctx, req); err != nil {
		return nil, err
	}
	cfg, err := parseConfig(req.Config)
	if err != nil {
		return nil, err
	}
	resolved, redactions, err := resolveRequest(ctx, req, cfg)
	if err != nil {
		return nil, err
	}

	var effectID string
	if req.EffectPublisher != nil {
		effectID, err = req.EffectPublisher.RecordIntent(ctx, executor.EffectIntent{
			Target:        cfg.url,
			RequestDigest: requestDigest(resolved, redactions),
		})
		if err != nil {
			return nil, err
		}
	}

	response, sendErr := e.dispatch(ctx, req, resolved, cfg.allowHosts, redactions)
	if sendErr != nil {
		if errors.Is(sendErr, errDispatchUncertain) {
			e.recordReceipt(ctx, req, effectID, executor.EffectUnknown, nil)
			return nil, executor.ErrUncertain
		}
		if errors.Is(sendErr, executor.ErrTimeout) {
			e.recordReceipt(ctx, req, effectID, executor.EffectUnknown, nil)
			return nil, executor.ErrTimeout
		}
		if errors.Is(sendErr, executor.ErrCancelled) {
			e.recordReceipt(ctx, req, effectID, executor.EffectUnknown, nil)
			return nil, executor.ErrCancelled
		}
		e.recordReceipt(ctx, req, effectID, executor.EffectRejected, nil)
		return nil, &FailureError{reason: "request could not be delivered"}
	}

	body, truncated := bound(redact(response.body, redactions), e.limit())
	status := response.status
	receipt, _ := json.Marshal(map[string]any{
		"status_code": status,
		"truncated":   truncated,
	})
	effectStatus := executor.EffectConfirmed
	if status >= 400 {
		effectStatus = executor.EffectRejected
	}
	if err := e.recordReceipt(ctx, req, effectID, effectStatus, receipt); err != nil {
		return nil, executor.ErrUncertain
	}

	result := &executor.Result{Output: map[string]any{
		"status_code": status,
		"body":        string(body),
		"truncated":   truncated,
	}}
	if req.ArtifactPublisher != nil {
		ref, err := req.ArtifactPublisher.Publish(ctx, executor.ArtifactInput{
			Name:      "response",
			MediaType: response.mediaType,
			Content:   body,
			Truncated: truncated,
		})
		if err != nil {
			return nil, &FailureError{reason: "artifact publication failed"}
		}
		result.Artifacts = append(result.Artifacts, ref)
	}
	if status >= 400 {
		result.Output["exit_code"] = status
		return result, &FailureError{exitCode: status}
	}
	return result, nil
}

func (e *Executor) recordReceipt(ctx context.Context, req *executor.Request, effectID string, status executor.EffectState, receipt []byte) error {
	if effectID == "" || req.EffectPublisher == nil {
		return nil
	}
	return req.EffectPublisher.RecordReceipt(ctx, executor.EffectReceipt{
		EffectID: effectID,
		Status:   status,
		Receipt:  receipt,
	})
}

var errDispatchUncertain = errors.New("request outcome is uncertain")
var errRedirectDenied = errors.New("redirect target host is not allowlisted")
var errDeliveryFailed = errors.New("request delivery failed")

func (e *Executor) dispatch(ctx context.Context, req *executor.Request, resolved resolvedRequest, hosts []string, redactions [][]byte) (responseSummary, error) {
	watchCtx := ctx
	var cancel context.CancelFunc
	timeout := e.timeout(req)
	if timeout > 0 {
		watchCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if req.Cancellation != nil {
		var cancelWatch context.CancelFunc
		watchCtx, cancelWatch = context.WithCancel(watchCtx)
		defer cancelWatch()
		go func() {
			select {
			case <-watchCtx.Done():
			case <-req.Cancellation:
				cancelWatch()
			}
		}()
	}

	httpReq, err := http.NewRequestWithContext(watchCtx, resolved.method, resolved.url, bytes.NewReader(resolved.body))
	if err != nil {
		return responseSummary{}, errDeliveryFailed
	}
	for name, value := range resolved.headers {
		httpReq.Header.Set(name, value)
	}
	if req.Contract == executor.Idempotent {
		httpReq.Header.Set("Idempotency-Key", stableOperationKey(req))
	}

	client := e.clientFor(hosts)
	response, err := client.Do(httpReq)
	if err != nil {
		if watchCtx.Err() != nil {
			if errors.Is(watchCtx.Err(), context.DeadlineExceeded) {
				return responseSummary{}, executor.ErrTimeout
			}
			if req.Cancellation != nil {
				select {
				case <-req.Cancellation:
					return responseSummary{}, executor.ErrCancelled
				default:
				}
			}
			if ctx.Err() != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return responseSummary{}, executor.ErrTimeout
			}
			return responseSummary{}, executor.ErrCancelled
		}
		if req.Cancellation != nil {
			select {
			case <-req.Cancellation:
				return responseSummary{}, executor.ErrCancelled
			default:
			}
		}
		if isPreSendError(err) || errors.Is(err, errRedirectDenied) {
			return responseSummary{}, errDeliveryFailed
		}
		return responseSummary{}, errDispatchUncertain
	}
	defer response.Body.Close()

	limit := e.limit()
	capture := limit + captureExtra(redactions)
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, int64(capture)+1))
	if readErr != nil {
		// The request was already dispatched, so a torn body cannot be
		// reported as a definitive outcome.
		return responseSummary{}, errDispatchUncertain
	}
	mediaType := response.Header.Get("Content-Type")
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}
	return responseSummary{
		status:    response.StatusCode,
		body:      raw,
		mediaType: mediaType,
	}, nil
}

// clientFor returns the HTTP client for a request against the given
// allowlisted hosts. The constructed default enforces the redirect
// allowlist on every hop; an injected client remains a test seam.
func (e *Executor) clientFor(hosts []string) *http.Client {
	if e.Client != nil {
		return e.Client
	}
	return &http.Client{
		CheckRedirect: func(next *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("stopped after 10 redirects")
			}
			if !hostAllowed(hosts, next.URL) {
				return errRedirectDenied
			}
			return nil
		},
	}
}

func isPreSendError(err error) bool {
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return false
	}
	var netErr net.Error
	if errors.As(urlErr.Err, &netErr) && netErr.Timeout() {
		return false
	}
	var dnsErr *net.DNSError
	if errors.As(urlErr.Err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(urlErr.Err, &opErr) && opErr.Op == "dial" {
		return true
	}
	return false
}

func (e *Executor) ReconcileResult(ctx context.Context, req *executor.Request) (*executor.Result, executor.EffectState, error) {
	if req == nil {
		return nil, executor.EffectUnknown, &capability.Error{Message: "executor request is required"}
	}
	cfg, err := parseConfig(req.Config)
	if err != nil {
		return nil, executor.EffectUnknown, err
	}
	resolved, _, err := resolveRequest(ctx, req, cfg)
	if err != nil {
		return nil, executor.EffectUnknown, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, resolved.url, nil)
	if err != nil {
		return nil, executor.EffectUnknown, errDeliveryFailed
	}
	for name, value := range resolved.headers {
		httpReq.Header.Set(name, value)
	}
	client := e.clientFor(cfg.allowHosts)
	response, err := client.Do(httpReq)
	if err != nil {
		return nil, executor.EffectUnknown, nil
	}
	defer response.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, int64(e.limit())+1))
	if readErr != nil {
		return nil, executor.EffectUnknown, nil
	}
	switch {
	case response.StatusCode >= 200 && response.StatusCode < 300:
		return &executor.Result{Output: map[string]any{
			"status_code": response.StatusCode,
			"body":        string(raw),
			"reconciled":  true,
		}}, executor.EffectConfirmed, nil
	case response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusGone:
		return &executor.Result{Output: map[string]any{
			"status_code": response.StatusCode,
			"reconciled":  true,
		}}, executor.EffectAbsent, nil
	default:
		return nil, executor.EffectUnknown, nil
	}
}

func (e *Executor) Reconcile(ctx context.Context, req *executor.Request) (executor.EffectState, error) {
	_, state, err := e.ReconcileResult(ctx, req)
	return state, err
}

func resolveRequest(ctx context.Context, req *executor.Request, cfg httpConfig) (resolvedRequest, [][]byte, error) {
	resolved := resolvedRequest{
		method:  cfg.method,
		url:     cfg.url,
		headers: make(map[string]string, len(cfg.headers)),
		body:    cfg.body,
	}
	var redactions [][]byte
	for name, value := range cfg.headers {
		if !isFullSecretReference(value) {
			resolved.headers[name] = value
			continue
		}
		secretName := value[2 : len(value)-1]
		if !cfg.secretNames[secretName] {
			return resolvedRequest{}, nil, &capability.Error{Message: "secret reference is not declared"}
		}
		if req.Secrets == nil {
			return resolvedRequest{}, nil, &capability.Error{Message: "secret resolver is required"}
		}
		secret, err := req.Secrets.Resolve(ctx, secretName)
		if err != nil {
			return resolvedRequest{}, nil, &capability.Error{Message: "secret resolution failed"}
		}
		if len(secret) > 0 {
			redactions = append(redactions, append([]byte(nil), secret...))
		}
		resolved.headers[name] = string(secret)
	}
	return resolved, redactions, nil
}

func requestDigest(resolved resolvedRequest, redactions [][]byte) string {
	names := make([]string, 0, len(resolved.headers))
	for name := range resolved.headers {
		names = append(names, name)
	}
	sort.Strings(names)
	h := sha256.New()
	fmt.Fprintf(h, "%s\n%s\n", resolved.method, resolved.url)
	for _, name := range names {
		value := resolved.headers[name]
		for _, secret := range redactions {
			if len(secret) > 0 {
				value = strings.ReplaceAll(value, string(secret), "[REDACTED]")
			}
		}
		fmt.Fprintf(h, "%s: %s\n", name, value)
	}
	bodyHash := sha256.Sum256(resolved.body)
	fmt.Fprintf(h, "%s", hex.EncodeToString(bodyHash[:]))
	return hex.EncodeToString(h.Sum(nil))
}

func stableOperationKey(req *executor.Request) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s", req.RunID, req.DefinitionDigest, req.NodeKey)
	return hex.EncodeToString(h.Sum(nil))
}

func captureExtra(redactions [][]byte) int {
	maxSecret := 0
	for _, secret := range redactions {
		if len(secret) > maxSecret {
			maxSecret = len(secret)
		}
	}
	if maxSecret <= 1 {
		return 0
	}
	return maxSecret - 1
}

func redact(value []byte, secrets [][]byte) []byte {
	for _, secret := range secrets {
		if len(secret) > 0 {
			value = bytes.ReplaceAll(value, secret, []byte("[REDACTED]"))
		}
	}
	return value
}

func bound(value []byte, limit int) ([]byte, bool) {
	if len(value) > limit {
		return value[:limit], true
	}
	return value, false
}

func (e *Executor) limit() int {
	if e.OutputLimit > 0 {
		return e.OutputLimit
	}
	return DefaultOutputLimit
}

func (e *Executor) timeout(req *executor.Request) time.Duration {
	if req != nil && req.TimeoutMs > 0 {
		return time.Duration(req.TimeoutMs) * time.Millisecond
	}
	return 30 * time.Second
}

type resolvedRequest struct {
	method  string
	url     string
	headers map[string]string
	body    []byte
}

type responseSummary struct {
	status    int
	body      []byte
	mediaType string
}

type FailureError struct {
	exitCode int
	reason   string
}

var _ executor.Executor = (*Executor)(nil)
var _ executor.Admitter = (*Executor)(nil)
var _ executor.ResultReconciler = (*Executor)(nil)

func (e *FailureError) Error() string {
	if e.reason != "" {
		return "NODE_FAILED: " + e.reason
	}
	return fmt.Sprintf("NODE_FAILED: request failed with status %d", e.exitCode)
}
