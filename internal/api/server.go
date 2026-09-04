package api

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"proceed/internal/compiler"
	"proceed/internal/config"
	"proceed/internal/controller"
	"proceed/internal/query/export"
	"proceed/internal/store"
)

type Deps struct {
	Store      *store.Store
	Controller *controller.Controller
	Config     config.Config
}

type Server struct {
	deps Deps
	mux  *http.ServeMux
}

func NewServer(deps Deps) *Server {
	s := &Server{deps: deps, mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /v1/runs", s.handleCreateRun)
	s.mux.HandleFunc("GET /v1/runs", s.handleListRuns)
	s.mux.HandleFunc("GET /v1/runs/{id}", s.handleGetRun)
	s.mux.HandleFunc("GET /v1/runs/{id}/graph", s.handleGetRun)
	s.mux.HandleFunc("POST /v1/runs/{id}/cancel", s.handleCancelRun)
	s.mux.HandleFunc("POST /v1/waits/{id}/complete", s.handleCompleteWait)
	s.mux.HandleFunc("POST /v1/approvals/{id}/decision", s.handleApprovalDecision(""))
	s.mux.HandleFunc("POST /v1/approvals/{id}/grant", s.handleApprovalDecision("grant"))
	s.mux.HandleFunc("POST /v1/approvals/{id}/deny", s.handleApprovalDecision("deny"))
	s.mux.HandleFunc("POST /v1/runs/{id}/approve", s.handleReserved("approve"))
	s.mux.HandleFunc("POST /v1/runs/{id}/reconcile", s.handleReserved("admin"))
	s.mux.HandleFunc("GET /v1/runs/{id}/export", s.handleExport)
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func (s *Server) authorize(w http.ResponseWriter, r *http.Request, scope string) bool {
	header := r.Header.Get("Authorization")
	token, ok := strings.CutPrefix(header, "Bearer ")
	if !ok || token == "" {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing bearer token", nil)
		return false
	}
	scopes, known := s.deps.Config.TokenScopes(token)
	if !known {
		writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "unknown token", nil)
		return false
	}
	for _, granted := range scopes {
		if granted == scope {
			return true
		}
	}
	writeError(w, http.StatusForbidden, "POLICY_DENIED", "token lacks the required scope", map[string]any{
		"required_scope": scope,
	})
	return false
}

func (s *Server) handleReserved(scope string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.authorize(w, r, scope) {
			return
		}
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "route is reserved for a future feature", nil)
	}
}

type createRunRequest struct {
	Graph string `json:"graph"`
}

func (s *Server) handleCreateRun(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(w, r, "run") {
		return
	}
	var body createRunRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "GRAPH_INVALID", "request body must be JSON with a graph field", nil)
		return
	}
	if body.Graph == "" {
		writeError(w, http.StatusBadRequest, "GRAPH_INVALID", "graph is required", nil)
		return
	}
	versionID := body.Graph
	if !s.versionExists(r.Context(), versionID) {
		freeze, err := freezeGraph(r.Context(), s.deps.Store, versionID)
		if err != nil {
			writeStoreError(w, err)
			return
		}
		versionID = freeze
	}
	runID, err := s.deps.Controller.Run(r.Context(), controller.RunInput{GraphVersionID: versionID})
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if err := s.deps.Controller.Drain(r.Context(), runID); err != nil && !errors.Is(err, context.Canceled) {
		writeStoreError(w, err)
		return
	}
	g, err := s.deps.Store.RuntimeGraph(r.Context(), runID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"run_id":            g.RunID,
		"status":            g.Status,
		"graph_version_id":  g.GraphVersionID,
		"definition_digest": g.DefinitionDigest,
	})
}

func (s *Server) versionExists(ctx context.Context, versionID string) bool {
	var count int
	if err := s.deps.Store.DB().QueryRowContext(ctx,
		"SELECT COUNT(*) FROM graph_version WHERE id = ?", versionID).Scan(&count); err != nil {
		return false
	}
	return count > 0
}

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(w, r, "read") {
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "GRAPH_INVALID", "limit must be an integer", nil)
			return
		}
		limit = parsed
	}
	summaries, err := s.deps.Store.ListRuns(r.Context(), r.URL.Query().Get("status"), limit)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, store.RunList{Runs: summaries})
}

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(w, r, "read") {
		return
	}
	g, err := s.deps.Store.RuntimeGraph(r.Context(), r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, g)
}

func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(w, r, "run") {
		return
	}
	runID := r.PathValue("id")
	if err := s.deps.Controller.CancelRun(r.Context(), runID); err != nil {
		writeStoreError(w, err)
		return
	}
	// The accepted envelope names the requested transition, not the
	// post-cancel status: cancellation is durable but asynchronous.
	writeJSON(w, http.StatusAccepted, map[string]any{
		"run_id": runID,
		"status": "cancel_requested",
	})
}

func (s *Server) handleCompleteWait(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(w, r, "event") {
		return
	}
	waitID := r.PathValue("id")
	if waitID == "" {
		writeError(w, http.StatusBadRequest, "GRAPH_INVALID", "wait id is required", nil)
		return
	}

	var body controller.CompleteWaitRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "GRAPH_INVALID", "request body must be valid JSON: "+err.Error(), nil)
		return
	}

	body.WaitID = waitID
	result, err := s.deps.Controller.CompleteExternalWait(r.Context(), body)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	if result.HTTPStatus >= 400 {
		writeError(w, result.HTTPStatus, result.Code, result.Message, result.Details)
		return
	}

	if result.Accepted() && result.RunID != "" {
		_ = s.deps.Controller.ResumeRun(r.Context(), result.RunID)
	}

	resp := map[string]any{
		"wait_id": result.WaitID,
		"status":  result.Code,
	}
	if result.RunID != "" {
		resp["run_id"] = result.RunID
	}
	if result.NodeKey != "" {
		resp["node_key"] = result.NodeKey
	}
	if result.Message != "" {
		resp["message"] = result.Message
	}
	writeJSON(w, result.HTTPStatus, resp)
}

func (s *Server) handleApprovalDecision(fixedDecision string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.authorize(w, r, "approve") {
			return
		}
		approvalID := r.PathValue("id")
		if approvalID == "" {
			writeError(w, http.StatusBadRequest, "GRAPH_INVALID", "approval id is required", nil)
			return
		}

		var body struct {
			Decision       string `json:"decision"`
			Actor          string `json:"actor"`
			IdempotencyKey string `json:"decision_idempotency_key"`
			Reason         string `json:"reason"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&body); err != nil {
			writeError(w, http.StatusBadRequest, "GRAPH_INVALID", "request body must be valid JSON: "+err.Error(), nil)
			return
		}
		decision := body.Decision
		if fixedDecision != "" {
			if decision != "" && decision != fixedDecision {
				writeError(w, http.StatusBadRequest, "GRAPH_INVALID", "decision must be "+fixedDecision, nil)
				return
			}
			decision = fixedDecision
		}

		result, err := s.deps.Controller.DecideApproval(r.Context(), controller.ApprovalDecisionRequest{
			ApprovalID:     approvalID,
			Decision:       decision,
			Actor:          body.Actor,
			IdempotencyKey: body.IdempotencyKey,
			Reason:         body.Reason,
		})
		if err != nil {
			writeApprovalDecisionError(w, err)
			return
		}

		switch result.Code {
		case controller.ApprovalDecided, controller.ApprovalAlreadyDecided:
			_ = s.deps.Controller.ResumeRun(r.Context(), result.RunID)
			writeJSON(w, result.HTTPStatus, map[string]any{
				"approval_id": result.ApprovalID,
				"run_id":      result.RunID,
				"node_key":    result.NodeKey,
				"status":      result.Code,
				"decision":    result.Decision,
				"actor":       result.Actor,
			})
		case controller.ApprovalExpired:
			writeJSON(w, result.HTTPStatus, map[string]any{
				"approval_id": result.ApprovalID,
				"run_id":      result.RunID,
				"node_key":    result.NodeKey,
				"status":      result.Code,
				"message":     result.Message,
			})
		default:
			writeError(w, result.HTTPStatus, result.Code, result.Message, nil)
		}
	}
}

func writeApprovalDecisionError(w http.ResponseWriter, err error) {
	if _, ok := compiler.AsGraphInvalid(err); ok {
		writeError(w, http.StatusBadRequest, store.CodeGraphInvalid, err.Error(), nil)
		return
	}
	switch store.ErrorCode(err) {
	case store.CodeGraphInvalid:
		writeError(w, http.StatusBadRequest, store.CodeGraphInvalid, err.Error(), nil)
	default:
		writeInternalError(w, err)
	}
}

func writeInternalError(w http.ResponseWriter, err error) {
	log.Printf("internal error: %v", err)
	writeError(w, http.StatusInternalServerError, "INTERNAL", "internal error", nil)
}

func (s *Server) handleExport(w http.ResponseWriter, r *http.Request) {
	if !s.authorize(w, r, "read") {
		return
	}
	runID := r.PathValue("id")
	if runID == "" {
		writeError(w, http.StatusBadRequest, store.CodeGraphInvalid, "run id is required", nil)
		return
	}
	format := r.URL.Query().Get("format")
	if format == "" {
		format = "json"
	}
	format = strings.ToLower(format)
	if err := export.ValidateFormat(format); err != nil {
		writeError(w, http.StatusBadRequest, store.CodeGraphInvalid, err.Error(), nil)
		return
	}
	out, err := export.Export(r.Context(), s.deps.Store, runID, format)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if format == "mermaid" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(out)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

func freezeGraph(ctx context.Context, st *store.Store, path string) (string, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return "", store.NewCodeError(store.CodeGraphInvalid, "graph file %s cannot be read", path)
	}
	doc, err := compiler.Parse(src)
	if err != nil {
		return "", err
	}
	if err := compiler.Validate(doc); err != nil {
		return "", err
	}
	frozen, err := st.FreezeDefinition(ctx, path, src, doc)
	if err != nil {
		return "", err
	}
	return frozen.GraphVersionID, nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func writeError(w http.ResponseWriter, status int, code, message string, details map[string]any) {
	writeJSON(w, status, errorEnvelope{Error: errorBody{Code: code, Message: message, Details: details}})
}

func writeStoreError(w http.ResponseWriter, err error) {
	if _, ok := compiler.AsGraphInvalid(err); ok {
		writeError(w, http.StatusBadRequest, store.CodeGraphInvalid, err.Error(), nil)
		return
	}
	code := store.ErrorCode(err)
	switch code {
	case "RUN_NOT_FOUND", "WAIT_NOT_FOUND":
		writeError(w, http.StatusNotFound, code, err.Error(), nil)
	case store.CodeGraphInvalid:
		writeError(w, http.StatusBadRequest, code, err.Error(), nil)
	case store.CodeStoreBusy:
		writeError(w, http.StatusConflict, code, err.Error(), nil)
	case store.CodeStoreConflict, "WAIT_CONFLICT":
		writeError(w, http.StatusConflict, code, err.Error(), nil)
	default:
		writeInternalError(w, err)
	}
}
