package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"

	"proceed/internal/compiler"
	"proceed/internal/config"
	"proceed/internal/controller"
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
	s.mux.HandleFunc("GET /v1/runs/{id}", s.handleGetRun)
	s.mux.HandleFunc("GET /v1/runs/{id}/graph", s.handleGetRun)
	s.mux.HandleFunc("POST /v1/runs/{id}/cancel", s.handleCancelRun)
	s.mux.HandleFunc("POST /v1/runs/{id}/approve", s.handleReserved("approve"))
	s.mux.HandleFunc("POST /v1/runs/{id}/reconcile", s.handleReserved("admin"))
	s.mux.HandleFunc("GET /v1/runs/{id}/export", s.handleReserved("read"))
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
	g, err := s.deps.Store.RuntimeGraph(r.Context(), runID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"run_id": runID,
		"status": g.Status,
	})
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
	case "RUN_NOT_FOUND":
		writeError(w, http.StatusNotFound, code, err.Error(), nil)
	case store.CodeGraphInvalid:
		writeError(w, http.StatusBadRequest, code, err.Error(), nil)
	case store.CodeStoreBusy:
		writeError(w, http.StatusConflict, code, err.Error(), nil)
	case store.CodeStoreConflict:
		writeError(w, http.StatusConflict, code, err.Error(), nil)
	default:
		writeError(w, http.StatusInternalServerError, "INTERNAL", err.Error(), nil)
	}
}
