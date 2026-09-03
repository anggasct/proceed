package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	_ "modernc.org/sqlite"

	"github.com/oklog/ulid/v2"
	yaml "gopkg.in/yaml.v3"

	"proceed/internal/compiler"
)

type Store struct {
	db       *sql.DB
	dataDir  string
	lockPath string
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(filepath.Dir(path), dirLockName)
	dsn := fmt.Sprintf("file:%s?_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=strict(1)&_txlock=immediate", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, closeOnErr(db, err)
	}
	var current int
	if err := db.QueryRow("PRAGMA user_version").Scan(&current); err != nil {
		return nil, closeOnErr(db, err)
	}
	if current > storeSchemaVersion {
		return nil, closeOnErr(db, storeErr(CodeGraphInvalid,
			"store schema version %d is newer than supported %d", current, storeSchemaVersion))
	}
	if current < storeSchemaVersion {
		if err := withDataDirLock(filepath.Dir(path), func() error {
			var locked int
			if err := db.QueryRow("PRAGMA user_version").Scan(&locked); err != nil {
				return err
			}
			if locked >= storeSchemaVersion {
				return nil
			}
			return migrateUnderLock(ctxBackground(), db, path)
		}); err != nil {
			return nil, closeOnErr(db, err)
		}
	} else if _, err := db.Exec(schemaDDL); err != nil {
		return nil, closeOnErr(db, fmt.Errorf("apply schema: %w", err))
	}
	return &Store{db: db, dataDir: filepath.Dir(path), lockPath: lockPath}, nil
}

func migrateUnderLock(ctx context.Context, db *sql.DB, path string) error {
	backupPath := migrationBackupPath(path)
	if err := os.Remove(backupPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	if _, err := db.Exec(fmt.Sprintf("VACUUM INTO '%s'", backupPath)); err != nil {
		return fmt.Errorf("pre-migration backup: %w", err)
	}
	return migrateInPlace(ctx, db)
}

func migrationBackupPath(path string) string {
	return path + fmt.Sprintf(".pre-schema-%d.bak", storeSchemaVersion)
}

func migrateSchemaAdditions(ctx context.Context, conn *sql.Conn) error {
	var n int
	if err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('policy_change_proposal') WHERE name = 'rejection_reason'`).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	_, err := conn.ExecContext(ctx, `ALTER TABLE policy_change_proposal ADD COLUMN rejection_reason TEXT`)
	return err
}

func ctxBackground() context.Context { return context.Background() }

func migrateInPlace(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, schemaDDL); err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		return fmt.Errorf("apply schema: %w", err)
	}
	if err := migrateSchemaAdditions(ctx, conn); err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		return fmt.Errorf("apply schema migrations: %w", err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", storeSchemaVersion)); err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		_, _ = conn.ExecContext(ctx, "ROLLBACK")
		return err
	}
	return nil
}

func closeOnErr(db *sql.DB, err error) error {
	if closeErr := db.Close(); closeErr != nil {
		return fmt.Errorf("%w (close: %v)", err, closeErr)
	}
	return err
}

func (s *Store) Close() error {
	return s.db.Close()
}

type SourceMetadata struct {
	Path         string `json:"path"`
	ByteSize     int64  `json:"byte_size"`
	SourceSHA256 string `json:"source_sha256"`
}

type FrozenVersion struct {
	GraphID        string
	GraphVersionID string
	Digest         string
	Created        bool
}

type definitionRow struct {
	Name                string
	Digest              string
	SourceSchemaVersion string
	SourceMetadata      string
	Extras              string
	Doc                 *compiler.Document
}

func (s *Store) FreezeDefinition(ctx context.Context, sourcePath string, src []byte, doc *compiler.Document) (FrozenVersion, error) {
	canonical, err := compiler.CanonicalJSON(src)
	if err != nil {
		return FrozenVersion{}, err
	}
	digest := compiler.DefinitionDigest(canonical)
	meta, err := json.Marshal(SourceMetadata{
		Path:         sourcePath,
		ByteSize:     int64(len(src)),
		SourceSHA256: compiler.DefinitionDigest(src),
	})
	if err != nil {
		return FrozenVersion{}, err
	}
	row := definitionRow{
		Name:                doc.Name,
		Digest:              digest,
		SourceSchemaVersion: doc.Schema,
		SourceMetadata:      string(meta),
		Extras:              extrasJSON(doc.Extras),
		Doc:                 doc,
	}
	var result FrozenVersion
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		graphID, err := ensureGraph(ctx, tx, row.Name)
		if err != nil {
			return err
		}
		result.GraphID = graphID
		existing, err := versionIDByDigest(ctx, tx, digest)
		if err != nil {
			return err
		}
		if existing != "" {
			result.GraphVersionID = existing
			result.Digest = digest
			result.Created = false
			return nil
		}
		versionID := ulid.Make().String()
		if err := insertVersion(ctx, tx, versionID, graphID, row); err != nil {
			return err
		}
		if err := insertNodes(ctx, tx, versionID, row.Doc); err != nil {
			return err
		}
		if err := insertEdges(ctx, tx, versionID, row.Doc); err != nil {
			return err
		}
		if err := insertPolicies(ctx, tx, versionID, row.Doc); err != nil {
			return err
		}
		result.GraphVersionID = versionID
		result.Digest = digest
		result.Created = true
		return nil
	})
	if err != nil {
		return FrozenVersion{}, err
	}
	return result, nil
}

func (s *Store) lockShared() (release func(), err error) {
	if s.lockPath == "" {
		return func() {}, nil
	}
	f, err := os.OpenFile(s.lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH); err != nil {
		f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}

func (s *Store) withTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	release, err := s.lockShared()
	if err != nil {
		return err
	}
	defer release()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("%w (rollback: %v)", err, rbErr)
		}
		return err
	}
	return tx.Commit()
}

func (s *Store) WithTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	return s.withTx(ctx, fn)
}

func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) DataDir() string { return s.dataDir }

func ensureGraph(ctx context.Context, tx *sql.Tx, name string) (string, error) {
	var id string
	err := tx.QueryRowContext(ctx, "SELECT id FROM graph WHERE name = ? COLLATE NOCASE", name).Scan(&id)
	if err == nil {
		return id, nil
	}
	if err != sql.ErrNoRows {
		return "", err
	}
	id = ulid.Make().String()
	if _, err := tx.ExecContext(ctx, "INSERT INTO graph (id, name) VALUES (?, ?)", id, name); err != nil {
		return "", err
	}
	return id, nil
}

func versionIDByDigest(ctx context.Context, tx *sql.Tx, digest string) (string, error) {
	var id string
	err := tx.QueryRowContext(ctx, "SELECT id FROM graph_version WHERE definition_digest = ?", digest).Scan(&id)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return id, nil
}

func insertVersion(ctx context.Context, tx *sql.Tx, versionID, graphID string, row definitionRow) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO graph_version (id, graph_id, definition_digest, source_schema_version,
                           compiled_schema_version, source_metadata, extras, status, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, 'frozen', ?)`,
		versionID, graphID, row.Digest, row.SourceSchemaVersion,
		compiler.CompiledSchemaVersion, row.SourceMetadata, row.Extras, time.Now().UnixMilli())
	return err
}

func insertNodes(ctx context.Context, tx *sql.Tx, versionID string, doc *compiler.Document) error {
	for i := range doc.Nodes {
		n := &doc.Nodes[i]
		config, err := nodeConfigJSON(n)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO graph_node (id, graph_version_id, node_key, type, config) VALUES (?, ?, ?, ?, ?)",
			ulid.Make().String(), versionID, n.ID, n.Type, config); err != nil {
			return err
		}
	}
	return nil
}

func insertEdges(ctx context.Context, tx *sql.Tx, versionID string, doc *compiler.Document) error {
	for i := range doc.Edges {
		e := &doc.Edges[i]
		var condition any
		if e.Type == "routes_to" && e.HasWhen {
			condition = e.When
		}
		var maxTraversals any
		if e.HasMaxTraversals {
			maxTraversals = e.MaxTraversals
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO graph_edge (id, graph_version_id, from_node_key, to_node_key, type, condition, max_traversals, extras)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			ulid.Make().String(), versionID, e.From, e.To, e.Type, condition, maxTraversals, extrasJSON(e.Extras)); err != nil {
			return err
		}
	}
	return nil
}

func insertPolicies(ctx context.Context, tx *sql.Tx, versionID string, doc *compiler.Document) error {
	for i := range doc.Policies {
		po := &doc.Policies[i]
		rule, err := compiler.NodeToJSONValue(po.Rule)
		if err != nil {
			return err
		}
		encoded, err := json.Marshal(rule)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO policy (id, graph_version_id, kind, rule, extras) VALUES (?, ?, ?, ?, ?)",
			ulid.Make().String(), versionID, po.Kind, string(encoded), extrasJSON(po.Extras)); err != nil {
			return err
		}
	}
	return nil
}

func extrasJSON(extras map[string]yaml.Node) string {
	if len(extras) == 0 {
		return "{}"
	}
	out := make(map[string]any, len(extras))
	for k, v := range extras {
		val, err := compiler.NodeToJSONValue(&v)
		if err != nil {
			return "{}"
		}
		out[k] = val
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

func nodeConfigJSON(n *compiler.Node) (string, error) {
	config := map[string]any{}
	if n.HasTerminal {
		config["terminal"] = n.Terminal
	}
	if n.Executor != nil {
		executor := map[string]any{"kind": n.Executor.Kind}
		for k, v := range n.Executor.Extras {
			xv, err := compiler.NodeToJSONValue(&v)
			if err != nil {
				return "", err
			}
			executor[k] = xv
		}
		switch n.Executor.Kind {
		case "shell":
			executor["command"] = n.Executor.Command
			if n.Executor.Workdir != "" {
				executor["workdir"] = n.Executor.Workdir
			}
		case "http":
			executor["method"] = n.Executor.Method
			executor["url"] = n.Executor.URL
			if len(n.Executor.Headers) > 0 {
				executor["headers"] = n.Executor.Headers
			}
			if n.Executor.Body != nil {
				body, err := compiler.NodeToJSONValue(n.Executor.Body)
				if err != nil {
					return "", err
				}
				executor["body"] = body
			}
		case "human_approval":
			executor["scope"] = n.Executor.Scope
			if n.Executor.ExpiresInMs != 0 {
				executor["expires_in_ms"] = n.Executor.ExpiresInMs
			}
		case "agent_cli":
			executor["cli"] = n.Executor.CLI
			if len(n.Executor.Args) > 0 {
				executor["args"] = n.Executor.Args
			}
		}
		config["executor"] = executor
	}
	if n.Capability != nil {
		capability := map[string]any{}
		for k, v := range n.Capability.Extras {
			cv, err := compiler.NodeToJSONValue(&v)
			if err != nil {
				return "", err
			}
			capability[k] = cv
		}
		if n.Capability.Filesystem != "" {
			capability["filesystem"] = n.Capability.Filesystem
		}
		if n.Capability.Network != nil {
			net := n.Capability.Network
			if net.Mode != "" {
				capability["network"] = net.Mode
			} else {
				network := map[string]any{}
				for k, v := range net.Extras {
					nv, err := compiler.NodeToJSONValue(&v)
					if err != nil {
						return "", err
					}
					network[k] = nv
				}
				if len(net.AllowlistedHosts) > 0 {
					network["allowlisted_hosts"] = net.AllowlistedHosts
				}
				capability["network"] = network
			}
		}
		if n.Capability.Process != "" {
			capability["process"] = n.Capability.Process
		}
		if n.Capability.SecretsLiteral != "" {
			capability["secrets"] = n.Capability.SecretsLiteral
		} else if len(n.Capability.Secrets) > 0 {
			capability["secrets"] = n.Capability.Secrets
		}
		if n.Capability.Human != "" {
			capability["human"] = n.Capability.Human
		}
		config["capability"] = capability
	}
	if n.HasContract {
		config["contract"] = n.Contract
	}
	if n.TimeoutMs != 0 {
		config["timeout_ms"] = n.TimeoutMs
	}
	if n.Retry != nil {
		retry := map[string]any{}
		for k, v := range n.Retry.Extras {
			rv, err := compiler.NodeToJSONValue(&v)
			if err != nil {
				return "", err
			}
			retry[k] = rv
		}
		if n.Retry.MaxAttempts != 0 {
			retry["max_attempts"] = n.Retry.MaxAttempts
		}
		if n.Retry.BackoffMs != 0 {
			retry["backoff_ms"] = n.Retry.BackoffMs
		}
		if len(n.Retry.RetryableErrors) > 0 {
			retry["retryable_errors"] = n.Retry.RetryableErrors
		}
		config["retry"] = retry
	}
	for k, v := range n.Extras {
		ev, err := compiler.NodeToJSONValue(&v)
		if err != nil {
			return "", err
		}
		config[k] = ev
	}
	encoded, err := json.Marshal(config)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}
