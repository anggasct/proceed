package store

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	manifestMember = "manifest.json"
	dbMember       = "proceed.db"
)

type archiveEntry struct {
	Path      string `json:"path"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

type exportManifest struct {
	SchemaVersion int            `json:"schema_version"`
	DB            archiveEntry   `json:"db"`
	Artifacts     []archiveEntry `json:"artifacts"`
}

func sha256File(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func (s *Store) verifyCurrent(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	report, err := rebuildProjectionsTx(ctx, tx)
	if err != nil {
		return err
	}
	if report.Diverged {
		return storeErr(CodeGraphInvalid, "store projections diverge from the event stream; refusing to export a divergent store")
	}
	return nil
}

func (s *Store) Export(ctx context.Context, dataDir, output string) error {
	return withDataDirLock(dataDir, func() error {
		return s.exportLocked(ctx, dataDir, output)
	})
}

func (s *Store) exportLocked(ctx context.Context, dataDir, output string) error {
	if err := s.verifyCurrent(ctx); err != nil {
		return err
	}
	snapshot := output + ".snapshot"
	if err := os.Remove(snapshot); err != nil && !os.IsNotExist(err) {
		return err
	}
	quoted := strings.ReplaceAll(snapshot, "'", "''")
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("VACUUM INTO '%s'", quoted)); err != nil {
		return fmt.Errorf("snapshot store: %w", err)
	}
	defer os.Remove(snapshot)

	var artifacts []archiveEntry
	artifactRoot := filepath.Join(dataDir, "artifacts")
	err := filepath.WalkDir(artifactRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) && path == artifactRoot {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dataDir, path)
		if err != nil {
			return err
		}
		digest, size, err := sha256File(path)
		if err != nil {
			return err
		}
		artifacts = append(artifacts, archiveEntry{Path: filepath.ToSlash(rel), SHA256: digest, SizeBytes: size})
		return nil
	})
	if err != nil {
		return err
	}
	dbDigest, dbSize, err := sha256File(snapshot)
	if err != nil {
		return err
	}
	manifest := exportManifest{
		SchemaVersion: storeSchemaVersion,
		DB:            archiveEntry{Path: dbMember, SHA256: dbDigest, SizeBytes: dbSize},
		Artifacts:     artifacts,
	}

	out, err := os.Create(output)
	if err != nil {
		return err
	}
	defer out.Close()
	gz := gzip.NewWriter(out)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	writeMember := func(name string, content io.Reader, size int64) error {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: size}); err != nil {
			return err
		}
		_, err := io.Copy(tw, content)
		return err
	}

	dbFile, err := os.Open(snapshot)
	if err != nil {
		return err
	}
	defer dbFile.Close()
	if err := writeMember(dbMember, dbFile, dbSize); err != nil {
		return err
	}
	for _, a := range artifacts {
		f, err := os.Open(filepath.Join(dataDir, filepath.FromSlash(a.Path)))
		if err != nil {
			return err
		}
		err = writeMember(a.Path, f, a.SizeBytes)
		f.Close()
		if err != nil {
			return err
		}
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	if err := writeMember(manifestMember, strings.NewReader(string(encoded)), int64(len(encoded))); err != nil {
		return err
	}
	return nil
}

func readArchiveMembers(path string) (map[string][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, storeErr(CodeGraphInvalid, "corrupt archive: %v", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	members := map[string][]byte{}
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, storeErr(CodeGraphInvalid, "corrupt archive: %v", err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		name := filepath.ToSlash(filepath.Clean(header.Name))
		if strings.HasPrefix(name, "../") || strings.HasPrefix(name, "/") || name == ".." {
			return nil, storeErr(CodeGraphInvalid, "corrupt archive: unsafe member %q", header.Name)
		}
		content, err := io.ReadAll(tr)
		if err != nil {
			return nil, storeErr(CodeGraphInvalid, "corrupt archive: %v", err)
		}
		members[name] = content
	}
	return members, nil
}

func verifyMember(members map[string][]byte, entry archiveEntry, label string) error {
	content, ok := members[entry.Path]
	if !ok {
		return storeErr(CodeGraphInvalid, "corrupt archive: missing %s %q", label, entry.Path)
	}
	sum := sha256.Sum256(content)
	if hex.EncodeToString(sum[:]) != entry.SHA256 {
		return storeErr(CodeGraphInvalid, "corrupt archive: checksum mismatch for %s %q", label, entry.Path)
	}
	if int64(len(content)) != entry.SizeBytes {
		return storeErr(CodeGraphInvalid, "corrupt archive: size mismatch for %s %q", label, entry.Path)
	}
	return nil
}

func targetHasActiveLease(dbPath string, now int64) (bool, error) {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return false, nil
	}
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return false, err
	}
	defer db.Close()
	var expires int64
	err = db.QueryRow("SELECT lease_expires_at FROM controller_lease WHERE store_id = 'default'").Scan(&expires)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, storeErr(CodeGraphInvalid, "target store unreadable: %v", err)
	}
	return expires > now, nil
}

const dirLockName = "proceed.lock"

func acquireDirLock(dataDir string) (release func() error, err error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dataDir, dirLockName), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return f.Close, nil
}

func withDataDirLock(dataDir string, fn func() error) error {
	release, err := acquireDirLock(dataDir)
	if err != nil {
		return err
	}
	defer func() { _ = release() }()
	return fn()
}

func Import(ctx context.Context, archive, dataDir string) error {
	members, err := readArchiveMembers(archive)
	if err != nil {
		return err
	}
	raw, ok := members[manifestMember]
	if !ok {
		return storeErr(CodeGraphInvalid, "corrupt archive: missing %s", manifestMember)
	}
	var manifest exportManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return storeErr(CodeGraphInvalid, "corrupt archive: bad manifest: %v", err)
	}
	if err := verifyMember(members, manifest.DB, "store"); err != nil {
		return err
	}
	for _, a := range manifest.Artifacts {
		if err := verifyMember(members, a, "artifact"); err != nil {
			return err
		}
	}

	return withDataDirLock(dataDir, func() error {
		dbPath := filepath.Join(dataDir, dbMember)
		busy, err := targetHasActiveLease(dbPath, time.Now().UnixMilli())
		if err != nil {
			return err
		}
		if busy {
			return storeErr(CodeStoreBusy, "target store holds an active controller lease")
		}

		staging, err := os.MkdirTemp(dataDir, ".import-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(staging)

		if err := stageRestore(staging, &manifest, members); err != nil {
			return err
		}
		if err := validateStagedStore(filepath.Join(staging, dbMember), manifest.SchemaVersion); err != nil {
			return err
		}
		if err := verifyStagedProjections(ctx, staging); err != nil {
			return err
		}
		return commitRestore(staging, dataDir, dbPath, &manifest)
	})
}

func stageRestore(staging string, manifest *exportManifest, members map[string][]byte) error {
	if err := os.WriteFile(filepath.Join(staging, dbMember), members[manifest.DB.Path], 0o644); err != nil {
		return err
	}
	for _, a := range manifest.Artifacts {
		target := filepath.Join(staging, filepath.FromSlash(a.Path))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(target, members[a.Path], 0o644); err != nil {
			return err
		}
	}
	return nil
}

var requiredTables = []string{
	"graph", "graph_version", "graph_node", "graph_edge", "policy",
	"graph_run", "run_node", "run_edge", "node_attempt", "controller_lease",
	"event", "artifact", "evaluation", "effect",
	"decision", "causal_link", "approval",
	"anchor", "outcome", "metric", "policy_change_proposal", "proposal_metric",
}

func validateStagedStore(path string, manifestVersion int) error {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return err
	}
	defer db.Close()
	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return storeErr(CodeGraphInvalid, "corrupt archive: store snapshot is unreadable: %v", err)
	}
	if version != manifestVersion {
		return storeErr(CodeGraphInvalid,
			"corrupt archive: store snapshot schema version %d does not match manifest %d", version, manifestVersion)
	}
	if version > storeSchemaVersion {
		return storeErr(CodeGraphInvalid, "archive schema version %d is newer than supported %d",
			version, storeSchemaVersion)
	}
	for _, table := range requiredTables {
		var n int
		if err := db.QueryRow(
			"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&n); err != nil {
			return storeErr(CodeGraphInvalid, "corrupt archive: store snapshot is unreadable: %v", err)
		}
		if n != 1 {
			return storeErr(CodeGraphInvalid, "corrupt archive: store snapshot is missing table %q", table)
		}
	}
	rows, err := db.Query("PRAGMA foreign_key_check")
	if err != nil {
		return storeErr(CodeGraphInvalid, "corrupt archive: store snapshot is unreadable: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		return storeErr(CodeGraphInvalid, "corrupt archive: store snapshot violates foreign key constraints")
	}
	return rows.Err()
}

func verifyStagedProjections(ctx context.Context, staging string) error {
	source := filepath.Join(staging, dbMember)
	copyPath := filepath.Join(staging, "verify.db")
	content, err := os.ReadFile(source)
	if err != nil {
		return err
	}
	if err := os.WriteFile(copyPath, content, 0o644); err != nil {
		return err
	}
	db, err := sql.Open("sqlite",
		"file:"+copyPath+"?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_txlock=immediate")
	if err != nil {
		return err
	}
	defer db.Close()
	var verifyErr error
	err = func() error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		report, err := rebuildProjectionsTx(ctx, tx)
		if err != nil {
			return err
		}
		if report.Diverged {
			return storeErr(CodeGraphInvalid,
				"corrupt archive: projections diverge from the event stream")
		}
		return tx.Commit()
	}()
	if err != nil {
		var se *Error
		if errors.As(err, &se) {
			verifyErr = err
		} else {
			verifyErr = storeErr(CodeGraphInvalid, "corrupt archive: projection verification failed: %v", err)
		}
	} else {
		rows, ferr := db.Query("PRAGMA foreign_key_check")
		if ferr == nil {
			defer rows.Close()
			if rows.Next() {
				verifyErr = storeErr(CodeGraphInvalid, "corrupt archive: rebuilt projections violate foreign key constraints")
			}
		}
	}
	return verifyErr
}

func commitRestore(staging, dataDir, dbPath string, manifest *exportManifest) error {
	busy, err := targetHasActiveLease(dbPath, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	if busy {
		return storeErr(CodeStoreBusy, "target store holds an active controller lease")
	}

	type applied struct {
		dest   string
		backup string
	}
	var appliedMoves []applied
	rollback := func() {
		for i := len(appliedMoves) - 1; i >= 0; i-- {
			m := appliedMoves[i]
			os.Remove(m.dest)
			if m.backup != "" {
				_ = os.Rename(m.backup, m.dest)
			}
		}
	}
	moveIn := func(from, dest string) error {
		var backup string
		if _, err := os.Stat(dest); err == nil {
			rel, err := filepath.Rel(dataDir, dest)
			if err != nil {
				return err
			}
			backup = filepath.Join(staging, "prev", filepath.FromSlash(rel))
			if err := os.MkdirAll(filepath.Dir(backup), 0o755); err != nil {
				return err
			}
			if err := os.Rename(dest, backup); err != nil {
				return err
			}
		}
		if err := os.Rename(from, dest); err != nil {
			if backup != "" {
				_ = os.Rename(backup, dest)
			}
			return err
		}
		appliedMoves = append(appliedMoves, applied{dest: dest, backup: backup})
		return nil
	}

	if err := moveIn(filepath.Join(staging, dbMember), dbPath); err != nil {
		rollback()
		return err
	}
	for _, a := range manifest.Artifacts {
		dest := filepath.Join(dataDir, filepath.FromSlash(a.Path))
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			rollback()
			return err
		}
		if err := moveIn(filepath.Join(staging, filepath.FromSlash(a.Path)), dest); err != nil {
			rollback()
			return err
		}
	}
	return nil
}
