package store

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

func (s *Store) Export(ctx context.Context, dataDir, output string) error {
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
	commit := func() error {
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return err
		}
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
		if _, err := os.Stat(dbPath); err == nil {
			if err := os.Remove(dbPath); err != nil {
				return err
			}
		}
		if err := os.Rename(filepath.Join(staging, dbMember), dbPath); err != nil {
			return err
		}
		for _, a := range manifest.Artifacts {
			dest := filepath.Join(dataDir, filepath.FromSlash(a.Path))
			if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
				return err
			}
			if err := os.Rename(filepath.Join(staging, filepath.FromSlash(a.Path)), dest); err != nil {
				return err
			}
		}
		return nil
	}
	if err := commit(); err != nil {
		return err
	}

	restored, err := Open(dbPath)
	if err != nil {
		return err
	}
	defer restored.Close()
	if restored.SchemaVersion() != manifest.SchemaVersion {
		return storeErr(CodeGraphInvalid, "restored store schema version %d does not match archive manifest %d",
			restored.SchemaVersion(), manifest.SchemaVersion)
	}
	return nil
}
