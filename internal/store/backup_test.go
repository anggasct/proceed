package store

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

const artifactContent = "research artifact payload"

func buildPopulatedDir(t *testing.T, dataDir string) *Store {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dataDir, "artifacts"), 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	versionID, edgeID, nodeA, nodeB := fixtureVersion(t, s)
	run, err := s.CreateRun(context.Background(), versionID)
	if err != nil {
		t.Fatal(err)
	}
	appendFixtureStream(t, s, run, edgeID, nodeA, nodeB)
	if err := os.WriteFile(filepath.Join(dataDir, "artifacts", "aa"), []byte(artifactContent), 0o644); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestExportImportRoundTrip(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	before, err := s.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(archive); err != nil || info.Size() == 0 {
		t.Fatalf("archive stat: %v (%v)", info, err)
	}
	s.Close()

	targetDir := t.TempDir()
	if err := Import(ctx, archive, targetDir); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(filepath.Join(targetDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	after, err := restored.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Errorf("restored digest %s != source digest %s", after, before)
	}
	got, err := os.ReadFile(filepath.Join(targetDir, "artifacts", "aa"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != artifactContent {
		t.Errorf("restored artifact = %q", got)
	}
}

func TestExportManifestCarriesChecksums(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	defer s.Close()
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}
	members, err := readArchiveMembers(archive)
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := members[manifestMember]
	if !ok {
		t.Fatal("archive missing manifest")
	}
	var manifest exportManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != storeSchemaVersion {
		t.Errorf("manifest schema_version = %d", manifest.SchemaVersion)
	}
	if len(manifest.Artifacts) != 1 || manifest.Artifacts[0].Path != "artifacts/aa" {
		t.Fatalf("manifest artifacts = %+v", manifest.Artifacts)
	}
	sum := sha256.Sum256([]byte(artifactContent))
	if manifest.Artifacts[0].SHA256 != hex.EncodeToString(sum[:]) {
		t.Errorf("artifact checksum = %s", manifest.Artifacts[0].SHA256)
	}
	dbSum := sha256.Sum256(members[manifest.DB.Path])
	if manifest.DB.SHA256 != hex.EncodeToString(dbSum[:]) {
		t.Error("db checksum does not match archived store")
	}
}

func TestImportTargetExistingFileNotTouched(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	s.Close()
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}

	targetFile := filepath.Join(t.TempDir(), "not-a-dir")
	content := []byte("unrelated file content")
	if err := os.WriteFile(targetFile, content, 0o644); err != nil {
		t.Fatal(err)
	}

	err := Import(ctx, archive, targetFile)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
	got, err := os.ReadFile(targetFile)
	if err != nil {
		t.Fatalf("target file was removed by refused import: %v", err)
	}
	if string(got) != string(content) {
		t.Errorf("target file mutated by refused import: %q", got)
	}
}

func TestImportActiveLeaseRefusalLeavesNoLock(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	s.Close()
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}

	targetDir := t.TempDir()
	target := buildPopulatedDir(t, targetDir)
	now := time.Now().UnixMilli()
	if _, err := target.db.Exec(
		`INSERT INTO controller_lease (store_id, owner_id, mode, heartbeat_at, lease_expires_at)
VALUES ('default', 'controller-1', 'run', ?, ?)`, now, now+60000); err != nil {
		t.Fatal(err)
	}
	target.Close()
	if err := os.Remove(filepath.Join(targetDir, dirLockName)); err != nil {
		t.Fatal(err)
	}

	err := Import(ctx, archive, targetDir)
	if !IsCode(err, CodeStoreBusy) {
		t.Fatalf("error = %v, want STORE_BUSY", err)
	}
	if _, statErr := os.Stat(filepath.Join(targetDir, dirLockName)); !os.IsNotExist(statErr) {
		t.Errorf("active-lease refusal must not leave a lock file behind (stat err = %v)", statErr)
	}
}

func TestExportMigratesOlderStore(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	if _, err := s.db.Exec("PRAGMA user_version = 1"); err != nil {
		t.Fatal(err)
	}
	s.Close()

	archive := filepath.Join(t.TempDir(), "older.tgz")
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatalf("export of older store must migrate and succeed: %v", err)
	}

	members, err := readArchiveMembers(archive)
	if err != nil {
		t.Fatal(err)
	}
	var manifest exportManifest
	if err := json.Unmarshal(members[manifestMember], &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != storeSchemaVersion {
		t.Fatalf("manifest schema version = %d, want %d", manifest.SchemaVersion, storeSchemaVersion)
	}

	target := filepath.Join(t.TempDir(), "restored")
	if err := Import(ctx, archive, target); err != nil {
		t.Fatalf("archive exported from migrated older store must import: %v", err)
	}
}

func TestImportRefusesActiveLease(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}
	s.Close()

	targetDir := t.TempDir()
	target := buildPopulatedDir(t, targetDir)
	before, err := target.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	if _, err := target.db.Exec(
		`INSERT INTO controller_lease (store_id, owner_id, mode, heartbeat_at, lease_expires_at)
VALUES ('default', 'controller-1', 'run', ?, ?)`, now, now+60000); err != nil {
		t.Fatal(err)
	}
	target.Close()

	err = Import(ctx, archive, targetDir)
	if !IsCode(err, CodeStoreBusy) {
		t.Fatalf("error = %v, want STORE_BUSY", err)
	}
	after, err := Open(filepath.Join(targetDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	digest, err := after.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if digest != before {
		t.Error("leased target store was modified by refused import")
	}
}

func TestImportAcceptsExpiredLease(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}
	s.Close()

	targetDir := t.TempDir()
	target := buildPopulatedDir(t, targetDir)
	now := time.Now().UnixMilli()
	if _, err := target.db.Exec(
		`INSERT INTO controller_lease (store_id, owner_id, mode, heartbeat_at, lease_expires_at)
VALUES ('default', 'controller-1', 'run', ?, ?)`, now-120000, now-60000); err != nil {
		t.Fatal(err)
	}
	target.Close()

	if err := Import(ctx, archive, targetDir); err != nil {
		t.Fatalf("import with expired lease failed: %v", err)
	}
}

func TestImportRejectsCorruptArchive(t *testing.T) {
	ctx := context.Background()
	archive := filepath.Join(t.TempDir(), "broken.tgz")
	if err := os.WriteFile(archive, []byte("this is not a tarball"), 0o644); err != nil {
		t.Fatal(err)
	}
	targetDir := t.TempDir()
	err := Import(ctx, archive, targetDir)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "proceed.db")); !os.IsNotExist(err) {
		t.Error("refused import wrote into the target directory")
	}
}

func writeTestArchive(t *testing.T, path string, members map[string][]byte) {
	t.Helper()
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gz := gzip.NewWriter(out)
	tw := tar.NewWriter(gz)
	for name, content := range members {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(content); err != nil {
			t.Fatal(err)
		}
	}
	tw.Close()
	gz.Close()
	out.Close()
}

func TestImportRejectsChecksumMismatch(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}
	s.Close()

	members, err := readArchiveMembers(archive)
	if err != nil {
		t.Fatal(err)
	}
	members["artifacts/aa"] = []byte("tampered payload")

	tampered := filepath.Join(t.TempDir(), "tampered.tgz")
	writeTestArchive(t, tampered, members)

	targetDir := t.TempDir()
	err = Import(ctx, tampered, targetDir)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "proceed.db")); !os.IsNotExist(err) {
		t.Error("checksum-mismatched import wrote into the target directory")
	}
}

func TestImportValidatesStagedStoreBeforeReplacingTarget(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}
	s.Close()

	targetDir := t.TempDir()
	target := buildPopulatedDir(t, targetDir)
	before, err := target.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target.Close()

	members, err := readArchiveMembers(archive)
	if err != nil {
		t.Fatal(err)
	}
	var manifest exportManifest
	if err := json.Unmarshal(members[manifestMember], &manifest); err != nil {
		t.Fatal(err)
	}
	garbage := []byte("definitely not a sqlite database, just bytes that pass the recorded checksum")
	manifest.DB.SHA256 = sha256Hex(garbage)
	manifest.DB.SizeBytes = int64(len(garbage))
	members[manifest.DB.Path] = garbage
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	members[manifestMember] = encoded

	crafted := filepath.Join(t.TempDir(), "crafted.tgz")
	writeTestArchive(t, crafted, members)

	err = Import(ctx, crafted, targetDir)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
	survivor, err := Open(filepath.Join(targetDir, "proceed.db"))
	if err != nil {
		t.Fatalf("target store damaged by refused import: %v", err)
	}
	defer survivor.Close()
	digest, err := survivor.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if digest != before {
		t.Error("target store content changed by refused import")
	}
}

func TestImportRejectsSchemaVersionDisagreement(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}
	s.Close()

	targetDir := t.TempDir()
	target := buildPopulatedDir(t, targetDir)
	before, err := target.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target.Close()

	members, err := readArchiveMembers(archive)
	if err != nil {
		t.Fatal(err)
	}
	var manifest exportManifest
	if err := json.Unmarshal(members[manifestMember], &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.SchemaVersion = storeSchemaVersion + 7
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	members[manifestMember] = encoded

	crafted := filepath.Join(t.TempDir(), "crafted.tgz")
	writeTestArchive(t, crafted, members)

	err = Import(ctx, crafted, targetDir)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
	survivor, err := Open(filepath.Join(targetDir, "proceed.db"))
	if err != nil {
		t.Fatalf("target store damaged by refused import: %v", err)
	}
	defer survivor.Close()
	digest, err := survivor.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if digest != before {
		t.Error("target store content changed by refused import")
	}
	entries, err := os.ReadDir(targetDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".import-") {
			t.Errorf("staging directory %s left behind", e.Name())
		}
	}
}

func recraftArchiveWithDB(t *testing.T, archive, outPath string, mutate func(db *sql.DB)) {
	t.Helper()
	members, err := readArchiveMembers(archive)
	if err != nil {
		t.Fatal(err)
	}
	var manifest exportManifest
	if err := json.Unmarshal(members[manifestMember], &manifest); err != nil {
		t.Fatal(err)
	}
	work := filepath.Join(t.TempDir(), "work.db")
	if err := os.WriteFile(work, members[manifest.DB.Path], 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+work+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	mutate(db)
	db.Close()
	content, err := os.ReadFile(work)
	if err != nil {
		t.Fatal(err)
	}
	members[manifest.DB.Path] = content
	manifest.DB.SHA256 = sha256Hex(content)
	manifest.DB.SizeBytes = int64(len(content))
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	members[manifestMember] = encoded
	writeTestArchive(t, outPath, members)
}

func TestImportFailedIntoFreshTargetLeavesNoTrace(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	s.Close()
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}

	garbage := filepath.Join(t.TempDir(), "garbage.tgz")
	recraftArchiveWithGarbageDB(t, archive, garbage)

	target := filepath.Join(t.TempDir(), "fresh-target")
	err := Import(ctx, garbage, target)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
	if _, statErr := os.Stat(target); !os.IsNotExist(statErr) {
		t.Errorf("failed import into a fresh target must leave no directory behind (stat err = %v)", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(target, dirLockName)); !os.IsNotExist(statErr) {
		t.Errorf("failed import into a fresh target must leave no lock file behind (stat err = %v)", statErr)
	}
}

func recraftArchiveWithGarbageDB(t *testing.T, archive, outPath string) {
	t.Helper()
	members, err := readArchiveMembers(archive)
	if err != nil {
		t.Fatal(err)
	}
	var manifest exportManifest
	if err := json.Unmarshal(members[manifestMember], &manifest); err != nil {
		t.Fatal(err)
	}
	content := []byte("this is not a sqlite database at all")
	members[manifest.DB.Path] = content
	manifest.DB.SHA256 = sha256Hex(content)
	manifest.DB.SizeBytes = int64(len(content))
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	members[manifestMember] = encoded
	writeTestArchive(t, outPath, members)
}

func TestImportRejectsSymlinkedTargetParent(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	s.Close()
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}

	outside := t.TempDir()
	targetDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(targetDir, "artifacts-parent"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(targetDir, "artifacts")); err != nil {
		t.Fatal(err)
	}

	before, err := snapshotDirExcluding(targetDir, dirLockName)
	if err != nil {
		t.Fatal(err)
	}
	outsideBefore, err := snapshotDir(outside)
	if err != nil {
		t.Fatal(err)
	}

	err = Import(ctx, archive, targetDir)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
	after, err := snapshotDirExcluding(targetDir, dirLockName)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Error("target state changed by refused symlinked import")
	}
	outsideAfter, err := snapshotDir(outside)
	if err != nil {
		t.Fatal(err)
	}
	if outsideAfter != outsideBefore {
		t.Error("import wrote outside dataDir through the symlink")
	}
}

func snapshotDirExcluding(dir, skip string) (string, error) {
	var names []string
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		if rel == skip || strings.HasPrefix(rel, skip+string(filepath.Separator)) {
			if info.IsDir() && rel != skip {
				return filepath.SkipDir
			}
			return nil
		}
		names = append(names, rel)
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(names)
	return strings.Join(names, "\n"), nil
}

func snapshotDir(dir string) (string, error) {
	var names []string
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		names = append(names, rel)
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(names)
	return strings.Join(names, "\n"), nil
}

func TestExportRefusesDivergentSource(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	if _, err := s.db.Exec("UPDATE run_node SET attempt_count = 47 WHERE rowid = 1"); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	err := Export(ctx, sourceDir, archive)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
	if _, err := os.Stat(archive); !os.IsNotExist(err) {
		t.Error("divergent export produced an archive file")
	}
	if _, err := os.Stat(archive + ".snapshot"); !os.IsNotExist(err) {
		t.Error("divergent export left a snapshot file behind")
	}
	s.Close()
}

func TestExportWaitsForDataDirLock(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	defer s.Close()

	release, err := acquireDirLock(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	done := make(chan error, 1)
	go func() {
		done <- Export(ctx, sourceDir, archive)
	}()
	select {
	case err := <-done:
		t.Fatalf("export completed while the data dir lock was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "artifacts", "aa"), []byte("mutated after lock"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := release.release(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("export never completed after the lock was released")
	}
	content, err := os.ReadFile(filepath.Join(sourceDir, "artifacts", "aa"))
	if err != nil {
		t.Fatal(err)
	}
	if string(content) != "mutated after lock" {
		t.Fatalf("artifact mutated under export: %q", content)
	}
	members, err := readArchiveMembers(archive)
	if err != nil {
		t.Fatal(err)
	}
	var manifest exportManifest
	if err := json.Unmarshal(members[manifestMember], &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Artifacts) != 1 || manifest.Artifacts[0].SHA256 != sha256Hex([]byte("mutated after lock")) {
		t.Errorf("archive captured a stale artifact state: %+v", manifest.Artifacts)
	}
	if string(members["artifacts/aa"]) != "mutated after lock" {
		t.Error("archived artifact content does not match the post-lock state")
	}
}

func TestImportRefusesDivergentProjectionArchive(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}
	s.Close()

	targetDir := t.TempDir()
	target := buildPopulatedDir(t, targetDir)
	before, err := target.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target.Close()

	crafted := filepath.Join(t.TempDir(), "divergent.tgz")
	recraftArchiveWithDB(t, archive, crafted, func(db *sql.DB) {
		if _, err := db.Exec("UPDATE run_node SET attempt_count = 99 WHERE rowid = 1"); err != nil {
			t.Fatal(err)
		}
	})

	err = Import(ctx, crafted, targetDir)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
	survivor, err := Open(filepath.Join(targetDir, "proceed.db"))
	if err != nil {
		t.Fatalf("target store damaged by refused import: %v", err)
	}
	defer survivor.Close()
	digest, err := survivor.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if digest != before {
		t.Error("target store content changed by refused import")
	}
}

func TestImportRefusesIncompleteSchemaArchive(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}
	s.Close()

	targetDir := t.TempDir()
	target := buildPopulatedDir(t, targetDir)
	before, err := target.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target.Close()

	crafted := filepath.Join(t.TempDir(), "incomplete.tgz")
	recraftArchiveWithDB(t, archive, crafted, func(db *sql.DB) {
		if _, err := db.Exec("DROP TABLE causal_link"); err != nil {
			t.Fatal(err)
		}
	})

	err = Import(ctx, crafted, targetDir)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
	survivor, err := Open(filepath.Join(targetDir, "proceed.db"))
	if err != nil {
		t.Fatalf("target store damaged by refused import: %v", err)
	}
	defer survivor.Close()
	digest, err := survivor.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if digest != before {
		t.Error("target store content changed by refused import")
	}
}

func TestDataDirLockMutualExclusion(t *testing.T) {
	dir := t.TempDir()
	release, err := acquireDirLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	blocked := make(chan error, 1)
	go func() {
		blocked <- withDataDirLock(dir, func() error {
			close(entered)
			return nil
		})
	}()
	select {
	case <-entered:
		t.Fatal("second lock holder entered while the first still holds the lock")
	case <-time.After(100 * time.Millisecond):
	}
	if err := release.release(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-blocked:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second lock holder never acquired after release")
	}
	select {
	case <-entered:
	default:
		t.Fatal("second lock holder did not run after acquiring")
	}
}

func TestImportWaitsForDataDirLock(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}
	s.Close()

	targetDir := filepath.Join(t.TempDir(), "data")
	release, err := acquireDirLock(targetDir)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- Import(ctx, archive, targetDir)
	}()
	select {
	case err := <-done:
		t.Fatalf("import completed while the data dir lock was held: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := release.release(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("import never completed after the lock was released")
	}
	if _, err := os.Stat(filepath.Join(targetDir, "proceed.db")); err != nil {
		t.Fatal(err)
	}
}

func TestImportCreatesMissingTargetDirectory(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	before, err := s.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}
	s.Close()

	targetDir := filepath.Join(t.TempDir(), "fresh", "nested", "data")
	if err := Import(ctx, archive, targetDir); err != nil {
		t.Fatalf("import into fresh data dir failed: %v", err)
	}
	restored, err := Open(filepath.Join(targetDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	digest, err := restored.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if digest != before {
		t.Errorf("restored digest %s != source %s", digest, before)
	}
}

func TestExportRejectsSymlinkArtifacts(t *testing.T) {
	ctx := context.Background()
	outside := filepath.Join(t.TempDir(), "secret.txt")
	secret := "contents of a file outside the data dir"
	if err := os.WriteFile(outside, []byte(secret), 0o644); err != nil {
		t.Fatal(err)
	}
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	s.Close()
	if err := os.Symlink(outside, filepath.Join(sourceDir, "artifacts", "leak")); err != nil {
		t.Fatal(err)
	}

	archive := filepath.Join(t.TempDir(), "backup.tgz")
	err := Export(ctx, sourceDir, archive)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
	if _, err := os.Stat(archive); !os.IsNotExist(err) {
		t.Fatal("export with a symlinked artifact produced an archive")
	}
	matches, err := filepath.Glob(filepath.Join(sourceDir, ".export-*"))
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range matches {
		content, rerr := os.ReadFile(m)
		if rerr == nil && strings.Contains(string(content), secret) {
			t.Errorf("temporary file %s contains outside content", m)
		}
	}
}

func TestExportRejectsOutputPathCollisions(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	before, err := s.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	for _, collision := range []string{
		filepath.Join(sourceDir, "proceed.db"),
		filepath.Join(sourceDir, "proceed.db-wal"),
		filepath.Join(sourceDir, "proceed.lock"),
		filepath.Join(sourceDir, "artifacts", "out.tgz"),
	} {
		if err := os.MkdirAll(filepath.Dir(collision), 0o755); err != nil {
			t.Fatal(err)
		}
		err := Export(ctx, sourceDir, collision)
		if !IsCode(err, CodeGraphInvalid) {
			t.Errorf("output %s: error = %v, want GRAPH_INVALID", collision, err)
		}
		if collision == filepath.Join(sourceDir, "artifacts", "out.tgz") {
			if _, serr := os.Stat(collision); !os.IsNotExist(serr) {
				t.Errorf("output %s: collision path was created", collision)
			}
		}
	}

	survivor, err := Open(filepath.Join(sourceDir, "proceed.db"))
	if err != nil {
		t.Fatalf("live store damaged by refused exports: %v", err)
	}
	defer survivor.Close()
	digest, err := survivor.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if digest != before {
		t.Error("live store content changed by refused exports")
	}
}

func TestImportWaitsForActiveWriter(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	sourceDigest, err := s.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}

	targetDir := t.TempDir()
	target := buildPopulatedDir(t, targetDir)
	target.Close()

	writer, err := Open(filepath.Join(targetDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	inTx := make(chan struct{})
	releaseTx := make(chan struct{})
	go func() {
		_ = writer.withTx(ctx, func(tx *sql.Tx) error {
			close(inTx)
			<-releaseTx
			return nil
		})
		writer.Close()
	}()
	<-inTx

	done := make(chan error, 1)
	go func() {
		done <- Import(ctx, archive, targetDir)
	}()
	select {
	case err := <-done:
		t.Fatalf("import completed while a writer held the store lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseTx)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("import never completed after the writer released the store lock")
	}

	restored, err := Open(filepath.Join(targetDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	digest, err := restored.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if digest != sourceDigest {
		t.Errorf("restored digest %s != source %s", digest, sourceDigest)
	}
}

func TestAppendWaitsForImport(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}

	targetDir := t.TempDir()
	target := buildPopulatedDir(t, targetDir)
	target.Close()

	release, err := acquireDirLock(targetDir)
	if err != nil {
		t.Fatal(err)
	}
	appended := make(chan error, 1)
	go func() {
		w, err := Open(filepath.Join(targetDir, "proceed.db"))
		if err != nil {
			appended <- err
			return
		}
		defer w.Close()
		events, err := w.Events(ctx, "01HZZZZZZZZZZZZZZZZZZZZZZZ")
		if err != nil {
			appended <- err
			return
		}
		_ = events
		_, err = w.Append(ctx, Event{
			RunID:         "01HZZZZZZZZZZZZZZZZZZZZZZZ",
			Sequence:      99,
			SchemaVersion: eventSchemaVersion,
			Type:          "checkpoint",
			OccurredAt:    time.Now().UnixMilli(),
			ActorType:     "controller",
			ActorID:       "test",
			Payload:       `{}`,
		})
		appended <- err
	}()
	select {
	case err := <-appended:
		t.Fatalf("append resolved while import held the exclusive lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := release.release(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-appended:
	case <-time.After(10 * time.Second):
		t.Fatal("append never completed after the exclusive lock was released")
	}
}

func TestImportRejectsReservedArtifactNames(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}

	targetDir := t.TempDir()
	target := buildPopulatedDir(t, targetDir)
	before, err := target.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target.Close()

	members, err := readArchiveMembers(archive)
	if err != nil {
		t.Fatal(err)
	}
	var manifest exportManifest
	if err := json.Unmarshal(members[manifestMember], &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Artifacts = append(manifest.Artifacts, archiveEntry{
		Path:      "proceed.db",
		SHA256:    sha256Hex(members[manifest.DB.Path]),
		SizeBytes: int64(len(members[manifest.DB.Path])),
	})
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	members[manifestMember] = encoded
	crafted := filepath.Join(t.TempDir(), "reserved.tgz")
	writeTestArchive(t, crafted, members)

	err = Import(ctx, crafted, targetDir)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
	survivor, err := Open(filepath.Join(targetDir, "proceed.db"))
	if err != nil {
		t.Fatalf("target store damaged by refused import: %v", err)
	}
	defer survivor.Close()
	digest, err := survivor.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if digest != before {
		t.Error("target store content changed by refused import")
	}
}

func TestExportRejectsSymlinkedDataDirCollision(t *testing.T) {
	ctx := context.Background()
	realDir := t.TempDir()
	s := buildPopulatedDir(t, realDir)
	before, err := s.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.Close()

	linkDir := filepath.Join(t.TempDir(), "linked-data")
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Fatal(err)
	}

	err = Export(ctx, linkDir, filepath.Join(realDir, "proceed.db"))
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("export via symlinked data dir to the real db: error = %v, want GRAPH_INVALID", err)
	}

	survivor, err := Open(filepath.Join(realDir, "proceed.db"))
	if err != nil {
		t.Fatalf("live store damaged by refused export: %v", err)
	}
	defer survivor.Close()
	digest, err := survivor.ProjectionDigest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if digest != before {
		t.Error("live store content changed by refused export")
	}

	archive := filepath.Join(t.TempDir(), "ok.tgz")
	if err := Export(ctx, linkDir, archive); err != nil {
		t.Fatalf("legitimate export through symlinked data dir failed: %v", err)
	}
}

func TestImportRejectsSidecarAndTraversalArtifacts(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	s.Close()
	if err := Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}

	for _, bad := range []string{
		"proceed.db-wal",
		"proceed.db-shm",
		"proceed.db-journal",
		"proceed.db",
		"proceed.lock",
		"manifest.json",
		"artifacts/../proceed.db",
		"../outside.txt",
		"/abs/path.txt",
		"artifacts/..",
		"",
	} {
		targetDir := t.TempDir()
		target := buildPopulatedDir(t, targetDir)
		before, err := target.ProjectionDigest(ctx)
		if err != nil {
			t.Fatal(err)
		}
		target.Close()

		members, err := readArchiveMembers(archive)
		if err != nil {
			t.Fatal(err)
		}
		var manifest exportManifest
		if err := json.Unmarshal(members[manifestMember], &manifest); err != nil {
			t.Fatal(err)
		}
		manifest.Artifacts = append(manifest.Artifacts, archiveEntry{
			Path:      bad,
			SHA256:    sha256Hex(members[manifest.DB.Path]),
			SizeBytes: int64(len(members[manifest.DB.Path])),
		})
		encoded, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		members[bad] = members[manifest.DB.Path]
		members[manifestMember] = encoded
		crafted := filepath.Join(t.TempDir(), "crafted.tgz")
		writeTestArchive(t, crafted, members)

		err = Import(ctx, crafted, targetDir)
		if !IsCode(err, CodeGraphInvalid) {
			t.Errorf("artifact path %q: error = %v, want GRAPH_INVALID", bad, err)
		}
		survivor, serr := Open(filepath.Join(targetDir, "proceed.db"))
		if serr != nil {
			t.Fatalf("artifact path %q: target store damaged: %v", bad, serr)
		}
		digest, derr := survivor.ProjectionDigest(ctx)
		survivor.Close()
		if derr != nil {
			t.Fatal(derr)
		}
		if digest != before {
			t.Errorf("artifact path %q: target store content changed", bad)
		}
	}
}
