package store

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
	if err := s.Export(ctx, sourceDir, archive); err != nil {
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
	if err := s.Export(ctx, sourceDir, archive); err != nil {
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

func TestImportRefusesActiveLease(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	if err := s.Export(ctx, sourceDir, archive); err != nil {
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
	if err := s.Export(ctx, sourceDir, archive); err != nil {
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

func TestImportRejectsChecksumMismatch(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	s := buildPopulatedDir(t, sourceDir)
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	if err := s.Export(ctx, sourceDir, archive); err != nil {
		t.Fatal(err)
	}
	s.Close()

	members, err := readArchiveMembers(archive)
	if err != nil {
		t.Fatal(err)
	}
	members["artifacts/aa"] = []byte("tampered payload")

	tampered := filepath.Join(t.TempDir(), "tampered.tgz")
	out, err := os.Create(tampered)
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

	targetDir := t.TempDir()
	err = Import(ctx, tampered, targetDir)
	if !IsCode(err, CodeGraphInvalid) {
		t.Fatalf("error = %v, want GRAPH_INVALID", err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "proceed.db")); !os.IsNotExist(err) {
		t.Error("checksum-mismatched import wrote into the target directory")
	}
}
