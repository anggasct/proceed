package main

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"proceed/internal/compiler"
	"proceed/internal/store"
)

func buildCLIStore(t *testing.T, dataDir string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dataDir, "artifacts"), 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	src, err := os.ReadFile("../../internal/compiler/testdata/customer-research.yaml")
	if err != nil {
		t.Fatal(err)
	}
	doc, err := compiler.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiler.Validate(doc); err != nil {
		t.Fatal(err)
	}
	frozen, err := s.FreezeDefinition(context.Background(), "customer-research.yaml", src, doc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateRun(context.Background(), frozen.GraphVersionID); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "artifacts", "note"), []byte("cli artifact"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestStoreExportImportCLI(t *testing.T) {
	sourceDir := t.TempDir()
	buildCLIStore(t, sourceDir)
	before := cliDigest(t, sourceDir)

	archive := filepath.Join(t.TempDir(), "backup.tgz")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"store", "export", "--data-dir", sourceDir, "--output", archive}, &stdout, &stderr); code != 0 {
		t.Fatalf("export exit = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "exported") {
		t.Errorf("stdout = %q", stdout.String())
	}

	targetDir := t.TempDir()
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"store", "import", "--input", archive, "--data-dir", targetDir}, &stdout, &stderr); code != 0 {
		t.Fatalf("import exit = %d, stderr = %q", code, stderr.String())
	}
	if after := cliDigest(t, targetDir); after != before {
		t.Errorf("restored digest %s != source %s", after, before)
	}
	got, err := os.ReadFile(filepath.Join(targetDir, "artifacts", "note"))
	if err != nil || string(got) != "cli artifact" {
		t.Errorf("restored artifact = %q (%v)", got, err)
	}
}

func cliDigest(t *testing.T, dataDir string) string {
	t.Helper()
	s, err := store.Open(filepath.Join(dataDir, "proceed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	digest, err := s.ProjectionDigest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func TestStoreImportLeasedTargetCLI(t *testing.T) {
	sourceDir := t.TempDir()
	buildCLIStore(t, sourceDir)
	archive := filepath.Join(t.TempDir(), "backup.tgz")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"store", "export", "--data-dir", sourceDir, "--output", archive}, &stdout, &stderr); code != 0 {
		t.Fatalf("export exit = %d", code)
	}

	targetDir := t.TempDir()
	buildCLIStore(t, targetDir)
	db, err := sql.Open("sqlite", "file:"+filepath.Join(targetDir, "proceed.db")+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	if _, err := db.Exec(
		`INSERT INTO controller_lease (store_id, owner_id, mode, heartbeat_at, lease_expires_at)
VALUES ('default', 'controller-1', 'run', ?, ?)`, now, now+60000); err != nil {
		t.Fatal(err)
	}
	db.Close()

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"store", "import", "--input", archive, "--data-dir", targetDir}, &stdout, &stderr); code != 1 {
		t.Fatalf("import exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "STORE_BUSY") {
		t.Errorf("stderr = %q, want STORE_BUSY", stderr.String())
	}
}

func TestStoreUsageErrors(t *testing.T) {
	cases := [][]string{
		{"store"},
		{"store", "frobnicate"},
		{"store", "export"},
		{"store", "export", "--data-dir", t.TempDir()},
		{"store", "import", "--data-dir", t.TempDir()},
		{"store", "export", "--data-dir"},
	}
	for _, args := range cases {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != 2 {
			t.Errorf("%v: exit = %d, want 2 (stderr %q)", args, code, stderr.String())
		}
	}
}
