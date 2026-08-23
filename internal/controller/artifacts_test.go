package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"proceed/internal/executor"
)

func TestArtifactPublisherRejectsConflictingContent(t *testing.T) {
	dataDir := t.TempDir()
	content := []byte("expected")
	digest := sha256.Sum256(content)
	path := filepath.Join(dataDir, "artifacts", hex.EncodeToString(digest[:]))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("tampered"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := (&artifactPublisher{dataDir: dataDir}).Publish(context.Background(), executor.ArtifactInput{
		Name:      "stdout",
		MediaType: "text/plain",
		Content:   content,
	})
	if err == nil {
		t.Fatal("Publish() error = nil, want hash conflict")
	}
}
