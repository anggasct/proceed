package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/oklog/ulid/v2"

	"proceed/internal/executor"
)

type artifactPublisher struct {
	dataDir string
}

func (p *artifactPublisher) Publish(ctx context.Context, input executor.ArtifactInput) (executor.ArtifactRef, error) {
	if err := ctx.Err(); err != nil {
		return executor.ArtifactRef{}, err
	}
	if input.Name == "" || input.MediaType == "" {
		return executor.ArtifactRef{}, fmt.Errorf("artifact name and media type are required")
	}
	hash := sha256.Sum256(input.Content)
	contentHash := hex.EncodeToString(hash[:])
	relativePath := filepath.ToSlash(filepath.Join("artifacts", contentHash))
	artifactDir := filepath.Join(p.dataDir, "artifacts")
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		return executor.ArtifactRef{}, err
	}
	absolutePath := filepath.Join(p.dataDir, filepath.FromSlash(relativePath))
	info, err := os.Lstat(absolutePath)
	switch {
	case os.IsNotExist(err):
		tmp, err := os.CreateTemp(artifactDir, ".artifact-")
		if err != nil {
			return executor.ArtifactRef{}, err
		}
		tmpName := tmp.Name()
		defer os.Remove(tmpName)
		if _, err := tmp.Write(input.Content); err != nil {
			_ = tmp.Close()
			return executor.ArtifactRef{}, err
		}
		if err := tmp.Close(); err != nil {
			return executor.ArtifactRef{}, err
		}
		if err := os.Rename(tmpName, absolutePath); err != nil && !os.IsExist(err) {
			return executor.ArtifactRef{}, err
		}
	case err != nil:
		return executor.ArtifactRef{}, err
	case info.IsDir() || info.Mode()&os.ModeSymlink != 0:
		return executor.ArtifactRef{}, fmt.Errorf("artifact path is not a regular file")
	}
	valid, err := artifactMatches(absolutePath, contentHash, int64(len(input.Content)))
	if err != nil {
		return executor.ArtifactRef{}, err
	}
	if !valid {
		return executor.ArtifactRef{}, fmt.Errorf("artifact content hash conflict")
	}
	return executor.ArtifactRef{
		ID:          ulid.Make().String(),
		Name:        input.Name,
		Path:        relativePath,
		ContentHash: contentHash,
		MediaType:   input.MediaType,
		SizeBytes:   int64(len(input.Content)),
		Truncated:   input.Truncated,
	}, nil
}

func artifactMatches(path, expectedHash string, expectedSize int64) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return false, err
	}
	return size == expectedSize && hex.EncodeToString(h.Sum(nil)) == expectedHash, nil
}
