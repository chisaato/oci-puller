package manager

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestUpdateIndex(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "oci-index-test")
	defer os.RemoveAll(tmpDir)

	mgr := NewManager(tmpDir)
	defer mgr.Close()

	// 1. First Update (Create)
	tag := "latest"
	digest := "sha256:1234567890abcdef"
	size := int64(123)
	mediaType := "application/vnd.oci.image.manifest.v1+json"

	if err := mgr.UpdateIndex(tag, digest, size, mediaType); err != nil {
		t.Fatalf("UpdateIndex failed: %v", err)
	}

	// Verify File Content
	indexPath := filepath.Join(tmpDir, "index.json")
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("Failed to read index.json: %v", err)
	}

	var index OCIIndex
	if err := json.Unmarshal(data, &index); err != nil {
		t.Fatalf("Invalid JSON: %v", err)
	}

	if len(index.Manifests) != 1 {
		t.Errorf("Expected 1 manifest, got %d", len(index.Manifests))
	}
	if index.Manifests[0].Annotations["org.opencontainers.image.ref.name"] != tag {
		t.Errorf("Tag mismatch")
	}

	// 2. Second Update (Update existing tag)
	newDigest := "sha256:abcdef1234567890"
	if err := mgr.UpdateIndex(tag, newDigest, size, mediaType); err != nil {
		t.Fatalf("Second UpdateIndex failed: %v", err)
	}

	data, _ = os.ReadFile(indexPath)
	json.Unmarshal(data, &index)

	if len(index.Manifests) != 1 {
		t.Errorf("Expected still 1 manifest after update, got %d", len(index.Manifests))
	}
	if index.Manifests[0].Digest != newDigest {
		t.Errorf("Digest not updated. Got %s, want %s", index.Manifests[0].Digest, newDigest)
	}

	// 3. Third Update (New Tag)
	if err := mgr.UpdateIndex("v1.0", digest, size, mediaType); err != nil {
		t.Fatalf("Third UpdateIndex failed: %v", err)
	}

	data, _ = os.ReadFile(indexPath)
	json.Unmarshal(data, &index)
	if len(index.Manifests) != 2 {
		t.Errorf("Expected 2 manifests, got %d", len(index.Manifests))
	}
}

func TestSaveBytesWithMissingDir(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "oci-test-missing-dir")
	defer os.RemoveAll(tmpDir)

	mgr := NewManager(tmpDir)
	defer mgr.Close()

	// Simulate external deletion of cache dir (or subdirs)
	// We specifically delete the temp and blobs directories
	if err := os.RemoveAll(filepath.Join(tmpDir, "temp")); err != nil {
		t.Fatalf("Failed to remove temp dir: %v", err)
	}
	if err := os.RemoveAll(filepath.Join(tmpDir, "blobs")); err != nil {
		t.Fatalf("Failed to remove blobs dir: %v", err)
	}

	data := []byte("some manifest content")
	digest, _, err := mgr.SaveBytes(data)
	if err != nil {
		t.Fatalf("SaveBytes failed after directory deletion: %v", err)
	}

	// Verify file was written to final path
	finalPath := mgr.getFinalPath(digest)
	if _, err := os.Stat(finalPath); err != nil {
		t.Errorf("Final file missing at %s: %v", finalPath, err)
	}
}

func TestUpdateIndexWithMissingDir(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "oci-index-missing-dir")
	defer os.RemoveAll(tmpDir)

	mgr := NewManager(tmpDir)
	defer mgr.Close()

	// Delete entire cache dir
	if err := os.RemoveAll(tmpDir); err != nil {
		t.Fatalf("Failed to remove tmpDir: %v", err)
	}

	// UpdateIndex should attempt to recreate
	err := mgr.UpdateIndex("tag", "sha256:digest", 123, "media/type")
	// Note: UpdateIndex uses os.WriteFile directly.
	// If the fix is correct, it should verify/create dir.
	// Wait, if I remove tmpDir (m.cacheDir), and UpdateIndex does MkdirAll(filepath.Dir(indexPath)),
	// indexPath is Join(m.cacheDir, "index.json"). Dir is m.cacheDir.
	// So it should recreate m.cacheDir.

	if err != nil {
		t.Fatalf("UpdateIndex failed after directory deletion: %v", err)
	}

	indexPath := filepath.Join(tmpDir, "index.json")
	if _, err := os.Stat(indexPath); err != nil {
		t.Errorf("index.json missing: %v", err)
	}
}
