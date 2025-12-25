package manager

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"oci-puller/pkg/logger"
)

// OCI 规范结构
type OCIIndex struct {
	SchemaVersion int             `json:"schemaVersion"`
	Manifests     []OCIDescriptor `json:"manifests"`
}

type OCIDescriptor struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// UpdateIndex 更新 index.json，添加或更新 Tag 指向的 Manifest
func (m *DownloadManager) UpdateIndex(tagName string, manifestDigest string, size int64, mediaType string) error {
	// 加锁保护 index.json 读写
	m.mu.Lock()
	defer m.mu.Unlock()

	indexPath := filepath.Join(m.cacheDir, "index.json")

	// 1. 读取现有 index.json 或初始化
	var index OCIIndex
	data, err := os.ReadFile(indexPath)
	if err == nil {
		if err := json.Unmarshal(data, &index); err != nil {
			logger.S.Warnw("解析现有 index.json 失败，将重置", "error", err)
			index = OCIIndex{SchemaVersion: 2}
		}
	} else {
		if !os.IsNotExist(err) {
			return fmt.Errorf("读取 index.json 失败: %w", err)
		}
		index = OCIIndex{SchemaVersion: 2}
	}

	// 2. 查找并更新同名 Tag，或新增
	// OCI 规范允许一个 Index 指向多个 Manifest (多架构)，
	// 但在这个简单的实现中，对于同一个 Tag，我们假设它是单一架构或者直接替换？
	// Skopeo 通常希望 Tag 唯一指向一个 Manifest (或 ManifestList)。
	// 这里的策略是：如果有同名 Tag，直接替换。
	found := false
	newDesc := OCIDescriptor{
		MediaType: mediaType,
		Digest:    manifestDigest,
		Size:      size,
		Annotations: map[string]string{
			"org.opencontainers.image.ref.name": tagName,
		},
	}

	// 遍历查找替换
	// 注意：index.Manifests 可能包含无 Tag 的项（如中间层），我们只关心带 Tag 的。
	for i, desc := range index.Manifests {
		if desc.Annotations != nil && desc.Annotations["org.opencontainers.image.ref.name"] == tagName {
			// 找到同名 Tag，更新
			// 保留该位置，替换内容
			index.Manifests[i] = newDesc
			found = true
			break
		}
	}

	if !found {
		index.Manifests = append(index.Manifests, newDesc)
	}

	// 3. 回写
	if err := os.MkdirAll(filepath.Dir(indexPath), 0755); err != nil {
		return fmt.Errorf("创建索引目录失败: %w", err)
	}
	newData, err := json.MarshalIndent(index, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(indexPath, newData, 0644)
}

// SaveBytes 将数据（如 Manifest 内容）保存为 Blob，并返回 Digest 和 Size
func (m *DownloadManager) SaveBytes(data []byte) (string, int64, error) {
	// 计算 hash
	hash := sha256.Sum256(data)
	digest := "sha256:" + hex.EncodeToString(hash[:])
	size := int64(len(data))

	// 获取路径
	finalPath := m.getFinalPath(digest)

	// 如果已存在，直接返回
	if _, err := os.Stat(finalPath); err == nil {
		return digest, size, nil
	}

	// 写入文件
	// 先创建目录
	if err := os.MkdirAll(filepath.Dir(finalPath), 0755); err != nil {
		return "", 0, err
	}

	// 写入临时文件然后重命名，保证原子性
	tempPath := m.getTempPath(digest) + ".manifest"
	if err := os.MkdirAll(filepath.Dir(tempPath), 0755); err != nil {
		return "", 0, fmt.Errorf("创建临时目录失败: %w", err)
	}
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return "", 0, err
	}

	if err := os.Rename(tempPath, finalPath); err != nil {
		return "", 0, err
	}

	return digest, size, nil
}

// SaveBlobFromReader 从流读取数据保存为 Blob，返回内容、Digest 和 Size
func (m *DownloadManager) SaveBlobFromReader(r io.Reader) ([]byte, string, int64, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, "", 0, err
	}
	digest, size, err := m.SaveBytes(data)
	return data, digest, size, err
}
