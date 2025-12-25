package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

var (
	bucketBlobs = []byte("blobs")
	bucketLRU   = []byte("lru_index")
)

type BlobMeta struct {
	Digest     string `json:"digest"`
	Size       int64  `json:"size"`
	Path       string `json:"path"`
	LastAccess int64  `json:"last_access"`
}

type Store struct {
	db *bbolt.DB
	mu sync.RWMutex
}

func NewStore(path string) (*Store, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("无法打开数据库: %w", err)
	}

	// 初始化存储桶（Buckets）
	err = db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketBlobs); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketLRU); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

// RecordAccess 记录新的 Blob 或更新现有 Blob 的访问时间。
// 这是“Put”和“Touch”的组合逻辑。
func (s *Store) RecordAccess(digest string, size int64, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Update(func(tx *bbolt.Tx) error {
		bBlobs := tx.Bucket(bucketBlobs)
		bLRU := tx.Bucket(bucketLRU)

		now := time.Now().UnixNano()

		// 1. 检查是否存在
		existingBytes := bBlobs.Get([]byte(digest))
		if existingBytes != nil {
			var oldMeta BlobMeta
			if err := json.Unmarshal(existingBytes, &oldMeta); err == nil {
				// 移除旧索引
				oldKey := makeLRUKey(oldMeta.LastAccess, digest)
				bLRU.Delete(oldKey)
			}
		}

		// 2. 创建新元数据
		newMeta := BlobMeta{
			Digest:     digest,
			Size:       size,
			Path:       path,
			LastAccess: now,
		}
		newMetaBytes, _ := json.Marshal(newMeta)

		// 3. 保存到 blobs 存储桶
		if err := bBlobs.Put([]byte(digest), newMetaBytes); err != nil {
			return err
		}

		// 4. 保存到 lru_index 存储桶
		newKey := makeLRUKey(now, digest)
		return bLRU.Put(newKey, []byte(digest))
	})
}

// GetAndTouch 获取 Blob 元数据。如果需要，也可以异步更新 LastAccess？
// 为了实现严格的 LRU，我们应该在这里更新访问时间。
func (s *Store) GetAndTouch(digest string) (*BlobMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var meta BlobMeta
	err := s.db.Update(func(tx *bbolt.Tx) error {
		bBlobs := tx.Bucket(bucketBlobs)
		bLRU := tx.Bucket(bucketLRU)

		bytes := bBlobs.Get([]byte(digest))
		if bytes == nil {
			return fmt.Errorf("未找到")
		}

		if err := json.Unmarshal(bytes, &meta); err != nil {
			return err
		}

		// 更新访问时间 (Touch)
		oldKey := makeLRUKey(meta.LastAccess, digest)
		bLRU.Delete(oldKey)

		now := time.Now().UnixNano()
		meta.LastAccess = now

		newBytes, _ := json.Marshal(meta)
		if err := bBlobs.Put([]byte(digest), newBytes); err != nil {
			return err
		}

		newKey := makeLRUKey(now, digest)
		return bLRU.Put(newKey, []byte(digest))
	})

	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// Prune 删除最旧的 Blob，直到缓存大小低于限制（由计数或特定查询模拟）。
// 由于我们尚未在数据库中跟踪总大小，目前实现基于数量的驱逐，或者仅暴露 "EvictOldest"。
// 但通常 Prune 需要知道当前的占用情况。
// 目前，我们先实现 EvictOldest(n) 来驱逐最旧的 N 项。
func (s *Store) EvictOldest(n int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var evicted []string

	err := s.db.Update(func(tx *bbolt.Tx) error {
		bBlobs := tx.Bucket(bucketBlobs)
		bLRU := tx.Bucket(bucketLRU)
		c := bLRU.Cursor()

		count := 0
		for k, v := c.First(); k != nil && count < n; k, v = c.Next() {
			digest := string(v)

			// 获取元数据以返回路径（以便调用者可以删除文件）
			// 等等，调用者需要路径。
			// 假设调用者只需要 Digest，或者我们在这里删除文件？
			// 最好返回 Digest 并由调用者负责删除文件。

			// 从数据库中移除
			if err := bLRU.Delete(k); err != nil {
				return err
			}
			if err := bBlobs.Delete(v); err != nil {
				return err
			}
			evicted = append(evicted, digest)
			count++
		}
		return nil
	})
	return evicted, err
}

// GetPath 获取路径但不更新访问时间（用于检查是否存在）
func (s *Store) GetPath(digest string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var path string
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketBlobs)
		v := b.Get([]byte(digest))
		if v == nil {
			return fmt.Errorf("未找到")
		}
		var meta BlobMeta
		if err := json.Unmarshal(v, &meta); err != nil {
			return err
		}
		path = meta.Path
		return nil
	})
	return path, err
}

func makeLRUKey(ts int64, digest string) []byte {
	// 键 (Key): <时间戳_8字节><摘要_digest>
	k := make([]byte, 8+len(digest))
	binary.BigEndian.PutUint64(k[0:8], uint64(ts))
	copy(k[8:], digest)
	return k
}
