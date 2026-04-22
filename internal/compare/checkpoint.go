package compare

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type compareCheckpoint struct {
	Version    int                        `json:"version"`
	ConfigHash string                     `json:"config_hash"`
	UpdatedAt  string                     `json:"updated_at"`
	Tables     map[string]tableCheckpoint `json:"tables"`
}

type tableCheckpoint struct {
	LastKey   string `json:"last_key"`
	Done      bool   `json:"done"`
	KeyColumn string `json:"key_column"`
	KeyType   string `json:"key_type"`
}

type checkpointManager struct {
	mu       sync.Mutex
	filePath string
	cp       compareCheckpoint
}

func newCheckpointManager(filePath string) (*checkpointManager, error) {
	m := &checkpointManager{filePath: filePath}
	if filePath == "" {
		return m, nil
	}
	b, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			m.cp = compareCheckpoint{Version: 1, Tables: map[string]tableCheckpoint{}}
			return m, nil
		}
		return nil, err
	}
	if len(b) == 0 {
		m.cp = compareCheckpoint{Version: 1, Tables: map[string]tableCheckpoint{}}
		return m, nil
	}
	if err := json.Unmarshal(b, &m.cp); err != nil {
		m.cp = compareCheckpoint{Version: 1, Tables: map[string]tableCheckpoint{}}
		return m, nil
	}
	if m.cp.Tables == nil {
		m.cp.Tables = map[string]tableCheckpoint{}
	}
	if m.cp.Version == 0 {
		m.cp.Version = 1
	}
	return m, nil
}

func hashConfig(v interface{}) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func (m *checkpointManager) init(configHash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.filePath == "" {
		return nil
	}
	if m.cp.ConfigHash != configHash {
		m.cp = compareCheckpoint{
			Version:    1,
			ConfigHash: configHash,
			UpdatedAt:  time.Now().UTC().Format(time.RFC3339Nano),
			Tables:     map[string]tableCheckpoint{},
		}
		return m.flushLocked()
	}
	return nil
}

func (m *checkpointManager) get(table string) (tableCheckpoint, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cp.Tables == nil {
		m.cp.Tables = map[string]tableCheckpoint{}
	}
	v, ok := m.cp.Tables[table]
	return v, ok
}

func (m *checkpointManager) set(table string, ck tableCheckpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.filePath == "" {
		return nil
	}
	if m.cp.Tables == nil {
		m.cp.Tables = map[string]tableCheckpoint{}
	}
	m.cp.Tables[table] = ck
	m.cp.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	return m.flushLocked()
}

func (m *checkpointManager) flushLocked() error {
	dir := filepath.Dir(m.filePath)
	base := filepath.Base(m.filePath)
	tmp := filepath.Join(dir, "."+base+".tmp")

	b, err := json.MarshalIndent(m.cp, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.filePath)
}
