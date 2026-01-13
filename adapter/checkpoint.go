package adapter

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// CheckpointStore File-based checkpoint store emulating the EMMA device scenario detailed in the ADR
type CheckpointStore struct {
	mu   sync.Mutex
	path string
}

type checkpointFile struct {
	LastCursor int64 `json:"lastCursor"`
}

func NewCheckpointStore(path string) *CheckpointStore {
	return &CheckpointStore{path: path}
}

func (c *CheckpointStore) Load() (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	b, err := os.ReadFile(c.path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("read checkpoint: %w", err)
	}

	var f checkpointFile
	if err := json.Unmarshal(b, &f); err != nil {
		return 0, fmt.Errorf("unmarshal checkpoint: %w", err)
	}
	return f.LastCursor, nil
}

func (c *CheckpointStore) Save(lastCursor int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	f := checkpointFile{LastCursor: lastCursor}
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	if err := os.WriteFile(c.path, b, 0o644); err != nil {
		return fmt.Errorf("write checkpoint: %w", err)
	}
	return nil
}
