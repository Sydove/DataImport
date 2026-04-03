package syncES

import (
	"DataImport/internal/pkg/utils"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const checkpointFileName = "full_load_checkpoint.json"

type Checkpoint struct {
	JobID              string    `json:"job_id"`
	SnapshotMaxID      int       `json:"snapshot_max_id"`
	LastPublishedEndID int       `json:"last_published_end_id"`
	UpdatedAt          time.Time `json:"updated_at"`
}

func checkpointFilePath() string {
	return filepath.Join(utils.GetConfigPath(), checkpointFileName)
}

func loadCheckpoint() (*Checkpoint, error) {
	data, err := os.ReadFile(checkpointFilePath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read full-load checkpoint: %w", err)
	}

	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, fmt.Errorf("unmarshal full-load checkpoint: %w", err)
	}
	if checkpoint.JobID == "" {
		return nil, fmt.Errorf("invalid full-load checkpoint: job_id is empty")
	}
	if checkpoint.SnapshotMaxID < 0 {
		return nil, fmt.Errorf("invalid full-load checkpoint: snapshot_max_id is negative")
	}
	if checkpoint.LastPublishedEndID < 0 {
		return nil, fmt.Errorf("invalid full-load checkpoint: last_published_end_id is negative")
	}
	if checkpoint.LastPublishedEndID > checkpoint.SnapshotMaxID {
		return nil, fmt.Errorf("invalid full-load checkpoint: last_published_end_id exceeds snapshot_max_id")
	}

	return &checkpoint, nil
}

func saveCheckpoint(checkpoint Checkpoint) error {
	checkpoint.UpdatedAt = time.Now().UTC()

	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal full-load checkpoint: %w", err)
	}

	if err := os.WriteFile(checkpointFilePath(), data, 0o644); err != nil {
		return fmt.Errorf("write full-load checkpoint: %w", err)
	}
	return nil
}

func deleteCheckpoint() error {
	err := os.Remove(checkpointFilePath())
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return fmt.Errorf("delete full-load checkpoint: %w", err)
}
