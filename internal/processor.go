package internal

import (
	"fmt"
	"os"
)

type Config struct {
	InputPath  string
	OutputPath string
	ChunkSize  int
	TempPath   string
}

type fileData struct {
	Name string
	Num  int
}

func ProcessFiles(cfg Config) error {
	if err := os.MkdirAll(cfg.TempPath, 0755); err != nil {
		return fmt.Errorf("could not create temp directory: %w", err)
	}
	defer os.RemoveAll(cfg.TempPath)

	if err := countFrequencies(cfg); err != nil {
		return fmt.Errorf("frequency counting phase failed: %w", err)
	}

	if err := sortAndMerge(cfg); err != nil {
		return fmt.Errorf("sorting and merging phase failed: %w", err)
	}

	return nil
}
