package internal

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
)

func countFrequencies(cfg Config) error {
	file, err := os.Open(cfg.InputPath)
	if err != nil {
		return fmt.Errorf("error opening input file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	chunk := make([]string, 0, cfg.ChunkSize)

	for scanner.Scan() {
		chunk = append(chunk, scanner.Text())
		if len(chunk) == cfg.ChunkSize {
			if err := processChunk(chunk, cfg.TempPath); err != nil {
				return err
			}
			chunk = make([]string, 0, cfg.ChunkSize)
		}
	}

	if len(chunk) > 0 {
		if err := processChunk(chunk, cfg.TempPath); err != nil {
			return err
		}
	}

	return scanner.Err()
}

func processChunk(chunk []string, tempPath string) error {
	for _, line := range chunk {
		if line == "" {
			continue
		}

		fileName := tempPath + "/" + line
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", fileName, err)
		}

		scanner := bufio.NewScanner(file)
		value := 1
		if scanner.Scan() {
			value, err = strconv.Atoi(scanner.Text())
			if err != nil {
				file.Close()
				return fmt.Errorf("failed to parse value in %s: %w", fileName, err)
			}
			value++

			if _, err = file.Seek(0, 0); err != nil {
				file.Close()
				return fmt.Errorf("failed to seek file %s: %w", fileName, err)
			}
			if err = file.Truncate(0); err != nil {
				file.Close()
				return fmt.Errorf("failed to truncate file %s: %w", fileName, err)
			}
		}

		if _, err := file.WriteString(strconv.Itoa(value)); err != nil {
			file.Close()
			return fmt.Errorf("failed to write to file %s: %w", fileName, err)
		}

		file.Close()
	}
	return nil
}
