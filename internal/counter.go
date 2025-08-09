package internal

import (
	"bufio"
	"fmt"
	"os"
	"sort"
)

func countFrequencies(cfg Config) error {
	file, err := os.Open(cfg.InputPath)
	if err != nil {
		return fmt.Errorf("error opening input file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	wordCounts := make(map[string]int)
	chunkCounter := 0

	for scanner.Scan() {
		word := scanner.Text()
		if word == "" {
			continue
		}
		wordCounts[word]++

		if len(wordCounts) >= cfg.ChunkSize {
			if err := processChunk(wordCounts, cfg.TempPath, chunkCounter); err != nil {
				return err
			}
			wordCounts = make(map[string]int)
			chunkCounter++
		}
	}

	if len(wordCounts) > 0 {
		if err := processChunk(wordCounts, cfg.TempPath, chunkCounter); err != nil {
			return err
		}
	}

	return scanner.Err()
}

func processChunk(wordCounts map[string]int, tempPath string, chunkCounter int) error {
	fileName := fmt.Sprintf("%s/chunk_%d.txt", tempPath, chunkCounter)
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create chunk file %s: %w", fileName, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	words := make([]string, 0, len(wordCounts))
	for word := range wordCounts {
		words = append(words, word)
	}
	sort.Strings(words)

	for _, word := range words {
		line := fmt.Sprintf("%s\t%d\n", word, wordCounts[word])
		if _, err := writer.WriteString(line); err != nil {
			return fmt.Errorf("failed to write to chunk file %s: %w", fileName, err)
		}
	}

	return writer.Flush()
}
