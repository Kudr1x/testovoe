package internal

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func sortAndMerge(cfg Config) error {
	files, err := ioutil.ReadDir(cfg.TempPath)
	if err != nil {
		return fmt.Errorf("could not read temp directory: %w", err)
	}

	var chunks []string
	var currentChunkData []fileData

	for i, file := range files {
		if file.IsDir() {
			continue
		}

		num, err := readNumberFromFile(filepath.Join(cfg.TempPath, file.Name()))
		if err != nil {
			return fmt.Errorf("error reading number from %s: %w", file.Name(), err)
		}

		currentChunkData = append(currentChunkData, fileData{Name: file.Name(), Num: num})

		if len(currentChunkData) == cfg.ChunkSize || i == len(files)-1 {
			sort.Slice(currentChunkData, func(i, j int) bool {
				return currentChunkData[i].Num > currentChunkData[j].Num
			})

			chunkFile := filepath.Join(cfg.TempPath, fmt.Sprintf("sorted_chunk_%d.txt", len(chunks)))
			if err := saveChunkToFile(chunkFile, currentChunkData); err != nil {
				return fmt.Errorf("could not save chunk to file: %w", err)
			}

			chunks = append(chunks, chunkFile)
			currentChunkData = nil
		}
	}

	return mergeSortedChunks(chunks, cfg.OutputPath)
}

func mergeSortedChunks(chunkFiles []string, outputFile string) error {
	out, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer out.Close()
	writer := bufio.NewWriter(out)
	defer writer.Flush()

	var scanners []*bufio.Scanner
	var files []*os.File
	var heap []fileData
	var fileIndexMap = make(map[int]int)

	for i, chunkFile := range chunkFiles {
		file, err := os.Open(chunkFile)
		if err != nil {
			return err
		}
		files = append(files, file)

		scanner := bufio.NewScanner(file)
		scanners = append(scanners, scanner)

		if scanner.Scan() {
			var name string
			var num int
			fmt.Sscanf(scanner.Text(), "%s %d", &name, &num)
			heap = append(heap, fileData{Name: name, Num: num})
			fileIndexMap[len(heap)-1] = i
		}
	}

	for len(heap) > 0 {
		maxIndexInHeap := 0
		for i := 1; i < len(heap); i++ {
			if heap[i].Num > heap[maxIndexInHeap].Num {
				maxIndexInHeap = i
			}
		}

		best := heap[maxIndexInHeap]
		if _, err := writer.WriteString(fmt.Sprintf("%s %d\n", best.Name, best.Num)); err != nil {
			return err
		}

		originalFileIndex := fileIndexMap[maxIndexInHeap]
		if scanners[originalFileIndex].Scan() {
			var name string
			var num int
			fmt.Sscanf(scanners[originalFileIndex].Text(), "%s %d", &name, &num)
			heap[maxIndexInHeap] = fileData{Name: name, Num: num}
		} else {
			heap = append(heap[:maxIndexInHeap], heap[maxIndexInHeap+1:]...)
			delete(fileIndexMap, maxIndexInHeap)
			newMap := make(map[int]int)
			i := 0
			for j, fi := range fileIndexMap {
				if j > maxIndexInHeap {
					newMap[i] = fi
				} else {
					newMap[i] = fi
				}
				i++
			}
			fileIndexMap = newMap
		}
	}

	for _, file := range files {
		file.Close()
	}

	return nil
}

func readNumberFromFile(path string) (int, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return 0, err
	}
	num, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, err
	}
	return num, nil
}

func saveChunkToFile(filename string, chunk []fileData) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	for _, data := range chunk {
		if _, err := writer.WriteString(fmt.Sprintf("%s %d\n", data.Name, data.Num)); err != nil {
			return err
		}
	}
	return writer.Flush()
}
