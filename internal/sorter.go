package internal

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type heapItem struct {
	word      string
	count     int
	fileIndex int
}

type minHeap []heapItem

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].count > h[j].count }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(heapItem))
}
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func sortAndMerge(cfg Config) error {
	files, err := os.ReadDir(cfg.TempPath)
	if err != nil {
		return fmt.Errorf("could not read temp directory: %w", err)
	}

	var chunkFiles []string
	for _, file := range files {
		if !file.IsDir() {
			chunkFiles = append(chunkFiles, filepath.Join(cfg.TempPath, file.Name()))
		}
	}

	return mergeSortedChunks(chunkFiles, cfg.OutputPath)
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
	h := &minHeap{}
	heap.Init(h)

	for i, chunkFile := range chunkFiles {
		file, err := os.Open(chunkFile)
		if err != nil {
			return err
		}
		files = append(files, file)

		scanner := bufio.NewScanner(file)
		scanners = append(scanners, scanner)

		if scanner.Scan() {
			parts := strings.Split(scanner.Text(), "\t")
			if len(parts) == 2 {
				count, _ := strconv.Atoi(parts[1])
				heap.Push(h, heapItem{word: parts[0], count: count, fileIndex: i})
			}
		}
	}

	wordTotals := make(map[string]int)

	for h.Len() > 0 {
		item := heap.Pop(h).(heapItem)
		wordTotals[item.word] += item.count

		scanner := scanners[item.fileIndex]
		if scanner.Scan() {
			parts := strings.Split(scanner.Text(), "\t")
			if len(parts) == 2 {
				count, _ := strconv.Atoi(parts[1])
				heap.Push(h, heapItem{word: parts[0], count: count, fileIndex: item.fileIndex})
			}
		}
	}

	finalData := make([]fileData, 0, len(wordTotals))
	for word, count := range wordTotals {
		finalData = append(finalData, fileData{Name: word, Num: count})
	}

	sort.Slice(finalData, func(i, j int) bool {
		return finalData[i].Num > finalData[j].Num
	})

	for _, data := range finalData {
		if _, err := writer.WriteString(fmt.Sprintf("%s\t%d\n", data.Name, data.Num)); err != nil {
			return err
		}
	}

	for _, file := range files {
		file.Close()
	}

	return nil
}
