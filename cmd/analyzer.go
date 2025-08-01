package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

var tempPath = "/home/kudrix/GolandProjects/testovoeOtVaiki/data/temp/"

type FileData struct {
	Name string
	Num  int
}

func processFile(inputPath, outputPath string, chunkSize int) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("error: %v", err)
	}
	defer file.Close()

	chunk := make([]string, 0, chunkSize)

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		chunk = append(chunk, scanner.Text())

		if len(chunk) == chunkSize {
			if err = processChunk(chunk); err != nil {
				return err
			}

			chunk = make([]string, 0, chunkSize)
		}
	}

	if len(chunk) > 0 {
		if err = processChunk(chunk); err != nil {
			return err
		}
	}

	if err = sortFiles(outputPath, chunkSize); err != nil {
		return err
	}

	return nil
}

func sortFiles(outputPatch string, chunkSize int) error {
	files, err := ioutil.ReadDir(tempPath)
	if err != nil {
		return err
	}

	var chunks []string
	var currentChunk []FileData

	for i, file := range files {
		if file.IsDir() {
			continue
		}

		num, err := readNumberFromFile(filepath.Join(tempPath, file.Name()))
		if err != nil {
			return fmt.Errorf("error %v", err)
		}

		currentChunk = append(currentChunk, FileData{
			Name: file.Name(),
			Num:  num,
		})

		if len(currentChunk) == chunkSize || i == len(files)-1 {
			sort.Slice(currentChunk, func(i, j int) bool {
				return currentChunk[i].Num > currentChunk[j].Num
			})

			chunkFile := fmt.Sprintf(tempPath+"chunk_%d.txt", len(chunks))
			err := saveChunkToFile(chunkFile, currentChunk)
			if err != nil {
				return fmt.Errorf("error %v", err)
			}

			chunks = append(chunks, chunkFile)
			currentChunk = nil
		}
	}

	err = mergeSortedChunks(chunks, outputPatch)
	if err != nil {
		return fmt.Errorf("error %v", err)
	}

	if err := os.RemoveAll(tempPath); err != nil {
		return fmt.Errorf("error: %v", err)
	}

	return nil
}

func processChunk(chunk []string) error {
	if err := os.MkdirAll(tempPath, 0755); err != nil {
		return fmt.Errorf("mkdir failed: %v", err)
	}

	for _, line := range chunk {
		if line == "" {
			continue
		}
		fileName := tempPath + line
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("open file failed: %v", err)
		}

		scanner := bufio.NewScanner(file)
		value := 1
		if scanner.Scan() {
			value, err = strconv.Atoi(scanner.Text())
			if err != nil {
				return fmt.Errorf("parse value failed: %v", err)
			}

			_, err = file.Seek(0, 0)
			if err != nil {
				return fmt.Errorf("seek file failed: %v", err)
			}

			err = file.Truncate(0)
			if err != nil {
				return fmt.Errorf("truncate file failed: %v", err)
			}

			_, err = file.WriteString(strconv.Itoa(value + 1))
			if err != nil {
				return fmt.Errorf("write file failed: %v", err)
			}
		} else {
			file.WriteString(strconv.Itoa(value))
		}

		file.Close()
	}

	return nil

}

func printUsage() {
	fmt.Fprintf(os.Stderr, "usage: %s [N] <input_file> <output_file>\n", os.Args[0])
}

func readNumberFromFile(path string) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return 0, fmt.Errorf("file is empty")
	}

	num, err := strconv.Atoi(scanner.Text())
	if err != nil {
		return 0, err
	}

	return num, nil
}

func saveChunkToFile(filename string, chunk []FileData) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	for _, data := range chunk {
		_, err := file.WriteString(fmt.Sprintf("%s %d\n", data.Name, data.Num))
		if err != nil {
			return err
		}
	}

	return nil
}

func mergeSortedChunks(chunkFiles []string, outputFile string) error {
	out, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer out.Close()

	writer := bufio.NewWriter(out)
	defer writer.Flush()

	scanners := make([]*bufio.Scanner, len(chunkFiles))
	files := make([]*os.File, len(chunkFiles))
	for i, chunkFile := range chunkFiles {
		file, err := os.Open(chunkFile)
		if err != nil {
			return err
		}
		files[i] = file
		scanners[i] = bufio.NewScanner(file)
	}

	heap := make([]FileData, 0)
	for _, scanner := range scanners {
		if scanner.Scan() {
			line := scanner.Text()
			var name string
			var num int
			_, err := fmt.Sscanf(line, "%s %d", &name, &num)
			if err != nil {
				return err
			}
			heap = append(heap, FileData{Name: name, Num: num})
		}
	}

	for len(heap) > 0 {
		minIndex := 0
		for i := 1; i < len(heap); i++ {
			if heap[i].Num > heap[minIndex].Num {
				minIndex = i
			}
		}

		_, err := writer.WriteString(fmt.Sprintf("%s %d\n", heap[minIndex].Name, heap[minIndex].Num))
		if err != nil {
			return err
		}

		if scanners[minIndex].Scan() {
			line := scanners[minIndex].Text()
			var name string
			var num int
			_, err := fmt.Sscanf(line, "%s %d", &name, &num)
			if err != nil {
				return err
			}
			heap[minIndex] = FileData{Name: name, Num: num}
		} else {
			files[minIndex].Close()
			heap = append(heap[:minIndex], heap[minIndex+1:]...)
		}
	}

	for _, file := range files {
		if file != nil {
			file.Close()
		}
	}

	return nil
}

func main() {
	var N int

	flag.IntVar(&N, "n", 1000, "max count unique request in memory")

	flag.Parse()

	args := flag.Args()
	if len(args) != 2 {
		fmt.Fprintf(os.Stderr, "error: must use input and outptut file\n")
		printUsage()
		os.Exit(1)
	}

	inputPath := args[0]
	outputPath := args[1]

	if N <= 0 {
		fmt.Fprintf(os.Stderr, "error: n must be > 0 \n")
		os.Exit(1)
	}

	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "error: input file %s is not exist\n", inputPath)
		os.Exit(1)
	}

	err := processFile(inputPath, outputPath, N)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\ndone!\n")
}
