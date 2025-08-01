package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

var words = []string{
	"this", "test", "asd", "the", "end",
	"sad", "is", "my", "only", "go", "language",
	"programming", "search", "query", "analysis",
	"utility", "developer", "random", "data", "generator",
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Error")
		return
	}

	numLines, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Error: %s", err)
	}

	const outputFileName = "/home/kudrix/GolandProjects/testovoeOtVaiki/data/input.txt"

	file, err := os.Create(outputFileName)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	rand.Seed(time.Now().UnixNano())

	fmt.Println("Start generate")

	for i := 0; i < numLines; i++ {
		randomWord := words[rand.Intn(len(words))]
		if _, err := writer.WriteString(randomWord + "\n"); err != nil {
			log.Fatalf("Error: %s", err)
		}
	}

	if err := writer.Flush(); err != nil {
		log.Fatalf("Error: %s", err)
	}

	fmt.Println("Done generate")
}
