package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"testovoeOtVaiki/internal"
)

func main() {
	var N int
	var tempPath string

	flag.IntVar(&N, "n", 1000, "max count unique items in one chunk")
	flag.StringVar(&tempPath, "temp", "./temp_data", "path to temporary directory")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <input_file> <output_file>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) != 2 {
		log.Println("Error: input and output file must be")
		flag.Usage()
		os.Exit(1)
	}

	if N <= 0 {
		log.Fatalln("Error: -n must be > 0")
	}

	inputPath := args[0]
	if _, err := os.Stat(inputPath); os.IsNotExist(err) {
		log.Fatalf("Error: input file %s does not exist\n", inputPath)
	}

	cfg := internal.Config{
		InputPath:  inputPath,
		OutputPath: args[1],
		ChunkSize:  N,
		TempPath:   tempPath,
	}

	fmt.Println("Processing started...")
	err := internal.ProcessFiles(cfg)
	if err != nil {
		log.Fatalf("error: %v\n", err)
	}

	fmt.Println("Done!")
}
