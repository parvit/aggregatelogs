package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/jessevdk/go-flags"
)

type Options struct {
	Input     flags.Filename `short:"i" long:"input" description:"Input file" default:"."`
	Reverse   bool           `short:"n" long:"Reverse" description:"Reverse numerical order of found files"`
	Delete    bool           `short:"d" long:"delete" description:"Delete original files'"`
	MaxChunks int            `short:"c" long:"max-chunks" description:"Max Chunks to merge, default 0 means merge all'" default:"0"`
}

const (
	optionsFormat       = "[Config]\nInput: %v\nReverse: %v\nDelete: %v\nMaxChunks: %v"
	aggregatedLogSuffix = "full"
)

func (o Options) String() string {
	return fmt.Sprintf(optionsFormat, o.Input, o.Reverse, o.Delete, o.MaxChunks)
}

var options Options

var parser = flags.NewParser(&options, flags.Default)

type logFile struct {
	index int
	name  string
}

type FilesList map[string][]*logFile

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Println("ERROR: ", err)
		}
		log.Println("[Finished]")
	}()

	log.Println("[Begin AggregateLogs]")
	if _, err := parser.Parse(); err != nil {
		outCode := 0
		if flagsErr, ok := err.(*flags.Error); !ok || flagsErr.Type != flags.ErrHelp {
			log.Println(flagsErr)
			outCode = 1
		}
		os.Exit(outCode)
	}

	log.Println(options)

	log.Println("[Begin scan of path]")
	allFiles, err := ScanFolderForFiles(options.Input)

	log.Println("[End scan of path]")
	if err != nil {
		log.Fatalf("ERROR: found during input path traversal: %v\n", err)
		os.Exit(1)
	}

	for fBase, list := range allFiles {
		MergeLogList(string(options.Input), fBase, list, &options)

		if options.Delete {
			DeleteLogList(string(options.Input), list)
		}
	}
}

func ScanFolderForFiles(logsPath flags.Filename) (FilesList, error) {
	// files list by base name
	filesMap := make(FilesList)

	basepath, _ := filepath.Abs(string(logsPath))
	log.Println("[Start analysis of basepath: ", basepath, "]")
	err := filepath.Walk(basepath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && path != basepath {
			return filepath.SkipDir
		}
		if info.IsDir() && path == basepath {
			return nil
		}

		// Do not check the extension, .log might be in the middle
		// of the name because of the split ".1"
		// also ignore previous runs as they'll be overwritten later
		if !strings.Contains(info.Name(), ".log") || strings.Contains(info.Name(), "."+aggregatedLogSuffix) {
			return nil
		}

		parts := strings.Split(info.Name(), ".")
		if filesMap[parts[0]] == nil {
			filesMap[parts[0]] = make([]*logFile, 0, 256)
		}
		log.Println("Found: ", info.Name())
		def := &logFile{
			index: 0,
			name:  info.Name(),
		}
		for i := len(parts) - 1; i > 0; i-- {
			// the last number is relevant for ordering
			if idx, err := strconv.Atoi(parts[i]); err == nil {
				def.index = idx
				break
			}
		}
		filesMap[parts[0]] = append(filesMap[parts[0]], def)

		return nil
	})

	return filesMap, err
}

func MergeLogList(basepath, basename string, list []*logFile, config *Options) {
	log.Println("[Start output of log: ", basepath, "]")
	// alphabetical order is not good here, actual numeric order is required
	sort.Slice(list, func(i, j int) bool {
		if config.Reverse {
			return list[i].index <= list[j].index
		}
		return list[i].index > list[j].index
	})

	var chunks = len(list)
	if options.MaxChunks > 1 {
		chunks = len(list) / options.MaxChunks
		if chunks < 2 {
			log.Println("[ERROR]: Cannot subdivide into the indicated number of chunks.")
			return
		}
		if len(list)%options.MaxChunks > 0 {
			chunks++
		}
	} else {
		options.MaxChunks = 1
	}

	nameOutFile := strings.Join([]string{basename, aggregatedLogSuffix, "log"}, ".")

	for chunkIdx := 0; chunkIdx < options.MaxChunks; chunkIdx++ {
		if options.MaxChunks > 1 {
			idxString := strconv.FormatInt(int64(chunkIdx+1), 10)
			nameOutFile = strings.Join([]string{basename, aggregatedLogSuffix, idxString, "log"}, ".")
		}

		outFile, _ := filepath.Abs(filepath.Join(basepath, nameOutFile))
		f, err := os.Create(outFile)
		if err != nil {
			log.Println("[End output for ERROR: ", err, "]")
			return
		}
		log.Println("Created output file: ", outFile)

		var currPos = chunkIdx * chunks
		var nextPos = (chunkIdx + 1) * chunks
		if nextPos >= len(list) {
			nextPos = len(list)
		}

		log.Println(chunkIdx*chunks, chunks)
		MergeLogChunk(basepath, f, list[currPos:nextPos])
	}
}

func MergeLogChunk(basepath string, f *os.File, list []*logFile) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("[ERROR]: %v\n", err)
		}
		if f != nil {
			// flush and close the file
			_ = f.Sync()
			_ = f.Close()
		}
		log.Println("[End output of log]")
	}()

	log.Println("[Start output of log chunk: ", basepath, "]")

	for idx, logPart := range list {
		log.Println("[", idx+1, " / ", len(list), "]: ", logPart.name)

		data, err := ioutil.ReadFile(filepath.Join(basepath, logPart.name))
		if err != nil {
			log.Println("[End output for ERROR: ", err, "]")
			return
		}

		log.Println("Read ", len(data), "bytes")
		_, _ = f.Write(data)
	}
}

func DeleteLogList(basepath string, list []*logFile) {
	log.Println("[Start delete of log: ", basepath, "]")
	wg := &sync.WaitGroup{}
	for _, logPart := range list {
		wg.Add(1)
		log.Println("[Delete ", logPart.name, "]")
		go func(deleteFile string) {
			defer wg.Done()
			if err := os.Remove(deleteFile); err != nil {
				log.Println("Delete file error: ", err)
			}

		}(filepath.Join(basepath, logPart.name))
	}
	wg.Wait()
	log.Println("[End delete of log: ", basepath, "]")
}
