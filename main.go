package main

import (
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
	Invert    bool           `short:"n" long:"invert" description:"Invert numerical order of found files"`
	Delete    bool           `short:"d" long:"delete" description:"Delete original files'"`
	MaxChunks int            `short:"c" long:"max-chunks" description:"Max Chunks to merge, default 0 means merge all'" default:"0"`
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

	log.Println("[Begin scan of path]")
	allFiles, err := ScanFolderForFiles(options.Input)

	log.Println("[End scan of path]")
	if err != nil {
		log.Fatalf("ERROR: found during input path traversal: %v\n", err)
		os.Exit(1)
	}

	for fBase, list := range allFiles {
		MergeLogList(string(options.Input), fBase, list, options.Invert)

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
		if !strings.Contains(info.Name(), ".log") || strings.Contains(info.Name(), ".aggregated") {
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

func MergeLogList(basepath, basename string, list []*logFile, reversedOrder bool) {
	log.Println("[Start output of log: ", basepath, "]")
	// alphabetical order is not good here, actual numeric order is required
	sort.Slice(list, func(i, j int) bool {
		if reversedOrder {
			return list[i].index <= list[j].index
		}
		return list[i].index > list[j].index
	})

	nameOutFile := strings.Join([]string{basename, "aggregated", "log"}, ".")
	outFile, _ := filepath.Abs(filepath.Join(basepath, nameOutFile))
	f, err := os.Create(outFile)
	if err != nil {
		log.Println("[End output for ERROR: ", err, "]")
		return
	}
	log.Println("Created output file: ", outFile)

	defer func() {
		// flush and close the file
		_ = f.Sync()
		_ = f.Close()
		log.Println("[End output of log]")
	}()
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
