package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/jessevdk/go-flags"
)

type Options struct {
	Input     flags.Filename `short:"i" long:"input" description:"Input file" default:"."`
	Reverse   bool           `short:"r" long:"reverse" description:"Reverse numerical order of found files"`
	Delete    bool           `short:"d" long:"delete" description:"Delete original files"`
	MaxChunks int            `short:"c" long:"max-chunks" description:"Max chunks to merge, default 0 means merge all" default:"0"`

	Filters         []string         `short:"f" long:"filter" description:"List of regex filters for fields" default:""`
	compiledFilters []*regexp.Regexp // compiled regex filters to be applied, if empty all data is accepted as-is
}

const (
	optionsFormat       = "[Config]\nInput: %v\nReverse: %v\nDelete: %v\nMaxChunks: %v"
	aggregatedLogSuffix = "full"
)

func (o Options) String() string {
	return fmt.Sprintf(optionsFormat, o.Input, o.Reverse, o.Delete, o.MaxChunks)
}

type logFile struct {
	index int
	name  string
}

type FilesList map[string][]*logFile

func main() {
	var options Options
	var parser = flags.NewParser(&options, flags.Default)

	defer func() {
		if err := recover(); err != nil {
			log.Errorf("[ERROR]: %v\n", err)
			log.Errorf("%v\n", string(debug.Stack()))
		}
		log.Println("[Finished]")
	}()

	log.Println("[Begin AggregateLogs]")
	if _, err := parser.Parse(); err != nil {
		outCode := 0
		if flagsErr, ok := err.(*flags.Error); !ok || flagsErr.Type != flags.ErrHelp {
			log.Errorf("%v\n", flagsErr)
			outCode = 1
		}
		os.Exit(outCode)
	}

	os.Exit(MainRoutine(&options))
}

func MainRoutine(options *Options) int {
	if options == nil {
		log.Errorf("Launch options not passed correctly\n")
		return 1
	}

	// precompile all filters specified to avoid overhead during execution
	for _, f := range options.Filters {
		options.compiledFilters = append(options.compiledFilters, regexp.MustCompile(f))
	}
	log.Println(options)

	log.Println("[Begin scan of path]")
	allFiles, err := ScanFolderForFiles(options.Input)
	log.Println("[End scan of path]")

	if err != nil {
		log.Errorf("ERROR: found during input path traversal: %v\n", err)
		return 1
	}

	for fBase, list := range allFiles {
		MergeLogList(options, string(options.Input), fBase, list)

		if options.Delete {
			DeleteLogList(string(options.Input), list)
		}
	}
	// correct execution
	return 0
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

func MergeLogList(config *Options, basepath, basename string, list []*logFile) {
	log.Println("[Start output of log: ", basepath, "]")
	// alphabetical order is not good here, actual numeric order is required
	sort.Slice(list, func(i, j int) bool {
		if config.Reverse {
			return list[i].index <= list[j].index
		}
		return list[i].index > list[j].index
	})

	var outputFilesPerChunk = len(list)
	if config.MaxChunks <= 1 {
		config.MaxChunks = 1
	} else {
		outputFilesPerChunk = len(list) / config.MaxChunks
		if outputFilesPerChunk < 2 {
			log.Errorf("[ERROR]: Cannot subdivide into the indicated number of outputFilesPerChunk.\n")
			return
		}
		if len(list)%config.MaxChunks > 0 {
			config.MaxChunks++
		}
	}

	nameOutFile := strings.Join([]string{basename, aggregatedLogSuffix, "log"}, ".")

	for chunkIdx := 0; chunkIdx < config.MaxChunks; chunkIdx++ {
		if config.MaxChunks > 1 {
			idxString := strconv.FormatInt(int64(chunkIdx+1), 10)
			nameOutFile = strings.Join([]string{basename, aggregatedLogSuffix, idxString, "log"}, ".")
		}

		outFile, _ := filepath.Abs(filepath.Join(basepath, nameOutFile))
		f, err := os.Create(outFile)
		if err != nil {
			log.Errorf("[End output for ERROR: %v]\n", err)
			return
		}
		log.Println("Created output file: ", outFile)

		var currPos = chunkIdx * outputFilesPerChunk
		var nextPos = (chunkIdx + 1) * outputFilesPerChunk
		if nextPos >= len(list) {
			nextPos = len(list)
		}

		MergeLogChunk(config, basepath, f, list[currPos:nextPos])
	}
}

func MergeLogChunk(config *Options, basepath string, f *os.File, list []*logFile) {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("[ERROR]: %v\n", err)
			log.Errorf("%v\n", string(debug.Stack()))
		}
		if f != nil {
			// flush and close the file
			_ = f.Sync()
			_ = f.Close()
		}
		log.Println("[End output of log chunk]")
	}()

	log.Println("[Start output of log chunk]")

	var currentWriteFileIndex = int32(0)

	// Loads all file parts specified for chunk and writes it
	// to the output file in parallel
	wg := &sync.WaitGroup{}
	wg.Add(len(list))
	for idx, _ := range list {
		go func(listIndex int32) {
			defer func() {
				if err := recover(); err != nil {
					log.Errorf("[ERROR]: %v\n", err)
					log.Errorf("%v\n", string(debug.Stack()))
				}
				wg.Done()
			}()

			fileFullpath := filepath.Join(basepath, list[listIndex].name)
			data, err := LoadDataToWrite(config, fileFullpath)
			if err != nil {
				log.Errorf("[ERROR]: End output for %v\n", err)
				return
			}

			// respect ordering of output even if loading in parallel
			for atomic.LoadInt32(&currentWriteFileIndex) != listIndex {
				time.Sleep(10 * time.Microsecond)
			}

			log.Printf("[%d / %d]: %s (Read %d bytes)\n", listIndex+1, len(list), list[listIndex].name, data.Len())
			_, _ = f.Write(data.Bytes())

			atomic.StoreInt32(&currentWriteFileIndex, listIndex+1)
		}(int32(idx))
	}
	wg.Wait()
}

func LoadDataToWrite(config *Options, fileFullpath string) (*bytes.Buffer, error) {
	if len(config.compiledFilters) < 1 {
		// fast path load entire file without filter
		data, err := ioutil.ReadFile(fileFullpath)
		if err != nil {
			return nil, err
		}

		buf := bytes.NewBuffer(data)
		return buf, nil
	}

	// filtering is requested so load file line by line with scanner and filter
	// it to a buffer
	f, err := os.OpenFile(fileFullpath, os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := bytes.NewBuffer(make([]byte, 4096))
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		data := scanner.Bytes()

		// lines can match multiple filters, if they contain captures
		// only those are written, else the entire line
		for _, filter := range config.compiledFilters {
			matches := filter.FindAllSubmatch(data, -1)
			if len(matches) < 1 {
				continue
			}

			for _, match := range matches {
				if len(match) == 1 {
					// no captures in filter
					buf.Write(match[0])
					continue
				}

				for k := 1; k < len(match)-1; k++ {
					buf.Write(match[k])
					buf.WriteByte(' ')
				}
				buf.Write(match[len(match)-1])
			}
		}
		buf.WriteByte('\n')
	}

	return buf, nil
}

func DeleteLogList(basepath string, list []*logFile) {
	log.Println("[Start delete of log: ", basepath, "]")
	wg := &sync.WaitGroup{}
	for _, logPart := range list {
		wg.Add(1)
		log.Println("[Delete ", logPart.name, "]")
		go func(deleteFile string) {
			defer func() {
				if err := recover(); err != nil {
					log.Errorf("[ERROR]: %v\n", err)
					log.Errorf("%v\n", string(debug.Stack()))
				}
				wg.Done()
			}()
			if err := os.Remove(deleteFile); err != nil {
				log.Warningf("Delete file error: %v\n", err)
			}

		}(filepath.Join(basepath, logPart.name))
	}
	wg.Wait()
	log.Println("[End delete of log: ", basepath, "]")
}
