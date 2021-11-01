package main

import (
	"bufio"
	"fmt"
	"os"
	
	"testing"
	"io/ioutil"
	
	req "github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	log "github.com/sirupsen/logrus"
)

var AllTestSuites []suite.TestingSuite

func TestAllSuites(t *testing.T) {
	if !testing.Verbose() {
		log.SetOutput(ioutil.Discard)
	}
	for _, testsuite := range AllTestSuites {
		suite.Run(t, testsuite)
	}
}

func init() {
	AllTestSuites = append(AllTestSuites, &AggregateSuite{})
}

type AggregateSuite struct {
	BaseSuite
}
func (s *AggregateSuite) BeforeTest(suiteName, testName string) {
	s.DeleteLogDir()
}

func (s *AggregateSuite) TestNilOptions() {
	result := MainRoutine(nil)
	req.Equalf(s.T(), 1, result, "Failed check correct method result")
}

func (s *AggregateSuite) TestFullData() {
	s.GenerateLogDir("out", 5)

	result := MainRoutine(&Options{
		Input: "tempTest",
	})
	req.Equalf(s.T(), 0, result, "Failed check correct method result")

	s.CheckLogDir("out", 5)
}

func (s *AggregateSuite) TestNoData() {
	result := MainRoutine(&Options{
		Input: "tempTest",
	})

	req.Equalf(s.T(), 1, result, "Failed check correct method result")
}

// --- Test Utils --- //
type BaseSuite struct {
	suite.Suite
}

func (b *BaseSuite) DeleteLogDir() {
	_ = os.RemoveAll("tempTest")
}

func (b *BaseSuite) GenerateLogDir(basename string, maxChunks int) {
	if maxChunks < 1 {
		maxChunks = 1
	}
	_ = os.Mkdir("tempTest", 0777)

	var index = 0
	var lines = 4000
	for k := 0; k < maxChunks; k++ {
		f, _ := os.Create(fmt.Sprintf("tempTest/%s.%d.log", basename, maxChunks-k))
		for index = 0; index < lines; index++ {
			_, _ = f.WriteString(fmt.Sprintf("[Line %d]\n", lines*k+index))
		}
		_ = f.Close()
	}
}

func (b *BaseSuite) CheckLogDir(basename string, maxChunks int) {
	if maxChunks < 1 {
		maxChunks = 1
	}

	fullFile := fmt.Sprintf("tempTest/%s.full.log", basename)

	f, _ := os.OpenFile(fullFile, os.O_RDONLY, 0)
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Split(bufio.ScanLines)

	var index = 0
	var lines = 4000 * maxChunks
	for sc.Scan() {
		txt := sc.Text()
		checkTxt := fmt.Sprintf("[Line %d]", index)
		req.Equalf(b.T(), txt, checkTxt, "Failed output log check")
		index++
	}
	req.Equalf(b.T(), index, lines, "Failed output log length check")
}

func (b *BaseSuite) CheckChunkedDir(basename string, fullLines, fullChunks int) {
	if fullChunks < 1 {
		fullChunks = 1
	}

	var index = 0

	for k := 0; k < fullChunks; k++ {
		chunkFile := fmt.Sprintf("tempTest/%s.full.%d.log", basename, k)

		f, _ := os.OpenFile(chunkFile, os.O_RDONLY, 0)
		defer f.Close()
		sc := bufio.NewScanner(f)
		sc.Split(bufio.ScanLines)

		for sc.Scan() {
			txt := sc.Text()
			checkTxt := fmt.Sprintf("[Line %d]", index)
			req.Equalf(b.T(), txt, checkTxt, "Failed output log check")
			index++
		}
		req.Equalf(b.T(), index, fullLines*k, "Failed output log length check")
	}
	req.Equalf(b.T(), index, fullLines*fullChunks, "Failed output log length check")
}
