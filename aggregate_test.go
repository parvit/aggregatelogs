package main

import "testing"

func TestAggregateLogs_NilOptions(t *testing.T) {
	MainRoutine(nil)
}

func TestAggregateLogs_NoData(t *testing.T) {
	MainRoutine(&Options{
		Input: ".",
	})
}

// --- Test Utils --- //
func GenerateLogFile(basename string, maxChunks int) {

}
