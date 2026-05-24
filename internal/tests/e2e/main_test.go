package e2e_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// waldbBin is the path to the compiled waldb binary used by CLI e2e tests.
var waldbBin string

func TestMain(m *testing.M) {
	bin, dir, err := buildWaldbBin()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build waldb binary: %v\n", err)
		os.Exit(1)
	}
	waldbBin = bin

	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func buildWaldbBin() (bin, dir string, err error) {
	dir, err = os.MkdirTemp("", "waldb-e2e-bin-*")
	if err != nil {
		return
	}
	bin = filepath.Join(dir, "waldb")
	cmd := exec.Command( //nolint:gosec // G204: fixed args, bin is a temp path created by this function
		"go", "build", "-o", bin, "github.com/julianstephens/waldb/cmd/waldb",
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	return
}
