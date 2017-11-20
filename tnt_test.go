package tnt

import (
	"flag"
	"os"
	"testing"
)

var testTntPrimaryPort int

type testingT interface {
	SkipNow()
}

func setUp(t testingT) (int, func()) {
	if testing.Short() {
		t.SkipNow()
		return 0, func() {}
	}
	return testTntPrimaryPort, func() {}
}

func TestMain(t *testing.M) {
	flag.IntVar(&testTntPrimaryPort, "test.tarantool_primary_port", 2001, "primary port for test tarantool")
	flag.Parse()

	os.Exit(t.Run())
}
