package tnt

import (
	"fmt"
	"math/rand"

	"gitlab.corp.mail.ru/rb/go/testlib/tntctl"
)

func setUp() (int, func(), error) {
	cfg := &tntctl.TarantoolConfig{}
	spaceCfg := cfg.AddSpace(0, true)
	spaceCfg.AddIndex(true, "HASH").AddKey(0, "NUM")
	spaceCfg.AddIndex(false, "TREE").AddKey(1, "NUM")

	spaceCfg = cfg.AddSpace(10, true)
	index := spaceCfg.AddIndex(true, "TREE")
	index.AddKey(0, "NUM")
	index.AddKey(1, "NUM64")
	index.AddKey(2, "STR")
	spaceCfg.AddIndex(false, "TREE").AddKey(0, "NUM")
	spaceCfg.AddIndex(false, "TREE").AddKey(1, "NUM64")
	spaceCfg.AddIndex(false, "TREE").AddKey(2, "STR")

	tearDown := func() { tntctl.TeardownAll() }
	nothingDown := func() {}

	tnt, err := tntctl.SetupTarantool(cfg, fmt.Sprintf("tnt_%d", rand.Int()))
	if err != nil {
		return 0, nothingDown, err
	}
	if err = tnt.Start(); err != nil {
		return 0, nothingDown, err
	}

	return int(tnt.Config.PrimaryPort), tearDown, nil
}
