package tntctl

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
)

// Box is tarantool instance. For start/stop tarantool in tests
type Box struct {
	Root     string
	Port     uint
	cmd      *exec.Cmd
	stopOnce sync.Once
	stopped  chan bool
}

type Options struct {
	Listen  uint
	PortMin uint
	PortMax uint
}

func New(config string, options *Options) (*Box, error) {
	if options == nil {
		options = &Options{}
	}

	if options.PortMin == 0 {
		options.PortMin = 8000
	}

	if options.PortMax == 0 {
		options.PortMax = 9000
	}

	if options.Listen != 0 {
		options.PortMin = options.Listen
		options.PortMax = options.Listen
	}

	var box *Box

START_LOOP:
	for port := options.PortMin; port <= options.PortMax; port++ {

		tmpDir, err := ioutil.TempDir("", "") //os.RemoveAll(tmpDir);
		if err != nil {
			return nil, err
		}

		tarantoolConf := `
		slab_alloc_arena = 0.1
		slab_alloc_factor = 1.04

		pid_file = {root}/box.pid
		work_dir = {root}
		snap_dir = snap
		wal_dir = wal

		snap_io_rate_limit = 10
		rows_per_wal = 1000000
		too_long_threshold = 0.025

		primary_port = {port}
		memcached_expire = false
        `

		tarantoolConf = strings.Replace(tarantoolConf, "{port}", fmt.Sprintf("%d", port), -1)
		tarantoolConf = strings.Replace(tarantoolConf, "{root}", tmpDir, -1)

		tarantoolConf = fmt.Sprintf("%s\n%s", tarantoolConf, config)

		tarantoolConfFile := path.Join(tmpDir, "tarantool.conf")
		err = ioutil.WriteFile(tarantoolConfFile, []byte(tarantoolConf), 0644)
		if err != nil {
			return nil, err
		}

		for _, subDir := range []string{"snap", "wal"} {
			err = os.Mkdir(path.Join(tmpDir, subDir), 0755)
			if err != nil {
				return nil, err
			}
		}

		cmd0 := exec.Command("tarantool_box", "-c", tarantoolConfFile, "--init-storage")
		err = cmd0.Run()
		if err != nil {
			return nil, err
		}

		cmd := exec.Command("tarantool_box", "-c", tarantoolConfFile)
		boxStderr, err := cmd.StderrPipe()
		if err != nil {
			return nil, err
		}

		err = cmd.Start()
		if err != nil {
			return nil, err
		}

		var boxStderrBuffer bytes.Buffer

		p := make([]byte, 1024)

		box = &Box{
			Root:    tmpDir,
			Port:    port,
			cmd:     cmd,
			stopped: make(chan bool),
		}

	WAIT_LOOP:
		for {
			if strings.Contains(boxStderrBuffer.String(), "entering event loop") {
				break START_LOOP
			}

			if strings.Contains(boxStderrBuffer.String(), "is already in use, will retry binding after") {
				cmd.Process.Kill()
				cmd.Process.Wait()
				break WAIT_LOOP
			}

			n, err := boxStderr.Read(p)
			if n > 0 {
				boxStderrBuffer.Write(p[:n])
			}
			if err != nil {
				fmt.Println(boxStderrBuffer.String())
				return nil, err
			}
		}

		os.RemoveAll(box.Root)
		box = nil
	}

	if box == nil {
		return nil, fmt.Errorf("Can't bind any port from %d to %d", options.PortMin, options.PortMax)
	}

	return box, nil
}

func (box *Box) Close() {
	box.stopOnce.Do(func() {
		box.cmd.Process.Kill()
		box.cmd.Process.Wait()
		os.RemoveAll(box.Root)
		close(box.stopped)
	})
	<-box.stopped
}
