package tnt

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

// Box is a tarantool instance with specified config and BoxOptions.
type Box struct {
	Root     string
	Port     uint
	cmd      *exec.Cmd
	stopOnce sync.Once
	stopped  chan bool
}

// BoxOptions is the options for the Box instance.
type BoxOptions struct {
	Listen  uint
	PortMin uint
	PortMax uint
}

// NewBox instance.
func NewBox(config string, options... BoxOptions) (*Box, error) {
	var opts BoxOptions
	if len(options) > 0 {
		opts = options[0]
	} else {
		opts = BoxOptions{}
	}

	if opts.PortMin == 0 {
		opts.PortMin = 8000
	}

	if opts.PortMax == 0 {
		opts.PortMax = 9000
	}

	if opts.Listen != 0 {
		opts.PortMin = opts.Listen
		opts.PortMax = opts.Listen
	}

	var box *Box

START_LOOP:
	for port := opts.PortMin; port <= opts.PortMax; port += 2 {

		tmpDir, err := ioutil.TempDir("", "") //os.RemoveAll(tmpDir);
		if err != nil {
			return nil, err
		}

		tarantoolConf := `
		slab_alloc_arena = 1
		slab_alloc_factor = 1.04

		pid_file = {root}/box.pid
		work_dir = {root}
		snap_dir = snap
		wal_dir = wal

		snap_io_rate_limit = 10
		rows_per_wal = 1000000
		too_long_threshold = 0.025

		primary_port = {port1}
		memcached_expire = false
		memcached_port = {port2}
        `

		tarantoolConf = strings.Replace(tarantoolConf, "{port1}", fmt.Sprintf("%d", port), -1)
		tarantoolConf = strings.Replace(tarantoolConf, "{port2}", fmt.Sprintf("%d", port+1), -1)
		tarantoolConf = strings.Replace(tarantoolConf, "{root}", tmpDir, -1)

		tarantoolConf = fmt.Sprintf("%s\n%s", tarantoolConf, config)

		tarantoolConfFile := path.Join(tmpDir, "tarantool.conf")
		if err = ioutil.WriteFile(tarantoolConfFile, []byte(tarantoolConf), 0644); err != nil {
			return nil, err
		}

		for _, subDir := range []string{"snap", "wal"} {
			if err = os.Mkdir(path.Join(tmpDir, subDir), 0755); err != nil {
				return nil, err
			}
		}

		cmd0 := exec.Command("tarantool_box", "-c", tarantoolConfFile, "--init-storage")
		if err = cmd0.Run(); err != nil {
			return nil, err
		}

		cmd := exec.Command("tarantool_box", "-c", tarantoolConfFile)
		boxStderr, err := cmd.StderrPipe()
		if err != nil {
			return nil, err
		}

		if err = cmd.Start(); err != nil {
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
		return nil, fmt.Errorf("couldn't bind any port from %d to %d", opts.PortMin, opts.PortMax)
	}

	return box, nil
}

// Close Box instance.
func (box *Box) Close() {
	box.stopOnce.Do(func() {
		box.cmd.Process.Kill()
		box.cmd.Process.Wait()
		os.RemoveAll(box.Root)
		close(box.stopped)
	})
	<-box.stopped
}
