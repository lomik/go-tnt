package tnt

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	dirSnap = "snap"
	dirWAL  = "wal"
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
	InitLua string
}

// NewBox instance.
func NewBox(config string, options ...BoxOptions) (*Box, error) {
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
	for port := opts.PortMin; port <= opts.PortMax; port += 3 {

		tmpDir, err := ioutil.TempDir("", "")
		if err != nil {
			return nil, err
		}

		tarantoolConf := `
		slab_alloc_arena = 1
		slab_alloc_factor = 1.04

		pid_file = {root}/box.pid
		work_dir = {root}
		snap_dir = {snap}
		wal_dir = {wal}

		snap_io_rate_limit = 10
		rows_per_wal = 1000000
		too_long_threshold = 0.025

		primary_port = {port1}
		memcached_expire = false
		memcached_port = {port2}
		admin_port = {port3}
		replication_port = {port4}
        `

		tarantoolConf = strings.Replace(tarantoolConf, "{port1}", fmt.Sprintf("%d", port), -1)
		tarantoolConf = strings.Replace(tarantoolConf, "{port2}", fmt.Sprintf("%d", port+1), -1)
		tarantoolConf = strings.Replace(tarantoolConf, "{port3}", fmt.Sprintf("%d", port+2), -1)
		tarantoolConf = strings.Replace(tarantoolConf, "{port4}", fmt.Sprintf("%d", port+3), -1)
		tarantoolConf = strings.Replace(tarantoolConf, "{root}", tmpDir, -1)
		tarantoolConf = strings.Replace(tarantoolConf, "{snap}", dirSnap, -1)
		tarantoolConf = strings.Replace(tarantoolConf, "{wal}", dirWAL, -1)

		tarantoolConf = fmt.Sprintf("%s\n%s", tarantoolConf, config)

		tarantoolConfFile := path.Join(tmpDir, "tarantool.conf")
		if err = ioutil.WriteFile(tarantoolConfFile, []byte(tarantoolConf), 0644); err != nil {
			return nil, err
		}

		initLuaFile := path.Join(tmpDir, "init.lua")
		if err = ioutil.WriteFile(initLuaFile, []byte(opts.InitLua), 0644); err != nil {
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

// Listen is the primary addr of the box.
func (box *Box) Listen() string {
	return fmt.Sprintf("127.0.0.1:%v", box.Port)
}

// ListenMemcache is the memcache addr of the box.
func (box *Box) ListenMemcache() string {
	return fmt.Sprintf("127.0.0.1:%v", box.Port+1)
}

// ListenAdmin is the admin addr of the box.
func (box *Box) ListenAdmin() string {
	return fmt.Sprintf("127.0.0.1:%v", box.Port+2)
}

// ListenReplica is the replication addr of the box.
func (box *Box) ListenReplica() string {
	return fmt.Sprintf("127.0.0.1:%v", box.Port+3)
}

// SaveSnapshot and return it's filename (with full path).
func (box *Box) SaveSnapshot() (string, error) {
	const (
		cmdSnapshot   = "save snapshot\n"
		cmdOK         = "ok"
		cmdFileExists = "fail: can't save snapshot, errno 17 (File exists)"
		cmdSeparators = "\n-. "
	)
	conn, err := net.Dial("tcp", box.ListenAdmin())
	if err != nil {
		return "", err
	}
	defer conn.Close()
	if err = conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return "", err
	}
	if _, err = conn.Write([]byte(cmdSnapshot)); err != nil {
		return "", err
	}
	respBytes := make([]byte, 256)
	n, err := conn.Read(respBytes)
	if err != nil {
		return "", err
	}
	response := strings.TrimRight(strings.TrimLeft(string(respBytes[:n]), cmdSeparators), cmdSeparators)
	switch response {
	case cmdOK, cmdFileExists:
		return box.Snapshot()
	default:
		return "", errors.New(response)
	}
}

// SnapDir of the box.
func (box *Box) SnapDir() string {
	return filepath.Join(box.Root, dirSnap)
}

// WALDir of the box.
func (box *Box) WALDir() string {
	return filepath.Join(box.Root, dirWAL)
}

var ErrSnapshotNotFound = errors.New("snapshot file hasn't been found")

// Snapshot returns the latest snapshot filename with full path.
func (box *Box) Snapshot() (string, error) {
	const snapExt = ".snap"
	files, err := ioutil.ReadDir(box.SnapDir())
	if err != nil {
		return "", err
	}

	// files are sorted alphabetically
	for i := len(files) - 1; i >= 0; i-- {
		if filepath.Ext(files[i].Name()) != snapExt {
			continue
		}
		return filepath.Join(box.SnapDir(), files[i].Name()), nil
	}
	return "", ErrSnapshotNotFound
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
