package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
)

// firstInheritedFD is where systemd puts the sockets of a socket-activated
// unit (sd_listen_fds(3)).
const firstInheritedFD = 3

// inheritedListeners returns the listening sockets systemd passed to this
// process, by their address. With a socket unit the port stays open while
// the service restarts, so a webhook delivery waits for the new process
// instead of being refused; GitHub does not retry a refused delivery.
func inheritedListeners() (map[string]net.Listener, error) {
	if pid, _ := strconv.Atoi(os.Getenv("LISTEN_PID")); pid != os.Getpid() {
		return nil, nil
	}
	n, err := strconv.Atoi(os.Getenv("LISTEN_FDS"))
	if err != nil || n < 0 {
		return nil, fmt.Errorf("LISTEN_FDS: not a count: %q", os.Getenv("LISTEN_FDS"))
	}
	files := make([]*os.File, n)
	for i := range files {
		files[i] = os.NewFile(uintptr(firstInheritedFD+i), "systemd socket")
	}
	return listenersFromFiles(files)
}

func listenersFromFiles(files []*os.File) (map[string]net.Listener, error) {
	out := map[string]net.Listener{}
	for _, f := range files {
		l, err := net.FileListener(f)
		// FileListener works on a duplicate
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("inherited socket: %w", err)
		}
		out[l.Addr().String()] = l
	}
	return out, nil
}
