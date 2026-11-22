package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestInheritedListenersNeedOurPID(t *testing.T) {
	t.Setenv("LISTEN_PID", "1")
	t.Setenv("LISTEN_FDS", "1")
	if l, err := inheritedListeners(); err != nil || l != nil {
		t.Errorf("sockets of another process: %v, %v; want none", l, err)
	}
}

// A request that arrives while no process serves the socket must wait for
// the next one, as it does across a restart under systemd.
func TestInheritedSocketHoldsRequestsAcrossARestart(t *testing.T) {
	socket, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = socket.Close() }()
	addr := socket.Addr().String()
	file := func() *os.File {
		f, err := socket.(*net.TCPListener).File()
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	serve := func(answer string) (stop func()) {
		ls, err := listenersFromFiles([]*os.File{file()})
		if err != nil || ls[addr] == nil {
			t.Fatalf("listeners = %v, %v; want one at %s", ls, err, addr)
		}
		ctx, cancel := context.WithCancel(context.Background())
		srv := &http.Server{Addr: addr, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, answer) })}
		done := make(chan error, 1)
		go func() { done <- serveHTTP(ctx, srv, ls[addr]) }()
		return func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("serveHTTP: %v", err)
			}
		}
	}
	get := func() string {
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Get("http://" + addr)
		if err != nil {
			return "error: " + err.Error()
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return string(b)
	}

	stop := serve("first")
	if got := get(); got != "first" {
		t.Fatalf("first process answered %q", got)
	}
	stop()

	// no process: the request is accepted by the kernel and waits
	answer := make(chan string, 1)
	go func() { answer <- get() }()
	time.Sleep(300 * time.Millisecond)
	stop = serve("second")
	defer stop()
	if got := <-answer; got != "second" {
		t.Errorf("request sent between the two processes got %q, want the answer of the second", got)
	}
}
