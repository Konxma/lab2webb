package gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"testing"
	"time"
)

type testPeer struct {
	net.Conn
	reader *bufio.Reader
}

func launch(t *testing.T) (string, string, func()) {
	t.Helper()
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tcp.Close() })
	udp, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { udp.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var serverErr error
	go func() {
		defer close(done)
		serverErr = Serve(ctx, tcp, udp, log.New(io.Discard, "", 0))
	}()
	wait := func() {
		cancel()
		select {
		case <-done:
			if serverErr != nil {
				t.Errorf("Server returned %v", serverErr)
			}
		case <-time.After(4 * time.Second):
			t.Error("Server did not shut down")
		}
	}
	t.Cleanup(wait)
	return tcp.Addr().String(), udp.LocalAddr().String(), wait
}

func connect(t *testing.T, address string) testPeer {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	p := testPeer{Conn: conn, reader: bufio.NewReader(conn)}
	expect(t, p, welcome)
	return p
}

func read(t *testing.T, p testPeer) string {
	t.Helper()
	if err := p.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	line, err := p.reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSuffix(line, "\n")
}

func write(t *testing.T, p testPeer, commands string) {
	t.Helper()
	_ = p.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.WriteString(p, commands); err != nil {
		t.Fatal(err)
	}
}

func expect(t *testing.T, p testPeer, want string) {
	t.Helper()
	if got := read(t, p); got != want {
		t.Fatalf("Got %q, want %q", got, want)
	}
}

func expectEOF(t *testing.T, p testPeer) {
	t.Helper()
	_ = p.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := p.reader.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("Expected EOF, got %v", err)
	}
}

func TestCommandsAndFraming(t *testing.T) {
	tcp, _, _ := launch(t)
	p := connect(t, tcp)
	write(t, p, "EC")
	write(t, p, "HO Тестова перевірка\r\nTIME\nunknown\nCLOSE\n")
	expect(t, p, "[ECHO]: Тестова перевірка")
	stamp := strings.TrimPrefix(read(t, p), "[TIME]: ")
	if _, err := time.Parse(time.RFC3339, stamp); err != nil {
		t.Fatal(err)
	}
	if line := read(t, p); !strings.HasPrefix(line, "[ERROR]: Unknown command.") || !strings.Contains(line, "BROADCAST") {
		t.Fatalf("Expected command help, got %q", line)
	}
	expect(t, p, goodbye)
	expectEOF(t, p)
	oversized := connect(t, tcp)
	write(t, oversized, strings.Repeat("X", maxLine+1)+"\n")
	expect(t, oversized, "[ERROR]: Command exceeds 4096 bytes")
	expectEOF(t, oversized)
}

func TestBroadcastTelemetryAndShutdown(t *testing.T) {
	tcp, udp, stop := launch(t)
	a := connect(t, tcp)
	b := connect(t, tcp)
	expect(t, a, "[SYSTEM]: Joined "+b.LocalAddr().String())
	c := connect(t, tcp)
	for _, p := range []testPeer{a, b} {
		expect(t, p, "[SYSTEM]: Joined "+c.LocalAddr().String())
	}
	write(t, a, "BROADCAST alert\nECHO private\n")
	for _, p := range []testPeer{b, c} {
		expect(t, p, "[BROADCAST from "+a.LocalAddr().String()+"]: alert")
	}
	expect(t, a, "[ECHO]: private")
	write(t, b, "MSG reply\n")
	for _, p := range []testPeer{a, c} {
		expect(t, p, "[BROADCAST from "+b.LocalAddr().String()+"]: reply")
	}
	sensor, err := net.Dial("udp", udp)
	if err != nil {
		t.Fatal(err)
	}
	defer sensor.Close()
	for _, payload := range []string{strings.Repeat("X", maxTelemetry+1), "embedded\nnewline", "temp=24.5;press=750"} {
		if _, err := io.WriteString(sensor, payload); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []testPeer{a, b, c} {
		expect(t, p, "[TELEMETRY from "+sensor.LocalAddr().String()+"]: temp=24.5;press=750")
	}
	write(t, c, "QUIT\n")
	expect(t, c, goodbye)
	expectEOF(t, c)
	for _, p := range []testPeer{a, b} {
		expect(t, p, "[SYSTEM]: Left "+c.LocalAddr().String())
	}
	stop()
	for _, p := range []testPeer{a, b} {
		expect(t, p, shutdownEvent)
		expectEOF(t, p)
	}
	rebound, err := net.ListenPacket("udp", udp)
	if err != nil {
		t.Fatalf("UDP port was not released: %v", err)
	}
	rebound.Close()
}

func TestConcurrentBroadcast(t *testing.T) {
	tcp, _, _ := launch(t)
	a := connect(t, tcp)
	b := connect(t, tcp)
	read(t, a) // b joined.
	c := connect(t, tcp)
	read(t, a)
	read(t, b)
	const count = 40
	results := make(chan error, 2)
	for _, item := range []struct {
		peer testPeer
		name string
	}{{a, "alpha"}, {b, "beta"}} {
		go func() {
			_ = item.peer.SetWriteDeadline(time.Now().Add(3 * time.Second))
			for i := 0; i < count; i++ {
				if _, err := fmt.Fprintf(item.peer, "BROADCAST %s-%d\n", item.name, i); err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}()
	}
	seen := make(map[string]bool)
	for i := 0; i < count*2; i++ {
		line := read(t, c)
		if !strings.HasPrefix(line, "[BROADCAST from ") || seen[line] {
			t.Fatalf("Unexpected or duplicate broadcast: %q", line)
		}
		seen[line] = true
	}
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}
