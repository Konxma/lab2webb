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
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	maxLine       = 4096
	maxTelemetry  = 1024
	queueSize     = 128
	writeWait     = 2 * time.Second
	welcome       = "[SERVER]: Connected. Commands: ECHO, TIME, BROADCAST, MSG, CLOSE, QUIT"
	goodbye       = "[SERVER]: Goodbye"
	shutdownEvent = "[SYSTEM]: SERVER_SHUTTING_DOWN"
)

type client struct {
	conn   net.Conn
	addr   string
	out    chan string
	ending bool // Protected by the server mutex.
}

type server struct {
	logger   *log.Logger
	mu       sync.Mutex
	clients  map[*client]struct{}
	handlers sync.WaitGroup
	stopping bool
}

// Serve owns both listeners. It returns after the socket loops and client handlers have stopped.
func Serve(ctx context.Context, tcp net.Listener, udp net.PacketConn, logger *log.Logger) error {
	s := &server{logger: logger, clients: make(map[*client]struct{})}
	stopped := make(chan error, 2)
	var listeners sync.WaitGroup
	listeners.Add(2)
	go func() {
		defer listeners.Done()
		stopped <- s.listenTCP(tcp)
	}()
	go func() {
		defer listeners.Done()
		stopped <- s.listenUDP(udp)
	}()
	var err error
	select {
	case <-ctx.Done():
		logger.Print("[SERVER] Shutdown signal received")
	case err = <-stopped:
		logger.Printf("[SERVER] Listener stopped: %v", err)
	}
	_ = tcp.Close()
	_ = udp.Close()
	listeners.Wait() // The accept loop can no longer add handlers.
	s.shutdown()
	return err
}

func (s *server) listenTCP(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		c := &client{conn: conn, addr: conn.RemoteAddr().String(), out: make(chan string, queueSize+1)}
		// Reserve one slot for the final shutdown event.
		c.out <- welcome
		s.mu.Lock()
		s.clients[c] = struct{}{}
		failed := s.broadcastLocked(c, "[SYSTEM]: Joined "+c.addr)
		s.mu.Unlock()
		s.logger.Printf("[TCP] Connected %s", c.addr)
		s.handlers.Add(1)
		go s.handle(c)
		closeClients(failed)
	}
}

func (s *server) handle(c *client) {
	defer s.handlers.Done()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		defer c.conn.Close()
		for line := range c.out {
			if err := c.conn.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				return
			}
			if _, err := io.WriteString(c.conn, line+"\n"); err != nil {
				return
			}
		}
	}()
	defer func() {
		s.leave(c)
		<-writerDone
		s.mu.Lock()
		delete(s.clients, c)
		s.mu.Unlock()
	}()
	reader := bufio.NewReaderSize(c.conn, maxLine+2)
	for {
		line, err := reader.ReadSlice('\n')
		text := strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r")
		if errors.Is(err, bufio.ErrBufferFull) || (err == nil && len(text) > maxLine) {
			s.reply(c, "[ERROR]: Command exceeds 4096 bytes")
			return
		}
		if err != nil || !s.command(c, strings.TrimSpace(text)) {
			return
		}
	}
}

func (s *server) command(c *client, line string) bool {
	if line == "" {
		return true
	}
	command, text := line, ""
	if i := strings.IndexAny(line, " \t"); i >= 0 {
		command, text = line[:i], strings.TrimSpace(line[i+1:])
	}
	command = strings.ToUpper(command)
	switch command {
	case "ECHO", "BROADCAST", "MSG":
		if !validText(text) {
			return s.reply(c, "[ERROR]: "+command+" needs one line of UTF-8 text")
		}
		if command == "ECHO" {
			return s.reply(c, "[ECHO]: "+text)
		}
		s.logger.Printf("[BROADCAST] %s: %q", c.addr, text)
		s.broadcast(c, "[BROADCAST from "+c.addr+"]: "+text)
		return true
	case "TIME", "CLOSE", "QUIT":
		if text != "" {
			return s.reply(c, "[ERROR]: "+command+" does not accept arguments")
		}
		if command == "TIME" {
			return s.reply(c, "[TIME]: "+time.Now().Format(time.RFC3339))
		}
		s.reply(c, goodbye)
		return false
	default:
		return s.reply(c, "[ERROR]: Unknown command. Available: ECHO, TIME, BROADCAST, MSG, CLOSE, QUIT")
	}
}

func (s *server) listenUDP(conn net.PacketConn) error {
	// A full datagram buffer lets us reject oversized packets without relaying truncated data.
	buffer := make([]byte, 65535)
	for {
		n, sender, err := conn.ReadFrom(buffer)
		if err != nil {
			return err
		}
		text := strings.TrimSpace(string(buffer[:n]))
		if n > maxTelemetry || !validText(text) {
			s.logger.Printf("[UDP] Ignored invalid datagram from %s (%d B)", sender, n)
			continue
		}
		s.logger.Printf("[UDP] %s (%d B): %q", sender, n, text)
		s.broadcast(nil, fmt.Sprintf("[TELEMETRY from %s]: %s", sender, text))
	}
}

func validText(text string) bool {
	return text != "" && utf8.ValidString(text) && !strings.ContainsFunc(text, unicode.IsControl)
}

func (s *server) reply(c *client, line string) bool {
	s.mu.Lock()
	active := !c.ending && !s.stopping
	queued := active && enqueue(c, line)
	s.mu.Unlock()
	if active && !queued {
		_ = c.conn.Close()
	}
	return queued
}

func (s *server) broadcast(exclude *client, line string) {
	s.mu.Lock()
	failed := s.broadcastLocked(exclude, line)
	s.mu.Unlock()
	closeClients(failed)
}

func (s *server) broadcastLocked(exclude *client, line string) []*client {
	var failed []*client
	if !s.stopping {
		for c := range s.clients {
			if c != exclude && !c.ending && !enqueue(c, line) {
				failed = append(failed, c)
			}
		}
	}
	return failed
}

// Only call enqueue while holding the server mutex. The writer is the only receiver.
func enqueue(c *client, line string) bool {
	if len(c.out) >= queueSize {
		return false
	}
	c.out <- line
	return true
}

func (s *server) leave(c *client) {
	s.mu.Lock()
	var failed []*client
	if !c.ending {
		c.ending = true
		close(c.out)
		failed = s.broadcastLocked(c, "[SYSTEM]: Left "+c.addr)
	}
	s.mu.Unlock()
	closeClients(failed)
	s.logger.Printf("[TCP] Disconnected %s", c.addr)
}

func (s *server) shutdown() {
	s.mu.Lock()
	s.stopping = true
	clients := make([]*client, 0, len(s.clients))
	var notified int
	for c := range s.clients {
		clients = append(clients, c)
		if !c.ending {
			c.out <- shutdownEvent
			close(c.out)
			c.ending = true
			notified++
		}
	}
	s.mu.Unlock()
	forceClosed := make(chan struct{})
	forceClose := time.AfterFunc(writeWait, func() {
		defer close(forceClosed)
		closeClients(clients)
	})
	s.handlers.Wait()
	if !forceClose.Stop() {
		<-forceClosed
	}
	s.logger.Printf("[SERVER] Shutdown complete; final event queued for %d client(s)", notified)
}

func closeClients(clients []*client) {
	for _, c := range clients {
		_ = c.conn.Close()
	}
}
